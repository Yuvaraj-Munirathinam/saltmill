# saltmill — Architecture & Flow

**Efficient large-CSV processing for Apache Spark / Databricks.**

saltmill takes a large CSV and produces a well-partitioned DataFrame (or writes
it to Delta/Parquet), automatically tuning salting, partition keys, and Spark
configuration so you don't have to hand-tune skew fixes. It is designed to run
on Azure Databricks and to execute in **bounded** time and cost — never
unbounded.

---

## Design principles

- **One stage, one responsibility.** Each pipeline step is a small class that
  takes `(spark, config)` and does exactly one thing. The orchestrator
  (`SaltmillProcessor`) is the only component that knows the whole pipeline.
- **Fail fast and cheap.** Invalid config is rejected at construction; oversized
  driver-side work is refused up front rather than grinding for hours.
- **Bounded execution.** Guardrails cap file-split size, chunk counts, wall-clock
  runtime, and redundant passes so a job cannot run away with cluster cost.
- **Resumable.** Optional checkpointing records stage completion so a rerun skips
  finished work.

---

## Module map

```
saltmill/
├── __init__.py        Public API surface (exports)
├── _version.py        __version__
│
│  ── ENTRY POINTS ──────────────────────────────────
├── processor.py       SaltmillProcessor — orchestrator (process / analyze)
├── compat.py          Simple v0.1 API: saltmill.read() + SaltMill class
│
│  ── CONFIG & DATA ─────────────────────────────────
├── config.py          SaltmillConfig dataclass + all validation
├── models.py          Frozen result types: SchemaInfo, SkewReport,
│                       PartitionPlan, ProcessingResult
├── exceptions.py      Exception hierarchy (SaltmillError → …)
│
│  ── PIPELINE STAGES ───────────────────────────────
├── splitter.py        FileSplitter — pre-split one big multiLine file
├── schema.py          SchemaInferrer — resolve/infer schema (+ caching)
├── cardinality.py     CardinalityAnalyzer — pick partition keys
├── skew.py            SkewDetector — measure skew, choose salt buckets
├── spark_conf.py      SparkConfigurator — detect cluster, set Spark confs
├── reader.py          CsvReader — schema-applied read + size estimate
├── salter.py          Salter — add salt column + repartition
├── writer.py          CsvWriter — write Delta/Parquet
│
│  ── SUPPORT ──────────────────────────────────────
├── checkpoint.py      CheckpointManager — resumable stage metadata (Hadoop FS)
└── progress.py        ProgressReporter — per-stage logging + callback
```

---

## Entry points

### Advanced API — full pipeline with write

```python
from saltmill import SaltmillProcessor, SaltmillConfig

result = SaltmillProcessor(SaltmillConfig(
    input_path="abfss://raw@acct.dfs.core.windows.net/data/large.csv",
    output_path="abfss://curated@acct.dfs.core.windows.net/output/delta/",
)).process()
```

### Simple API — get a DataFrame, no write

```python
import saltmill
df = saltmill.read(spark, "abfss://.../huge.csv")
```

`compat.py` wraps the processor: it calls `analyze()` (dry-run plan) + schema
resolve + read + salt and returns the DataFrame. The simple path intentionally
**skips** file splitting and the write stage.

---

## The main flow — `SaltmillProcessor.process()`

The Spark-heavy body runs inside `_runtime_guard` (optional wall-clock
watchdog). Stages run in order, each wrapped in `reporter.stage(...)`:

| # | Stage | Component | What happens |
|---|-------|-----------|--------------|
| 0 | `file_split` | `FileSplitter` | If input is one large multiLine file, pre-split it into record-aligned chunks; redirect `input_path` to the staging dir. No-op otherwise. |
| 1 | `schema_inference` | `SchemaInferrer` | Use explicit schema, or infer from a bounded sample. Restore from checkpoint cache if present and valid. |
| – | (cluster detect) | `SparkConfigurator` | Detect worker count and cores; build a single cached 5% sample shared by the next two stages. |
| 2 | `cardinality_analysis` | `CardinalityAnalyzer` | Score columns by `approx_count_distinct`; pick the top 1–2 as partition keys (skipped if provided). |
| 3 | `skew_detection` | `SkewDetector` | Per key, measure top-value frequency; choose salt-bucket count (power of two). |
| – | (plan) | `_build_plan` | Build `PartitionPlan`; partition counts clamped to `[200, 20000]`. |
| 4 | `spark_configuration` | `SparkConfigurator` | Set AQE, `shuffle.partitions`, `maxPartitionBytes`, Delta optimizeWrite/autoCompact. |
| 5 | `csv_read` | `CsvReader` | Full read with schema applied (no full-scan inference). |
| 6 | `salting` | `Salter` | Add salt column, repartition on `(keys, _salt)`, optional checkpoint, drop salt. |
| 7 | `write` | `CsvWriter` | Write Delta/Parquet; count output files. |
| – | (count) | — | `output_df.count()` for `total_rows` (skippable via `count_output_rows`). |

Output: a `ProcessingResult` (schema, plan, row/file counts, duration, applied
Spark conf, warnings).

```
SaltmillConfig ──validate──► process() ──► _runtime_guard ──► _run_pipeline()
                                                                   │
   file_split → schema_inference → [sample] → cardinality → skew → plan
        → spark_configuration → csv_read → salting(+checkpoint) → write → count
                                                                   │
                                                          ProcessingResult
```

---

## Salting — how skew is broken (deep dive)

**The problem.** When a partition/join/group-by key is imbalanced (one value
owns a large share of rows), Spark sends all those rows to a single task. That
task becomes a straggler: most executors finish quickly and idle while one runs
for the bulk of the job. This is *data skew*, and it is the dominant cause of
slow shuffles on large CSVs.

**The fix — salting.** saltmill widens each hot key into several synthetic
sub-keys so its rows fan out across many partitions instead of one. In
`salter.py`:

```python
df = df.withColumn(
    salt_col,
    F.pmod(F.monotonically_increasing_id(), F.lit(plan.salt_buckets)),
)
repartition_cols = [F.col(k) for k in plan.partition_keys] + [F.col(salt_col)]
return df.repartition(plan.target_partitions, *repartition_cols)
```

- `monotonically_increasing_id()` yields distinct longs across the DataFrame.
- `pmod(..., salt_buckets)` maps each row to a bucket in `[0, salt_buckets)`,
  spreading rows of the *same* key roughly evenly across buckets.
- Repartitioning on `(partition_keys, _salt)` means a hot key now occupies up to
  `salt_buckets` partitions instead of one — the straggler is split into
  `salt_buckets` parallel tasks.
- `drop_salt()` removes the synthetic column before the result is written or
  returned, so the salt is invisible downstream.

**Choosing the bucket count** (`skew.py`). The number isn't guessed; it scales
with measured skew and cluster width:

1. `SkewDetector.analyze()` computes, per partition key, the top value's share of
   rows (on the 5% sample). `top_freq > 10%` flags the key as skewed.
2. `_recommend_buckets(top_freq)` scales with severity, using
   `total_cores = worker_count × cores_per_worker`:
   - `≤ 10%` → 1 bucket (no salting needed)
   - `≤ 30%` → `max(4, cores/4)`
   - `≤ 60%` → `max(8, cores/2)`
   - `> 60%` → `max(16, cores)`
   The result is rounded up to the next power of two (clean hashing/coalescing).
3. `pick_salt_buckets()` takes the **max** recommendation across keys, so the
   most skewed key governs.

**Why a power of two and why cluster-relative?** Powers of two coalesce cleanly
under AQE and avoid uneven bucket sizes. Tying bucket count to core count means
a hot key is split into roughly as many pieces as there are cores to consume
them — enough parallelism to erase the straggler, without over-shredding into
tiny tasks.

**When `salt_buckets ≤ 1`** (no meaningful skew), `Salter.apply()` skips the
salt column entirely and just repartitions on the keys — no wasted column or
shuffle width.

**Safety.** If the configured `salt_column_name` already exists in the
DataFrame, `Salter` raises `ConfigurationError` rather than silently overwriting
a real column.

---

## Checkpointing — resumable runs (deep dive)

Large jobs fail (spot-instance loss, transient I/O). Without checkpointing, a
rerun repeats everything. saltmill's `CheckpointManager` (`checkpoint.py`)
records stage completion and cached results so a rerun skips finished work.

**Where state lives.** All metadata is written under
`<checkpoint_path>/_saltmill_meta` via the Hadoop FileSystem API, so it works on
any scheme Spark can see (`abfss`, `s3`, `dbfs`, `file`). Two kinds of files:

- `*.done` markers — a stage completed (e.g. `schema_inference.done`).
- `*.json` metadata — serialized stage output (e.g. `schema_info.json`).

**Public surface.**

| Method | Purpose |
|--------|---------|
| `setup()` | Set the Spark checkpoint dir for RDD/DataFrame checkpoints. |
| `is_stage_complete(stage)` | Does `<stage>.done` exist? |
| `mark_stage_complete(stage)` | Write `<stage>.done`. |
| `save_metadata(key, value)` | Write JSON metadata for a stage. |
| `load_metadata(key)` | Read JSON metadata (None if missing/unreadable). |
| `checkpoint_df(df, stage)` | Eagerly checkpoint a DataFrame, then mark the stage done. |

**Two complementary mechanisms.**

1. **Metadata caching (schema).** In `processor._resolve_schema()`:
   - On entry, if `schema_inference` is marked done, load `schema_info` and
     `SchemaInferrer.deserialize()` it. If valid, the (potentially expensive)
     inference scan is skipped entirely.
   - Otherwise infer, then `save_metadata("schema_info", …)` and
     `mark_stage_complete("schema_inference")`.
   - Deserialization is defensive: it validates required keys and types and
     cross-checks field names against any configured schema, returning `None`
     (triggering safe re-inference) on any mismatch — a corrupt or tampered
     cache can never inject a bad schema.

2. **DataFrame checkpointing (salting).** In `process()`'s salting stage:
   ```python
   if checkpoint and not checkpoint.is_stage_complete("salting"):
       salted_df = checkpoint.checkpoint_df(salted_df, "salting")
   ```
   `df.checkpoint(eager=True)` materializes the salted/repartitioned DataFrame to
   the checkpoint dir and **truncates its lineage**. On a rerun, the expensive
   shuffle that produced it does not have to be recomputed, and the stage is
   already marked done.

**Failure isolation.** Metadata writes are best-effort: `save_metadata` and the
file helpers swallow errors and log at debug level, so a metadata hiccup degrades
to "recompute this stage" rather than failing the whole job. `setup()` and
`checkpoint_df()` raise `CheckpointError` on hard failures, with sanitized
messages that don't leak storage URLs or credentials.

---

## Cross-cutting: runaway-cost guardrails

saltmill is meant to run **bounded**. The guardrails (in `config.py`,
`splitter.py`, `processor.py`):

| Guardrail | Default | Prevents |
|-----------|---------|----------|
| `split_max_file_gb` | 50 GB | A single multiLine file above this is refused (driver-side split would be serial for hours). |
| `max_split_chunks` | 100,000 | A tiny `target_chunk_size_mb` exploding into millions of small files. |
| `max_runtime_seconds` | `None` (off) | A hung/runaway action — saltmill's Spark jobs run in a dedicated job group; a watchdog cancels it on timeout and raises `ProcessingTimeoutError`. |
| `count_output_rows` | `True` | Set `False` to skip the final full-scan `count()`. |

The driver-side split is not a Spark job, so the watchdog cannot interrupt it
mid-read; that path is bounded separately by `split_max_file_gb`.

---

## Single large-file splitting

Spark cannot split one CSV across tasks when `multiLine=true` (a record may span
several physical lines). `FileSplitter` solves this by streaming the file once
on the driver with Python's `csv` reader — which tracks quote state, so chunk
boundaries fall only **between** complete records and a quoted multiline field is
never broken. The header is replicated to every chunk, and chunks are written
under a staging path that Spark then reads in parallel.

`plan_split()` decides: split only when the input is exactly one data file,
`multiLine=true`, and size ≥ `split_threshold_gb`. Multi-file inputs and
non-multiLine reads are left to Spark's native splitting.

---

## Configuration & results

- **`SaltmillConfig`** (`config.py`) — all inputs, validated in `__post_init__`
  (path schemes, write mode, log level, salt column name, split bounds, runtime
  bounds). `SaltmillProcessor.from_dict()` accepts only an allowlisted set of
  keys, safe for untrusted Databricks-widget input.
- **Result models** (`models.py`, frozen dataclasses) — `SchemaInfo`,
  `SkewReport`, `PartitionPlan`, and the final `ProcessingResult`.
- **Exceptions** (`exceptions.py`) — all derive from `SaltmillError`:
  `ConfigurationError`, `SchemaInferenceError`, `SkewDetectionError`,
  `CheckpointError`, `UnsupportedPathError`, `ProcessingTimeoutError`.
