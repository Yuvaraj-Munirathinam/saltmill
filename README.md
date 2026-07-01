# saltmill

**Efficient large CSV processing for PySpark and Databricks.**

saltmill automatically computes optimal salt buckets, partition counts, and Spark configuration for processing CSV files of any size — from a single API call.

---

## The problem

Reading a 500 GB CSV file naively in Spark causes data skew, memory pressure, and slow shuffles. Fixing it requires manually tuning:

- Salt bucket count
- Repartition strategy
- `spark.sql.shuffle.partitions`
- `spark.sql.files.maxPartitionBytes`
- Databricks Delta write optimizations

saltmill does all of this automatically, based on file size and cluster parallelism.

## Installation

```bash
pip install saltmill
```

> PySpark is a peer dependency — saltmill works with whatever version your cluster runs.

## Quick start

```python
import saltmill

df = saltmill.read(spark, "abfss://raw@myaccount.dfs.core.windows.net/data/large.csv")
```

That's it. saltmill will:
1. Detect file size via Hadoop FileSystem
2. Auto-compute salt buckets and partition count
3. Apply optimized Spark configs (shuffle partitions, maxPartitionBytes, Delta settings on Databricks)
4. Infer schema from a 0.1% sample of the first file
5. Read and return a well-partitioned DataFrame

## Usage

### Module-level function (simplest)

```python
import saltmill

df = saltmill.read(spark, "abfss://raw@myaccount.dfs.core.windows.net/data/huge.csv")
```

### Class-based (more control)

```python
from saltmill import SaltMill

sm = SaltMill(spark, workers=32)
df = sm.read("abfss://raw@myaccount.dfs.core.windows.net/data/huge.csv")
```

### Multiple files

```python
df = saltmill.read(
    spark,
    [
        "abfss://raw@myaccount.dfs.core.windows.net/data/2024-01.csv",
        "abfss://raw@myaccount.dfs.core.windows.net/data/2024-02.csv",
    ],
    hint_size_gb=500,
)
```

### With explicit schema

```python
df = saltmill.read(
    spark,
    "abfss://raw@myaccount.dfs.core.windows.net/data/sales.csv",
    schema={
        "order_id":   "long",
        "region":     "string",
        "amount":     "double",
        "created_at": "timestamp",
    },
    partition_col="region",
)
```

### With a PySpark StructType

```python
from pyspark.sql.types import StructType, StructField, LongType, StringType

schema = StructType([
    StructField("id", LongType(), True),
    StructField("name", StringType(), True),
])

df = saltmill.read(spark, "abfss://raw@myaccount.dfs.core.windows.net/data/data.csv", schema=schema)
```

### Preview tuning parameters without reading

```python
sm = SaltMill(spark)
params = sm.tune("abfss://raw@myaccount.dfs.core.windows.net/data/huge.csv", hint_size_gb=500)
print(params.summary())
# saltmill tuning → file: 500.0 GB, workers: 64, salt_buckets: 64,
#   partitions: 640, maxPartitionBytes: 64 MB
```

### Write to Delta Lake

```python
sm = SaltMill(spark)
df = sm.read("abfss://raw@myaccount.dfs.core.windows.net/data/huge.csv", partition_col="region")
sm.write_delta(df, "abfss://curated@myaccount.dfs.core.windows.net/delta/sales", partition_by="region")
```

## How it works

### Salting

saltmill assigns each row a random bucket using:

```python
df.withColumn("_salt", pmod(monotonically_increasing_id(), salt_buckets))
  .repartition(num_partitions, partition_col, "_salt")
  .drop("_salt")
```

This breaks data skew even when a join or group-by column is highly imbalanced.

### Auto-tuning formula

| Input | Rule |
|---|---|
| File size | 1 salt bucket per 8 GB, rounded to nearest power of 2 |
| Salt buckets | Clamped to [8, 512] |
| Partitions | `salt_buckets × 10`, rounded up to nearest multiple of worker count |
| maxPartitionBytes | 64 MB (matches default HDFS block) |

### Example: 500 GB file, 64 workers

```
file_size_gb   = 500
salt_buckets   = round_pow2(500 / 8) = round_pow2(62.5) = 64
num_partitions = 64 × 10 = 640  (already a multiple of 64)
shuffle_partitions = 640
maxPartitionBytes  = 64 MB
```

This matches the pattern proven in production:

```python
# What saltmill does internally
spark.conf.set("spark.sql.shuffle.partitions", 640)
spark.conf.set("spark.sql.files.maxPartitionBytes", 64 * 1024 * 1024)

dfw = (
    df.withColumn("_salt", pmod(monotonically_increasing_id(), 64))
      .repartition(640, "region", "_salt")
      .drop("_salt")
)
```

### Single large multiLine file splitting

Spark cannot split a single CSV across tasks when `multiLine=true` — a record may
span several physical lines, so the whole file is read by one task. When the input
resolves to **one** large multiLine file, saltmill pre-splits it on the driver into
many record-aligned chunks so the read parallelises:

```python
from saltmill import SaltmillProcessor, SaltmillConfig

result = SaltmillProcessor(SaltmillConfig(
    input_path="abfss://raw@account.dfs.core.windows.net/data/huge.csv",
    output_path="abfss://curated@account.dfs.core.windows.net/output/delta/",
    staging_path="abfss://raw@account.dfs.core.windows.net/_staging/",  # or set checkpoint_path
    csv_options={"header": "true", "multiLine": "true", "quote": '"', "escape": '"'},
)).process()
```

How the decision is made:

| Input | Behaviour |
|---|---|
| Multiple data files | Read in parallel as-is — **no splitting** |
| Single file, `multiLine=false` | Spark splits natively — **no splitting** |
| Single file, `multiLine=true`, ≥ `split_threshold_gb` | **Split** into `target_chunk_size_mb` chunks |

Splitting uses Python's `csv` reader, which tracks quote state — chunk boundaries
fall only **between** complete records, so a quoted multiline field is never broken.
Each chunk gets a copy of the header so every output file reads uniformly.

| Config | Default | Description |
|---|---|---|
| `split_large_files` | `True` | Master switch |
| `split_threshold_gb` | `1.0` | Minimum single-file size to trigger splitting |
| `target_chunk_size_mb` | `max_partition_bytes_mb` (128) | Approximate chunk size |
| `staging_path` | `<checkpoint_path>/_saltmill_split` | Where chunks are written |

> Splitting reads the file once on the driver, so it is a one-time serial cost.
> It is worthwhile for moderately large single files (unlocks parallelism for the
> re-read and all downstream stages). For truly massive single files, producing
> multiple files upstream avoids the driver pass entirely.

### Databricks-specific settings

When running on Databricks, saltmill also sets:

```
spark.databricks.delta.optimizeWrite.enabled = true
spark.databricks.delta.autoCompact.enabled   = true
```

## Schema dict shorthand

| Alias | Spark type |
|---|---|
| `"str"`, `"string"` | StringType |
| `"int"`, `"integer"` | IntegerType |
| `"long"`, `"bigint"` | LongType |
| `"float"` | FloatType |
| `"double"` | DoubleType |
| `"bool"`, `"boolean"` | BooleanType |
| `"date"` | DateType |
| `"timestamp"` | TimestampType |
| `"decimal"` | DecimalType(38,10) |

Any Spark SQL type string is also accepted directly (e.g. `"decimal(10,2)"`).

## API reference

### `saltmill.read(spark, paths, *, schema, partition_col, workers, salt_buckets, num_partitions, hint_size_gb, delimiter, encoding, null_value, verbose)`

Module-level convenience function. See class docs for parameter details.

### `SaltMill(spark, *, workers, verbose)`

| Parameter | Type | Default | Description |
|---|---|---|---|
| `spark` | SparkSession | required | Active session |
| `workers` | int | auto-detected | Worker node count |
| `verbose` | bool | True | Print tuning summary |

### `SaltMill.read(paths, *, schema, partition_col, salt_buckets, num_partitions, hint_size_gb, delimiter, encoding, null_value)`

| Parameter | Type | Default | Description |
|---|---|---|---|
| `paths` | str or list | required | CSV path(s) |
| `schema` | StructType / dict / None | None | Schema or auto-infer |
| `partition_col` | str or list | None | Extra partition key(s) |
| `salt_buckets` | int | auto | Override salt bucket count |
| `num_partitions` | int | auto | Override total partitions |
| `hint_size_gb` | float | None | File size hint (when detection fails) |
| `delimiter` | str | `","` | CSV separator |
| `encoding` | str | `"UTF-8"` | File encoding |
| `null_value` | str | `""` | Null string |

### `SaltMill.tune(paths, *, salt_buckets, num_partitions, hint_size_gb) → TuningParams`

Returns computed tuning parameters without reading any data.

### `SaltMill.write_delta(df, path, *, partition_by, mode)`

Writes a DataFrame to Delta Lake with Databricks-optimized settings.

## Databricks cluster compatibility

saltmill runs on **all** Databricks cluster types — single-user, job, shared,
and serverless. The core pipeline (schema inference, cardinality/skew analysis,
salting, and write) uses only the DataFrame API, so it works everywhere.

Shared and serverless clusters run **Spark Connect**, where the driver JVM is
sandboxed. A few auxiliary features need that JVM; when it isn't available they
**auto-disable with a warning** rather than failing the job:

| Feature | Single-user / Job | Shared / Serverless |
|---|---|---|
| Core pipeline (read, analyze, salt, write) | ✅ | ✅ |
| Single large multiLine file splitting | ✅ | ✅ (Spark-native) |
| File-size estimate & output file count | ✅ | ✅ (via `binaryFile`) |
| Checkpointing (resumable runs) | ✅ | ⚠️ disabled (needs driver JVM) |
| `max_runtime_seconds` watchdog | ✅ | ⚠️ disabled (needs driver JVM) |

If you rely on checkpointing or the runtime watchdog, use a single-user or job
cluster.

## Development

```bash
pip install -e ".[dev]"
pytest
```

## License

Apache 2.0
