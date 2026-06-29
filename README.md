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

df = saltmill.read(spark, "s3://my-bucket/large.csv")
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

df = saltmill.read(spark, "s3://bucket/huge.csv")
```

### Class-based (more control)

```python
from saltmill import SaltMill

sm = SaltMill(spark, workers=32)
df = sm.read("s3://bucket/huge.csv")
```

### Multiple files

```python
df = saltmill.read(
    spark,
    ["s3://bucket/2024-01.csv", "s3://bucket/2024-02.csv"],
    hint_size_gb=500,
)
```

### With explicit schema

```python
df = saltmill.read(
    spark,
    "s3://bucket/sales.csv",
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

df = saltmill.read(spark, "s3://bucket/data.csv", schema=schema)
```

### Preview tuning parameters without reading

```python
sm = SaltMill(spark)
params = sm.tune("s3://bucket/huge.csv", hint_size_gb=500)
print(params.summary())
# saltmill tuning → file: 500.0 GB, workers: 64, salt_buckets: 64,
#   partitions: 640, maxPartitionBytes: 64 MB
```

### Write to Delta Lake

```python
sm = SaltMill(spark)
df = sm.read("s3://bucket/huge.csv", partition_col="region")
sm.write_delta(df, "s3://bucket/delta/sales", partition_by="region")
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

## Development

```bash
pip install -e ".[dev]"
pytest
```

## License

Apache 2.0
