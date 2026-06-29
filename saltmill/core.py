"""
SaltMill: main entry point for large CSV ingestion with automatic tuning.
"""

from __future__ import annotations
from typing import TYPE_CHECKING

from .env import detect_environment, get_worker_count, get_total_size_bytes
from .optimizer import compute_tuning, TuningParams
from .schema import resolve_schema
from .spark_config import apply_spark_config

if TYPE_CHECKING:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.types import StructType


class SaltMill:
    """
    Reads large CSV files (single or multi-file) into a well-partitioned DataFrame.

    Parameters
    ----------
    spark:
        Active SparkSession.
    workers:
        Number of worker nodes / parallelism hint.  Auto-detected when omitted.
    verbose:
        Print tuning summary before reading.

    Examples
    --------
    Simplest usage::

        sm = SaltMill(spark)
        df = sm.read("s3://bucket/huge.csv")

    With explicit schema and partition column::

        sm = SaltMill(spark, workers=32)
        df = sm.read(
            ["s3://bucket/part1.csv", "s3://bucket/part2.csv"],
            schema={"id": "long", "region": "string", "amount": "double"},
            partition_col="region",
        )

    Inspect tuning without reading::

        params = sm.tune("s3://bucket/huge.csv", hint_size_gb=500)
        print(params.summary())
    """

    def __init__(self, spark: "SparkSession", *, workers: int | None = None, verbose: bool = True):
        self.spark = spark
        self._workers = workers
        self.verbose = verbose
        self._env = detect_environment(spark)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def read(
        self,
        paths: "str | list[str]",
        *,
        schema: "StructType | dict | None" = None,
        partition_col: "str | list[str] | None" = None,
        salt_buckets: int | None = None,
        num_partitions: int | None = None,
        hint_size_gb: float | None = None,
        delimiter: str = ",",
        encoding: str = "UTF-8",
        null_value: str = "",
    ) -> "DataFrame":
        """
        Read one or more large CSV files with automatic salt partitioning.

        Parameters
        ----------
        paths:
            Single path or list of paths (local, HDFS, S3, ABFSS, GCS).
        schema:
            Optional schema. StructType, dict shorthand {"col": "type"}, or None
            to auto-infer from a sample of the first file.
        partition_col:
            Column(s) to include in the repartition key alongside the salt.
            Useful for downstream operations that filter on that column.
        salt_buckets:
            Override the auto-computed number of salt buckets.
        num_partitions:
            Override the total partition count after repartition.
        hint_size_gb:
            Total uncompressed CSV size in GB.  Helps when file-size detection
            fails (e.g. no Hadoop credentials in unit tests).
        delimiter:
            CSV field delimiter (default ",").
        encoding:
            File encoding (default "UTF-8").
        null_value:
            String to treat as null (default "").

        Returns
        -------
        DataFrame
            Salted, repartitioned DataFrame ready for processing or Delta writes.
        """
        paths = [paths] if isinstance(paths, str) else list(paths)

        params = self.tune(
            paths,
            salt_buckets=salt_buckets,
            num_partitions=num_partitions,
            hint_size_gb=hint_size_gb,
        )

        if self.verbose:
            print(f"[saltmill] {params.summary()}")

        apply_spark_config(self.spark, params, self._env)

        resolved_schema = resolve_schema(self.spark, paths[0], schema)

        df = self._csv_reader(resolved_schema, delimiter, encoding, null_value).csv(paths)

        df = self._salt_and_repartition(df, params, partition_col)

        return df

    def tune(
        self,
        paths: "str | list[str] | None" = None,
        *,
        salt_buckets: int | None = None,
        num_partitions: int | None = None,
        hint_size_gb: float | None = None,
    ) -> TuningParams:
        """
        Return tuning parameters without reading any data.

        Useful for previewing what saltmill will do before committing to a read.
        """
        paths = ([paths] if isinstance(paths, str) else list(paths)) if paths else []

        hint_bytes = int(hint_size_gb * 1024 ** 3) if hint_size_gb else None
        file_size = hint_bytes or (get_total_size_bytes(self.spark, paths) if paths else None)
        workers = self._workers or get_worker_count(self.spark)

        return compute_tuning(
            file_size_bytes=file_size,
            workers=workers,
            salt_buckets_override=salt_buckets,
            num_partitions_override=num_partitions,
        )

    def write_delta(
        self,
        df: "DataFrame",
        path: str,
        *,
        partition_by: "str | list[str] | None" = None,
        mode: str = "overwrite",
    ) -> None:
        """
        Write a DataFrame to Delta Lake with Databricks-optimized settings.

        Parameters
        ----------
        df:
            DataFrame to write (typically the output of ``read()``).
        path:
            Delta table path (s3://, abfss://, dbfs:/, etc.).
        partition_by:
            Column(s) for Delta partition pruning.
        mode:
            Write mode — "overwrite" or "append".
        """
        writer = df.write.format("delta").mode(mode)
        if partition_by:
            cols = [partition_by] if isinstance(partition_by, str) else partition_by
            writer = writer.partitionBy(*cols)
        writer.save(path)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _csv_reader(self, schema, delimiter: str, encoding: str, null_value: str):
        reader = (
            self.spark.read.option("header", "true")
            .option("sep", delimiter)
            .option("encoding", encoding)
            .option("nullValue", null_value)
            .option("multiLine", "false")
            .option("ignoreLeadingWhiteSpace", "true")
            .option("ignoreTrailingWhiteSpace", "true")
            .option("mode", "PERMISSIVE")
        )
        if schema:
            reader = reader.schema(schema)
        else:
            reader = reader.option("inferSchema", "false")
        return reader

    def _salt_and_repartition(
        self, df: "DataFrame", params: TuningParams, partition_col
    ) -> "DataFrame":
        from pyspark.sql.functions import pmod, monotonically_increasing_id

        df = df.withColumn("_salt", pmod(monotonically_increasing_id(), params.salt_buckets))

        repartition_cols: list[str] = []
        if partition_col:
            if isinstance(partition_col, str):
                repartition_cols.append(partition_col)
            else:
                repartition_cols.extend(partition_col)
        repartition_cols.append("_salt")

        df = df.repartition(params.num_partitions, *repartition_cols).drop("_salt")
        return df
