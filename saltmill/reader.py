"""Module-level convenience function so users can call saltmill.read() directly."""

from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.types import StructType


def read(
    spark: "SparkSession",
    paths: "str | list[str]",
    *,
    schema: "StructType | dict | None" = None,
    partition_col: "str | list[str] | None" = None,
    workers: int | None = None,
    salt_buckets: int | None = None,
    num_partitions: int | None = None,
    hint_size_gb: float | None = None,
    delimiter: str = ",",
    encoding: str = "UTF-8",
    null_value: str = "",
    verbose: bool = True,
) -> "DataFrame":
    """
    One-call interface for large CSV ingestion with automatic salt partitioning.

    Parameters
    ----------
    spark:
        Active SparkSession.
    paths:
        Single path or list of paths to CSV file(s).
    schema:
        StructType, dict shorthand {"col": "type"}, or None to auto-infer.
    partition_col:
        Column(s) to co-locate in the repartition alongside the salt.
    workers:
        Cluster worker count (auto-detected when omitted).
    salt_buckets:
        Override auto-computed salt bucket count.
    num_partitions:
        Override total partition count.
    hint_size_gb:
        Total CSV size in GB — helps when Hadoop size detection fails.
    delimiter:
        CSV field separator (default ",").
    encoding:
        File encoding (default "UTF-8").
    null_value:
        String to treat as null (default "").
    verbose:
        Print tuning summary (default True).

    Returns
    -------
    DataFrame

    Examples
    --------
    ::

        import saltmill

        df = saltmill.read(spark, "s3://bucket/large.csv")

        df = saltmill.read(
            spark,
            ["s3://a/b.csv", "s3://a/c.csv"],
            schema={"id": "long", "region": "string", "revenue": "double"},
            partition_col="region",
            hint_size_gb=500,
        )
    """
    from .core import SaltMill

    sm = SaltMill(spark, workers=workers, verbose=verbose)
    return sm.read(
        paths,
        schema=schema,
        partition_col=partition_col,
        salt_buckets=salt_buckets,
        num_partitions=num_partitions,
        hint_size_gb=hint_size_gb,
        delimiter=delimiter,
        encoding=encoding,
        null_value=null_value,
    )
