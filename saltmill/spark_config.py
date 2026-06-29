"""Apply optimized Spark configuration for large CSV ingestion."""

from __future__ import annotations
from .optimizer import TuningParams
from .env import SparkEnv


def apply_spark_config(spark, params: TuningParams, env: SparkEnv) -> None:
    conf = spark.conf

    conf.set("spark.sql.shuffle.partitions", str(params.shuffle_partitions))
    conf.set(
        "spark.sql.files.maxPartitionBytes",
        str(params.max_partition_bytes),
    )
    # Avoid tiny files on re-reads
    conf.set("spark.sql.files.openCostInBytes", str(4 * 1024 * 1024))

    if env == SparkEnv.DATABRICKS:
        _apply_databricks_config(spark, params)


def _apply_databricks_config(spark, params: TuningParams) -> None:
    conf = spark.conf
    conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
    conf.set("spark.databricks.delta.autoCompact.enabled", "true")
    conf.set("spark.databricks.delta.optimizeWrite.numShuffleBlocks", "50000")
    conf.set(
        "spark.databricks.delta.optimizeWrite.binSize",
        str(params.max_partition_bytes),
    )
