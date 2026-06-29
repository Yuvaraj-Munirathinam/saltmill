"""Environment detection: Databricks, standalone Spark, local."""

from __future__ import annotations
from enum import Enum


class SparkEnv(Enum):
    DATABRICKS = "databricks"
    SPARK_STANDALONE = "standalone"
    LOCAL = "local"


def detect_environment(spark) -> SparkEnv:
    try:
        spark.conf.get("spark.databricks.clusterUsageTags.clusterName")
        return SparkEnv.DATABRICKS
    except Exception:
        pass
    master = spark.sparkContext.master
    if master.startswith("local"):
        return SparkEnv.LOCAL
    return SparkEnv.SPARK_STANDALONE


def is_databricks(spark) -> bool:
    return detect_environment(spark) == SparkEnv.DATABRICKS


def get_worker_count(spark) -> int:
    """Estimate usable parallelism from the cluster."""
    try:
        sc = spark.sparkContext
        # defaultParallelism = total cores across all executors
        return max(1, sc.defaultParallelism)
    except Exception:
        return 8


def get_total_size_bytes(spark, paths: list[str]) -> int | None:
    """
    Best-effort file size via Hadoop FileSystem API.
    Returns None when the path is inaccessible (e.g. no credentials in local tests).
    """
    try:
        sc = spark.sparkContext
        jvm = sc._jvm
        hadoop_conf = sc._jsc.hadoopConfiguration()
        total = 0
        for p in paths:
            fs_path = jvm.org.apache.hadoop.fs.Path(p)
            fs = fs_path.getFileSystem(hadoop_conf)
            summary = fs.getContentSummary(fs_path)
            total += summary.getLength()
        return total
    except Exception:
        return None
