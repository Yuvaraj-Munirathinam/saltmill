from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from saltmill.config import SaltmillConfig
    from saltmill.models import PartitionPlan

log = logging.getLogger("saltmill")


class SparkConfigurator:
    def __init__(self, spark: "SparkSession", config: "SaltmillConfig") -> None:
        self._spark = spark
        self._config = config

    def apply(self, plan: "PartitionPlan") -> dict[str, str]:
        """Apply runtime Spark settings and return what was set."""
        cfg = self._config
        settings: dict[str, str] = {}

        if cfg.enable_adaptive_query:
            settings["spark.sql.adaptive.enabled"] = "true"
            settings["spark.sql.adaptive.coalescePartitions.enabled"] = "true"
            settings["spark.sql.adaptive.skewJoin.enabled"] = "true"

        shuffle = cfg.shuffle_partitions or plan.shuffle_partitions
        settings["spark.sql.shuffle.partitions"] = str(shuffle)
        settings["spark.sql.files.maxPartitionBytes"] = str(
            cfg.max_partition_bytes_mb * 1024 * 1024
        )
        settings["spark.sql.files.openCostInBytes"] = str(4 * 1024 * 1024)

        from saltmill.config import WriteFormat

        if cfg.write_format == WriteFormat.DELTA:
            settings["spark.databricks.delta.optimizeWrite.enabled"] = (
                "true" if cfg.enable_optimize_write else "false"
            )
            settings["spark.databricks.delta.autoCompact.enabled"] = (
                "true" if cfg.enable_auto_compact else "false"
            )
            settings["spark.databricks.delta.schema.autoMerge.enabled"] = "false"

        for k, v in settings.items():
            try:
                self._spark.conf.set(k, v)
            except Exception:
                log.debug("[saltmill] Could not set %s", k, exc_info=True)

        log.info("[saltmill] applied %d Spark conf settings", len(settings))
        return settings

    def detect_worker_count(self) -> int:
        sc = self._spark.sparkContext
        try:
            return max(1, sc.defaultParallelism // self._config.cores_per_worker)
        except Exception:
            log.debug("[saltmill] Could not detect worker count", exc_info=True)
            return 4

    def detect_cores_per_worker(self) -> int:
        try:
            return int(self._spark.conf.get("spark.executor.cores", "8"))
        except Exception:
            return 8
