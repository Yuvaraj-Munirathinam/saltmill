from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

    from saltmill.config import SaltmillConfig
    from saltmill.models import PartitionPlan

log = logging.getLogger("saltmill")


class CsvWriter:
    def __init__(self, spark: "SparkSession", config: "SaltmillConfig") -> None:
        self._spark = spark
        self._config = config

    def write(self, df: "DataFrame", plan: "PartitionPlan") -> int:
        """Write DataFrame to output_path. Returns estimated file count."""
        cfg = self._config
        if not cfg.output_path:
            raise ValueError("output_path must be set before calling write()")

        log.debug("[saltmill] writing to %s format=%s mode=%s", cfg.output_path, cfg.write_format.value, cfg.write_mode)
        log.info("[saltmill] writing format=%s mode=%s", cfg.write_format.value, cfg.write_mode)

        writer = (
            df.write.format(cfg.write_format.value)
            .mode(cfg.write_mode)
            .option("compression", cfg.compression.value)
        )

        from saltmill.config import WriteFormat

        if cfg.write_format == WriteFormat.DELTA:
            writer = writer.option("mergeSchema", "false")

        partition_cols = cfg.delta_partition_columns or []
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
            log.info("[saltmill] Delta partitioned by %s", partition_cols)

        writer.save(cfg.output_path)
        file_count = self._count_output_files(cfg.output_path)
        log.info("[saltmill] write complete: ~%d files written", file_count)
        return file_count

    def _count_output_files(self, output_path: str) -> int:
        """Count data files written. Returns -1 if it can't be determined.

        Uses the binaryFile datasource (metadata only), which works on every
        cluster type including Spark Connect (shared/serverless).
        """
        from saltmill.spark_env import list_data_files

        try:
            return len(list_data_files(self._spark, output_path))
        except Exception:
            log.debug("[saltmill] Could not count output files", exc_info=True)
            return -1
