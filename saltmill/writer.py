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

        log.info(
            "[saltmill] writing to %s format=%s mode=%s",
            cfg.output_path, cfg.write_format.value, cfg.write_mode,
        )

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
        try:
            jvm = self._spark._jvm  # type: ignore[attr-defined]
            sc = self._spark.sparkContext
            hadoop_conf = sc._jsc.hadoopConfiguration()  # type: ignore[attr-defined]
            path_obj = jvm.org.apache.hadoop.fs.Path(output_path)
            fs = path_obj.getFileSystem(hadoop_conf)
            statuses = fs.listStatus(path_obj)
            return sum(
                1 for s in (statuses or [])
                if not s.isDirectory()
                and not s.getPath().getName().startswith(("_", "."))
            )
        except Exception:
            log.debug("[saltmill] Could not count output files", exc_info=True)
            return -1
