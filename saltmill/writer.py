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
        """Write the DataFrame to a path or a Unity Catalog table.

        Returns the number of data files written (-1 for a managed table where
        that isn't readily determinable)."""
        cfg = self._config
        if not cfg.output_path and not cfg.table_name:
            raise ValueError("output_path or table_name must be set before calling write()")
        if cfg.table_name:
            return self._write_table(df)
        return self._write_path(df)

    def _base_writer(self, df: "DataFrame", partition_cols: list, force_overwrite_schema: bool):
        from saltmill.config import WriteFormat

        cfg = self._config
        writer = (
            df.write.format(cfg.write_format.value)
            .mode(cfg.write_mode)
            .option("compression", cfg.compression.value)
        )
        if cfg.write_format == WriteFormat.DELTA:
            writer = writer.option("mergeSchema", "false")
            overwriting = cfg.write_mode.lower() == "overwrite"
            if overwriting and (cfg.overwrite_schema or force_overwrite_schema):
                writer = writer.option("overwriteSchema", "true")
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
            log.info("[saltmill] partitioned by %s", partition_cols)
        return writer

    def _write_path(self, df: "DataFrame") -> int:
        cfg = self._config
        log.info(
            "[saltmill] writing to path %s format=%s mode=%s",
            cfg.output_path, cfg.write_format.value, cfg.write_mode,
        )
        partition_cols = cfg.delta_partition_columns or []
        self._base_writer(df, partition_cols, force_overwrite_schema=False).save(cfg.output_path)
        file_count = self._count_output_files(cfg.output_path)
        log.info("[saltmill] write complete: ~%d files written", file_count)
        return file_count

    def _write_table(self, df: "DataFrame") -> int:
        cfg = self._config
        partition_cols, force_overwrite_schema = self._resolve_table_partitioning()
        writer = self._base_writer(df, partition_cols, force_overwrite_schema)
        if cfg.output_path:
            # External table backed by the given location.
            writer = writer.option("path", cfg.output_path)
        log.info(
            "[saltmill] saveAsTable %s format=%s mode=%s partitionBy=%s%s",
            cfg.table_name, cfg.write_format.value, cfg.write_mode,
            partition_cols or "none",
            " (external)" if cfg.output_path else "",
        )
        writer.saveAsTable(cfg.table_name)
        # File count is meaningful only for an external table with a known path.
        return self._count_output_files(cfg.output_path) if cfg.output_path else -1

    def _resolve_table_partitioning(self) -> tuple[list, bool]:
        """Decide partition columns for a table write and whether an overwrite
        of the schema is required. Returns (partition_cols, force_overwrite_schema).

        - New table            → requested columns (partition on creation).
        - Exists, same layout  → requested columns.
        - Exists, differs/none → keep existing layout, unless
          repartition_existing_table is set (then overwrite to the new layout).
        """
        from saltmill.exceptions import ConfigurationError

        cfg = self._config
        requested = cfg.delta_partition_columns or []

        try:
            exists = self._spark.catalog.tableExists(cfg.table_name)
        except Exception:
            log.debug("[saltmill] could not check table existence", exc_info=True)
            exists = False

        if not exists:
            return requested, False  # new table → partition as requested

        try:
            existing = [
                c.name for c in self._spark.catalog.listColumns(cfg.table_name)
                if getattr(c, "isPartition", False)
            ]
        except Exception:
            log.debug("[saltmill] could not read existing partitioning", exc_info=True)
            existing = []

        if not requested:
            return [], False  # nothing requested → preserve existing
        if set(requested) == set(existing):
            return requested, False  # already consistent

        # Requested partitioning differs from the existing table (incl. unpartitioned).
        if cfg.repartition_existing_table:
            if cfg.write_mode.lower() != "overwrite":
                raise ConfigurationError(
                    "repartition_existing_table=True requires write_mode='overwrite' "
                    f"to change the partitioning of existing table {cfg.table_name!r}."
                )
            log.warning(
                "[saltmill] re-partitioning existing table %s from %s to %s (overwrite + overwriteSchema)",
                cfg.table_name, existing or "none", requested,
            )
            return requested, True  # force overwriteSchema to change layout

        log.warning(
            "[saltmill] table %s is partitioned by %s; requested %s ignored to preserve "
            "its layout. Set repartition_existing_table=True (with write_mode='overwrite') "
            "to change it.",
            cfg.table_name, existing or "none", requested,
        )
        return existing, False  # preserve existing layout

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
