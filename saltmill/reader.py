from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from saltmill.exceptions import UnsupportedPathError

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.types import StructType

    from saltmill.config import SaltmillConfig

log = logging.getLogger("saltmill")

_SUPPORTED_SCHEMES = ("abfss://", "abfs://", "wasbs://", "dbfs:/", "s3://", "s3a://", "gs://", "file://", "/")


class CsvReader:
    def __init__(self, spark: "SparkSession", config: "SaltmillConfig") -> None:
        self._spark = spark
        self._config = config

    def read(self, schema: "StructType", paths: "list[str] | None" = None) -> "DataFrame":
        """Read CSV file(s). Uses config.input_path when paths is not provided."""
        cfg = self._config
        read_paths = paths or [cfg.input_path]
        normalized = [self._normalize_path(p) for p in read_paths]
        log.debug("[saltmill] reading CSV from %s", normalized)
        log.info("[saltmill] reading %d CSV path(s)", len(normalized))

        options = {**cfg.csv_options, "inferSchema": "false"}
        df = self._spark.read.schema(schema).options(**options).csv(normalized)
        log.info("[saltmill] CSV reader configured (schema applied, no full-scan inference)")
        return df

    def estimate_size_gb(self) -> float:
        """Estimate total input size in GB. Returns 0.0 on failure.

        Uses the binaryFile datasource (metadata only), which works on every
        cluster type including Spark Connect (shared/serverless).
        """
        from saltmill.spark_env import total_size_bytes

        return total_size_bytes(self._spark, self._config.input_path) / (1024**3)

    def _normalize_path(self, path: str) -> str:
        stripped = path.strip()
        if not any(stripped.startswith(s) for s in _SUPPORTED_SCHEMES):
            raise UnsupportedPathError(
                f"Unsupported path scheme: {stripped!r}. Supported: {_SUPPORTED_SCHEMES}"
            )
        return stripped
