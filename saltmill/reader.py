from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from saltmill.exceptions import UnsupportedPathError

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.types import StructType

    from saltmill.config import SaltmillConfig

log = logging.getLogger("saltmill")

_SUPPORTED_SCHEMES = ("s3://", "s3a://", "abfss://", "gs://", "dbfs:/", "file://", "/")


class CsvReader:
    def __init__(self, spark: "SparkSession", config: "SaltmillConfig") -> None:
        self._spark = spark
        self._config = config

    def read(self, schema: "StructType", paths: "list[str] | None" = None) -> "DataFrame":
        """Read CSV file(s). Uses config.input_path when paths is not provided."""
        cfg = self._config
        read_paths = paths or [cfg.input_path]
        normalized = [self._normalize_path(p) for p in read_paths]
        log.info("[saltmill] reading CSV from %s", normalized)

        options = {**cfg.csv_options, "inferSchema": "false"}
        df = self._spark.read.schema(schema).options(**options).csv(normalized)
        log.info("[saltmill] CSV reader configured (schema applied, no full-scan inference)")
        return df

    def estimate_size_gb(self) -> float:
        """Sum file sizes via Hadoop FileSystem API. Returns 0.0 on failure."""
        cfg = self._config
        try:
            jvm = self._spark._jvm  # type: ignore[attr-defined]
            sc = self._spark.sparkContext
            hadoop_conf = sc._jsc.hadoopConfiguration()  # type: ignore[attr-defined]
            path_obj = jvm.org.apache.hadoop.fs.Path(cfg.input_path)
            fs = path_obj.getFileSystem(hadoop_conf)
            status_list = fs.globStatus(path_obj)
            if status_list is None:
                return 0.0
            return sum(s.getLen() for s in status_list) / (1024**3)
        except Exception:
            log.debug("[saltmill] Could not estimate file size via Hadoop FS", exc_info=True)
            return 0.0

    def _normalize_path(self, path: str) -> str:
        stripped = path.strip()
        if not any(stripped.startswith(s) for s in _SUPPORTED_SCHEMES):
            raise UnsupportedPathError(
                f"Unsupported path scheme: {stripped!r}. Supported: {_SUPPORTED_SCHEMES}"
            )
        return stripped
