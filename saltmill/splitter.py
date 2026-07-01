"""
Splitting of a single large multiLine CSV into many files, so the read
parallelises.

Spark cannot split a single CSV across tasks when ``multiLine=true`` — a record
may span several physical lines, so the whole file is read by one task. This
module re-emits that one file as N files under a staging path; Spark then reads
the N files in parallel (multiLine still prevents splitting *within* a file, but
N files run on up to N tasks).

The split is **Spark-native**: it uses only the DataFrame API (read → repartition
→ write), so it works on every Databricks cluster type — single-user, job,
shared, and serverless (Spark Connect). Record integrity is guaranteed by
Spark's own CSV reader/writer: rows are parsed correctly under multiLine and are
never split across output files.

The decision logic (:func:`plan_split`) is a pure function and unit-testable
without Spark.
"""
from __future__ import annotations

import logging
import math
from typing import TYPE_CHECKING, Optional

from saltmill.exceptions import ConfigurationError

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from saltmill.config import SaltmillConfig

log = logging.getLogger("saltmill")

_GB = 1024 ** 3

# CSV options that are meaningful when writing the staged chunk files.
_CSV_WRITE_OPTIONS = frozenset({
    "header", "sep", "delimiter", "quote", "escape", "encoding", "charset",
    "nullValue", "emptyValue", "dateFormat", "timestampFormat", "lineSep",
    "quoteAll", "escapeQuotes",
})


def plan_split(cfg: "SaltmillConfig", data_files: list[tuple[str, int]]) -> tuple[str, Optional[str], Optional[int]]:
    """Pure split decision (no Spark/IO) so it is unit-testable.

    Returns one of:
      ("split", path, size) — one large multiLine file that should be split
      ("skip",  None, None) — leave the input to Spark (multi-file / non-multiLine / small)
    Raises ConfigurationError when a single file exceeds split_max_file_gb.
    """
    if not cfg.split_large_files:
        return ("skip", None, None)
    if len(data_files) != 1:
        # 0 → downstream read raises a clear error; >1 → already parallelisable.
        return ("skip", None, None)
    multiline = str(cfg.csv_options.get("multiLine", "false")).lower() == "true"
    if not multiline:
        # Spark splits a single non-multiLine CSV natively via maxPartitionBytes.
        return ("skip", None, None)
    path, size = data_files[0]
    size_gb = size / _GB
    if size_gb < cfg.split_threshold_gb:
        return ("skip", None, None)
    if size_gb > cfg.split_max_file_gb:
        raise ConfigurationError(
            f"Single multiLine file is {size_gb:.1f} GB, above split_max_file_gb="
            f"{cfg.split_max_file_gb} GB. Splitting reads the file once in a single "
            "task (multiLine cannot be parallelised); above this size pre-split the "
            "file upstream, or raise split_max_file_gb if you accept the cost."
        )
    return ("split", path, size)


def projected_chunk_count(file_size: int, target_bytes: int) -> int:
    """Number of chunks a file of ``file_size`` yields at ``target_bytes``."""
    if target_bytes <= 0:
        raise ValueError("target_bytes must be > 0")
    return max(1, math.ceil(file_size / target_bytes))


class FileSplitter:
    """Inspects an input path and, when warranted, splits one large multiLine
    CSV into N files under a staging directory — using only the DataFrame API,
    so it runs on all cluster types."""

    def __init__(self, spark: "SparkSession", config: "SaltmillConfig") -> None:
        self._spark = spark
        self._config = config

    def _resolve_staging_path(self) -> str:
        cfg = self._config
        if cfg.staging_path:
            return cfg.staging_path.rstrip("/")
        if cfg.checkpoint_path:
            return f"{cfg.checkpoint_path.rstrip('/')}/_saltmill_split"
        raise ConfigurationError(
            "A single large multiLine CSV needs splitting, but no staging location "
            "is configured. Set SaltmillConfig.staging_path or checkpoint_path."
        )

    def split(self, file_path: str, file_size: int) -> str:
        """Split ``file_path`` into N files under the staging dir; return that dir."""
        cfg = self._config
        staging = self._resolve_staging_path()
        target_mb = cfg.target_chunk_size_mb or cfg.max_partition_bytes_mb
        target_bytes = target_mb * 1024 * 1024

        n_chunks = projected_chunk_count(file_size, target_bytes)
        if n_chunks > cfg.max_split_chunks:
            raise ConfigurationError(
                f"Splitting this file at {target_mb} MB chunks would create ~{n_chunks} "
                f"files, above max_split_chunks={cfg.max_split_chunks}. Increase "
                "target_chunk_size_mb to avoid a small-file explosion."
            )

        log.info(
            "[saltmill] splitting single %.2f GB multiLine file into %d file(s) at %s",
            file_size / _GB, n_chunks, staging,
        )

        # Read the one file (single task under multiLine — correct parse), then
        # repartition and write N files. Spark preserves whole records per file.
        read_opts = {**cfg.csv_options, "inferSchema": "false"}
        df = self._spark.read.options(**read_opts).csv(file_path)

        write_opts = {k: v for k, v in cfg.csv_options.items() if k in _CSV_WRITE_OPTIONS}
        (
            df.repartition(n_chunks)
            .write.mode("overwrite")
            .options(**write_opts)
            .csv(staging)
        )

        log.info("[saltmill] split complete: staged to %s", staging)
        return staging

    def maybe_split(self) -> Optional[str]:
        """If the configured input warrants splitting, do it and return the
        staging path; otherwise return None (caller keeps the original path).

        Raises ConfigurationError when a single multiLine file exceeds
        split_max_file_gb."""
        from saltmill.spark_env import list_data_files

        try:
            data_files = list_data_files(self._spark, self._config.input_path)
        except Exception:
            log.debug(
                "[saltmill] could not list input files; skipping split", exc_info=True
            )
            return None

        action, path, size = plan_split(self._config, data_files)
        if action == "skip":
            if len(data_files) > 1:
                log.info(
                    "[saltmill] %d input files detected; reading in parallel without splitting",
                    len(data_files),
                )
            return None
        assert path is not None and size is not None
        return self.split(path, size)
