from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Callable, Optional

if TYPE_CHECKING:
    from pyspark.sql.types import StructType

_VALID_WRITE_MODES = frozenset({"overwrite", "append", "ignore", "error", "errorifexists"})
_VALID_LOG_LEVELS = frozenset({"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"})
_SALT_COL_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# Allowlist of CSV reader options that saltmill passes to Spark.
# Restricting to known-safe keys prevents callers from redirecting reads via
# options like recursiveFileLookup or modifiedBefore.
_ALLOWED_CSV_OPTIONS = frozenset({
    "header", "inferSchema", "sep", "delimiter", "encoding", "charset",
    "quote", "escape", "comment", "nullValue", "nanValue", "positiveInf",
    "negativeInf", "dateFormat", "timestampFormat", "timestampNTZFormat",
    "multiLine", "mode", "columnNameOfCorruptRecord", "emptyValue",
    "locale", "lineSep", "pathGlobFilter", "modifiedBefore", "modifiedAfter",
    "ignoreLeadingWhiteSpace", "ignoreTrailingWhiteSpace",
    "maxColumns", "maxCharsPerColumn", "unescapedQuoteHandling",
})

_SUPPORTED_SCHEMES = ("abfss://", "abfs://", "wasbs://", "dbfs:/", "s3://", "s3a://", "gs://", "file://", "/")


class WriteFormat(str, Enum):
    DELTA = "delta"
    PARQUET = "parquet"


class CompressionCodec(str, Enum):
    SNAPPY = "snappy"
    ZSTD = "zstd"
    GZIP = "gzip"
    NONE = "none"


@dataclass
class SaltmillConfig:
    # ── Input / Output ────────────────────────────────────────────────────────
    input_path: str
    # output_path is only required when calling SaltmillProcessor.process().
    # Leave empty when using the library purely for in-memory DataFrame transforms.
    output_path: str = ""

    # ── Schema ────────────────────────────────────────────────────────────────
    schema: Optional[StructType] = None
    schema_sample_fraction: float = 0.01
    schema_sample_max_rows: int = 100_000

    # ── Partition keys ────────────────────────────────────────────────────────
    partition_keys: Optional[list[str]] = None
    cardinality_sample_fraction: float = 0.05

    # ── Salting ───────────────────────────────────────────────────────────────
    salt_buckets: Optional[int] = None
    salt_column_name: str = "_salt"

    # ── Cluster ───────────────────────────────────────────────────────────────
    worker_count: Optional[int] = None
    cores_per_worker: int = 8

    # ── Spark tuning ──────────────────────────────────────────────────────────
    shuffle_partitions: Optional[int] = None
    max_partition_bytes_mb: int = 128
    enable_optimize_write: bool = True
    enable_auto_compact: bool = True
    enable_adaptive_query: bool = True

    # ── Write ─────────────────────────────────────────────────────────────────
    write_format: WriteFormat = WriteFormat.DELTA
    write_mode: str = "overwrite"
    compression: CompressionCodec = CompressionCodec.SNAPPY
    delta_partition_columns: Optional[list[str]] = None

    # ── Single-file splitting ─────────────────────────────────────────────────
    # When the input resolves to one large multiLine CSV (which Spark cannot
    # split across tasks), saltmill pre-splits it on the driver into many
    # record-aligned chunks so the read parallelises. No effect on multi-file
    # inputs or non-multiLine reads (Spark splits those natively).
    split_large_files: bool = True
    split_threshold_gb: float = 1.0
    target_chunk_size_mb: Optional[int] = None  # defaults to max_partition_bytes_mb
    staging_path: Optional[str] = None  # falls back to <checkpoint_path>/_saltmill_split

    # ── Fault tolerance ───────────────────────────────────────────────────────
    checkpoint_path: Optional[str] = None
    checkpoint_interval: int = 5

    # ── Observability ─────────────────────────────────────────────────────────
    log_level: str = "INFO"
    progress_callback: Optional[Callable[[str, float], None]] = None

    # ── Advanced CSV options ──────────────────────────────────────────────────
    csv_options: dict[str, str] = field(
        default_factory=lambda: {
            "header": "true",
            "inferSchema": "false",
            "mode": "PERMISSIVE",
            "columnNameOfCorruptRecord": "_corrupt_record",
        }
    )

    def __post_init__(self) -> None:
        if not self.input_path:
            raise ValueError("input_path must not be empty")
        if not 0 < self.schema_sample_fraction <= 1:
            raise ValueError("schema_sample_fraction must be in (0, 1]")
        if self.salt_buckets is not None and self.salt_buckets < 1:
            raise ValueError("salt_buckets must be >= 1")
        if self.split_threshold_gb <= 0:
            raise ValueError("split_threshold_gb must be > 0")
        if self.target_chunk_size_mb is not None and self.target_chunk_size_mb < 1:
            raise ValueError("target_chunk_size_mb must be >= 1")
        if self.write_mode.lower() not in _VALID_WRITE_MODES:
            raise ValueError(
                f"write_mode must be one of {sorted(_VALID_WRITE_MODES)}, got {self.write_mode!r}"
            )
        if self.log_level.upper() not in _VALID_LOG_LEVELS:
            raise ValueError(
                f"log_level must be one of {sorted(_VALID_LOG_LEVELS)}, got {self.log_level!r}"
            )
        if not self.salt_column_name or not _SALT_COL_RE.match(self.salt_column_name):
            raise ValueError(
                f"salt_column_name must match [A-Za-z_][A-Za-z0-9_]*, "
                f"got {self.salt_column_name!r}"
            )
        self._validate_path_scheme("input_path", self.input_path)
        if self.output_path:
            self._validate_path_scheme("output_path", self.output_path)
        if self.checkpoint_path:
            self._validate_path_scheme("checkpoint_path", self.checkpoint_path)
        if self.staging_path:
            self._validate_path_scheme("staging_path", self.staging_path)
        unknown_opts = set(self.csv_options) - _ALLOWED_CSV_OPTIONS
        if unknown_opts:
            raise ValueError(
                f"csv_options contains unrecognised keys: {sorted(unknown_opts)}. "
                f"Allowed keys: {sorted(_ALLOWED_CSV_OPTIONS)}"
            )

    def _validate_path_scheme(self, field_name: str, path: str) -> None:
        stripped = path.strip()
        if not any(stripped.startswith(s) for s in _SUPPORTED_SCHEMES):
            raise ValueError(
                f"{field_name} has unsupported scheme: {stripped!r}. "
                f"Supported: {_SUPPORTED_SCHEMES}"
            )
