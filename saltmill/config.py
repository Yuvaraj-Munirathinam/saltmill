from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Callable, Optional

if TYPE_CHECKING:
    from pyspark.sql.types import StructType


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
