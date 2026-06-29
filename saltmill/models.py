from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql.types import StructType


@dataclass(frozen=True)
class SchemaInfo:
    schema: StructType
    inferred: bool
    sample_rows: int
    inference_duration_seconds: float
    nullable_columns: list[str]


@dataclass(frozen=True)
class SkewReport:
    column: str
    total_rows_sampled: int
    top_value_frequency: float
    recommended_salt_buckets: int
    skew_detected: bool


@dataclass(frozen=True)
class PartitionPlan:
    partition_keys: list[str]
    salt_buckets: int
    target_partitions: int
    shuffle_partitions: int
    estimated_partition_size_mb: float
    skew_reports: list[SkewReport]


@dataclass
class ProcessingResult:
    input_path: str
    output_path: str
    schema_info: SchemaInfo
    partition_plan: PartitionPlan
    total_rows: int
    total_files_written: int
    duration_seconds: float
    checkpoint_used: bool
    spark_conf_applied: dict[str, str]
    warnings: list[str] = field(default_factory=list)
