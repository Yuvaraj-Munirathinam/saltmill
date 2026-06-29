"""
AutoTuner: computes optimal salt_buckets, num_partitions, and maxPartitionBytes
based on file size, cluster parallelism, and environment.
"""

from __future__ import annotations
import math
from dataclasses import dataclass


# Tuning constants
_BYTES_PER_BUCKET = 8 * 1024 ** 3   # 1 bucket per 8 GB of raw CSV
_SALT_MIN = 8
_SALT_MAX = 512
_PARTITION_MULTIPLIER = 10            # partitions = buckets * this
_MAX_PARTITION_BYTES = 64 * 1024 * 1024  # 64 MB target partition size


@dataclass(frozen=True)
class TuningParams:
    salt_buckets: int
    num_partitions: int
    max_partition_bytes: int
    shuffle_partitions: int
    file_size_bytes: int | None
    workers: int

    def summary(self) -> str:
        size_str = (
            f"{self.file_size_bytes / 1024**3:.1f} GB"
            if self.file_size_bytes
            else "unknown"
        )
        return (
            f"saltmill tuning → file: {size_str}, workers: {self.workers}, "
            f"salt_buckets: {self.salt_buckets}, partitions: {self.num_partitions}, "
            f"maxPartitionBytes: {self.max_partition_bytes // 1024**2} MB"
        )


def compute_tuning(
    file_size_bytes: int | None,
    workers: int,
    salt_buckets_override: int | None = None,
    num_partitions_override: int | None = None,
) -> TuningParams:
    buckets = salt_buckets_override or _compute_salt_buckets(file_size_bytes, workers)
    partitions = num_partitions_override or _compute_partitions(buckets, workers)
    return TuningParams(
        salt_buckets=buckets,
        num_partitions=partitions,
        max_partition_bytes=_MAX_PARTITION_BYTES,
        shuffle_partitions=partitions,
        file_size_bytes=file_size_bytes,
        workers=workers,
    )


def _compute_salt_buckets(file_size_bytes: int | None, workers: int) -> int:
    if file_size_bytes:
        raw = file_size_bytes / _BYTES_PER_BUCKET
        clamped = max(_SALT_MIN, min(_SALT_MAX, raw))
        return _round_to_power_of_2(int(math.ceil(clamped)))
    # Fallback: derive from worker count so work spreads evenly
    return _round_to_power_of_2(max(_SALT_MIN, workers))


def _compute_partitions(salt_buckets: int, workers: int) -> int:
    base = salt_buckets * _PARTITION_MULTIPLIER
    if workers > 0:
        # Round up so every worker gets an equal share
        return math.ceil(base / workers) * workers
    return base


def _round_to_power_of_2(n: int) -> int:
    if n <= 1:
        return 1
    return 2 ** math.ceil(math.log2(n))
