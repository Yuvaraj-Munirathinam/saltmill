"""Tests for the AutoTuner — no Spark required."""

import pytest
from saltmill.optimizer import compute_tuning, _round_to_power_of_2


def test_round_to_power_of_2():
    assert _round_to_power_of_2(1) == 1
    assert _round_to_power_of_2(5) == 8
    assert _round_to_power_of_2(64) == 64
    assert _round_to_power_of_2(65) == 128


def test_500gb_file():
    size = 500 * 1024 ** 3  # 500 GB
    params = compute_tuning(file_size_bytes=size, workers=64)
    # 500 GB / 8 GB = 62.5 → rounds up to 64
    assert params.salt_buckets == 64
    assert params.num_partitions == 640
    assert params.num_partitions % params.salt_buckets == 0


def test_small_file_clamps_to_minimum():
    size = 1 * 1024 ** 3  # 1 GB
    params = compute_tuning(file_size_bytes=size, workers=4)
    assert params.salt_buckets >= 8


def test_very_large_file_clamps_to_maximum():
    size = 10_000 * 1024 ** 3  # 10 TB
    params = compute_tuning(file_size_bytes=size, workers=128)
    assert params.salt_buckets <= 512


def test_no_size_falls_back_to_workers():
    params = compute_tuning(file_size_bytes=None, workers=32)
    assert params.salt_buckets >= 8
    assert params.num_partitions > 0


def test_overrides_respected():
    size = 100 * 1024 ** 3
    params = compute_tuning(
        file_size_bytes=size,
        workers=16,
        salt_buckets_override=32,
        num_partitions_override=999,
    )
    assert params.salt_buckets == 32
    assert params.num_partitions == 999


def test_partitions_multiple_of_workers():
    size = 200 * 1024 ** 3
    workers = 24
    params = compute_tuning(file_size_bytes=size, workers=workers)
    assert params.num_partitions % workers == 0


def test_summary_contains_key_info():
    params = compute_tuning(file_size_bytes=500 * 1024 ** 3, workers=64)
    s = params.summary()
    assert "500.0 GB" in s
    assert "salt_buckets" in s
    assert "partitions" in s
