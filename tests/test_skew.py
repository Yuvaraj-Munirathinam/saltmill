"""Tests for SkewDetector — no Spark required (uses mock DataFrames)."""
from unittest.mock import MagicMock, patch

import pytest

from saltmill.skew import SkewDetector, _next_power_of_two
from saltmill.config import SaltmillConfig
from saltmill.models import SkewReport


def _cfg(**kwargs):
    defaults = dict(input_path="/data/test.csv", worker_count=8, cores_per_worker=4)
    defaults.update(kwargs)
    return SaltmillConfig(**defaults)


def test_next_power_of_two():
    assert _next_power_of_two(1) == 1
    assert _next_power_of_two(3) == 4
    assert _next_power_of_two(8) == 8
    assert _next_power_of_two(9) == 16


def test_analyze_empty_df():
    spark = MagicMock()
    cfg = _cfg()
    detector = SkewDetector(spark, cfg)

    mock_df = MagicMock()
    mock_df.count.return_value = 0

    reports = detector.analyze(mock_df, ["region"])
    assert len(reports) == 1
    assert reports[0].skew_detected is False
    assert reports[0].recommended_salt_buckets == 1


def test_pick_salt_buckets_returns_max():
    spark = MagicMock()
    cfg = _cfg()
    detector = SkewDetector(spark, cfg)

    reports = [
        SkewReport("a", 1000, 0.05, 4, True),
        SkewReport("b", 1000, 0.40, 16, True),
        SkewReport("c", 1000, 0.02, 1, False),
    ]
    assert detector.pick_salt_buckets(reports) == 16


def test_pick_salt_buckets_empty():
    spark = MagicMock()
    cfg = _cfg()
    assert SkewDetector(spark, cfg).pick_salt_buckets([]) == 1


def test_recommend_buckets_no_skew():
    spark = MagicMock()
    cfg = _cfg(worker_count=8, cores_per_worker=4)
    detector = SkewDetector(spark, cfg)
    # top_freq=0.05 ≤ threshold → no skew → 1 bucket
    assert detector._recommend_buckets(0.05) == 1


def test_recommend_buckets_high_skew():
    spark = MagicMock()
    cfg = _cfg(worker_count=8, cores_per_worker=4)
    detector = SkewDetector(spark, cfg)
    # top_freq=0.80 > 0.60 → max(16, 32) = 32 → next power of 2 = 32
    assert detector._recommend_buckets(0.80) == 32
