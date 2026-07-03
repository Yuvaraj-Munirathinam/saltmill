"""Tests for the small-file fast-path decision (no Spark needed)."""
from saltmill.config import SaltmillConfig
from saltmill.processor import SaltmillProcessor


class _StubReader:
    def __init__(self, size_gb):
        self._size = size_gb

    def estimate_size_gb(self):
        return self._size


def _proc(**kw):
    return SaltmillProcessor(SaltmillConfig(input_path="/data/x.csv", **kw))


def test_small_input_detected():
    assert _proc(min_tuning_size_gb=1.0)._is_small_input(_StubReader(0.01)) is True


def test_large_input_not_small():
    assert _proc(min_tuning_size_gb=1.0)._is_small_input(_StubReader(5.0)) is False


def test_zero_size_not_small():
    # Unknown size (0.0) must not be treated as small — never skip tuning silently.
    assert _proc(min_tuning_size_gb=1.0)._is_small_input(_StubReader(0.0)) is False


def test_explicit_salt_buckets_disables_fast_path():
    assert _proc(salt_buckets=8)._is_small_input(_StubReader(0.01)) is False


def test_min_tuning_zero_disables_fast_path():
    # min_tuning_size_gb=0 means "always tune".
    assert _proc(min_tuning_size_gb=0)._is_small_input(_StubReader(0.01)) is False


def test_small_plan_has_no_repartition():
    plan = _proc()._small_plan(SaltmillConfig(input_path="/data/x.csv"))
    assert plan.salt_buckets == 1
    assert plan.target_partitions == 0
    assert plan.skew_reports == []
