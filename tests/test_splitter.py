"""Tests for the split-decision logic (pure, no Spark required)."""
import pytest

from saltmill.config import SaltmillConfig
from saltmill.exceptions import ConfigurationError
from saltmill.splitter import plan_split, projected_chunk_count

_GB = 1024 ** 3


def _cfg(**kw):
    base = dict(input_path="/data/x.csv", csv_options={"header": "true", "multiLine": "true"})
    base.update(kw)
    return SaltmillConfig(**base)


# ── plan_split ────────────────────────────────────────────────────────────────

def test_single_large_multiline_splits():
    action, path, size = plan_split(_cfg(), [("/data/big.csv", int(3 * _GB))])
    assert action == "split"
    assert path == "/data/big.csv"
    assert size == int(3 * _GB)


def test_multi_file_skips():
    files = [("/data/a.csv", int(3 * _GB)), ("/data/b.csv", int(3 * _GB))]
    assert plan_split(_cfg(), files)[0] == "skip"


def test_non_multiline_skips():
    cfg = _cfg(csv_options={"header": "true", "multiLine": "false"})
    assert plan_split(cfg, [("/data/big.csv", int(3 * _GB))])[0] == "skip"


def test_below_threshold_skips():
    cfg = _cfg(split_threshold_gb=5.0)
    assert plan_split(cfg, [("/data/big.csv", int(3 * _GB))])[0] == "skip"


def test_disabled_skips():
    cfg = _cfg(split_large_files=False)
    assert plan_split(cfg, [("/data/big.csv", int(3 * _GB))])[0] == "skip"


def test_zero_files_skips():
    assert plan_split(_cfg(), [])[0] == "skip"


def test_above_max_file_raises():
    cfg = _cfg(split_max_file_gb=10.0)
    with pytest.raises(ConfigurationError, match="above split_max_file_gb"):
        plan_split(cfg, [("/data/huge.csv", int(20 * _GB))])


# ── projected_chunk_count ─────────────────────────────────────────────────────

def test_projected_chunk_count():
    assert projected_chunk_count(0, 100) == 1
    assert projected_chunk_count(100, 100) == 1
    assert projected_chunk_count(101, 100) == 2
    assert projected_chunk_count(10 * _GB, 128 * 1024 * 1024) == 80


def test_projected_chunk_count_rejects_zero_target():
    with pytest.raises(ValueError):
        projected_chunk_count(100, 0)
