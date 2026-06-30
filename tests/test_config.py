"""Tests for SaltmillConfig validation."""
import pytest
from saltmill.config import SaltmillConfig, WriteFormat, CompressionCodec


def test_minimal_config():
    cfg = SaltmillConfig(input_path="abfss://raw@account.dfs.core.windows.net/data/data.csv")
    assert cfg.output_path == ""
    assert cfg.write_format == WriteFormat.DELTA
    assert cfg.compression == CompressionCodec.SNAPPY


def test_empty_input_path_raises():
    with pytest.raises(ValueError, match="input_path"):
        SaltmillConfig(input_path="")


def test_invalid_sample_fraction_raises():
    with pytest.raises(ValueError, match="schema_sample_fraction"):
        SaltmillConfig(input_path="/data/x.csv", schema_sample_fraction=0.0)


def test_invalid_salt_buckets_raises():
    with pytest.raises(ValueError, match="salt_buckets"):
        SaltmillConfig(input_path="/data/x.csv", salt_buckets=0)


def test_output_path_optional():
    cfg = SaltmillConfig(input_path="abfss://raw@account.dfs.core.windows.net/data/data.csv", output_path="")
    assert cfg.output_path == ""


# ── Security validation tests ─────────────────────────────────────────────────

def test_invalid_write_mode_raises():
    with pytest.raises(ValueError, match="write_mode"):
        SaltmillConfig(input_path="/data/x.csv", write_mode="upsert")


def test_valid_write_modes_accepted():
    for mode in ("overwrite", "append", "ignore", "error", "errorifexists"):
        cfg = SaltmillConfig(input_path="/data/x.csv", write_mode=mode)
        assert cfg.write_mode == mode


def test_invalid_log_level_raises():
    with pytest.raises(ValueError, match="log_level"):
        SaltmillConfig(input_path="/data/x.csv", log_level="VERBOSE")


def test_valid_log_levels_accepted():
    for level in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"):
        cfg = SaltmillConfig(input_path="/data/x.csv", log_level=level)
        assert cfg.log_level == level


def test_unsupported_input_path_scheme_raises():
    with pytest.raises(ValueError, match="unsupported scheme"):
        SaltmillConfig(input_path="ftp://host/file.csv")


def test_unsupported_output_path_scheme_raises():
    with pytest.raises(ValueError, match="unsupported scheme"):
        SaltmillConfig(input_path="/data/x.csv", output_path="ftp://host/out/")


def test_unsupported_checkpoint_path_scheme_raises():
    with pytest.raises(ValueError, match="unsupported scheme"):
        SaltmillConfig(input_path="/data/x.csv", checkpoint_path="http://host/cp/")


def test_relative_output_path_raises():
    with pytest.raises(ValueError, match="unsupported scheme"):
        SaltmillConfig(input_path="/data/x.csv", output_path="../../prod/out")


def test_from_dict_unknown_key_raises():
    from saltmill.processor import SaltmillProcessor
    with pytest.raises(ValueError, match="Unknown config keys"):
        SaltmillProcessor.from_dict({"input_path": "/data/x.csv", "progress_callback": "evil"})


def test_from_dict_valid():
    from saltmill.processor import SaltmillProcessor
    proc = SaltmillProcessor.from_dict({"input_path": "/data/x.csv", "write_mode": "append"})
    assert proc._config.write_mode == "append"


# ── Single-file splitting config ──────────────────────────────────────────────

def test_split_defaults():
    cfg = SaltmillConfig(input_path="/data/x.csv")
    assert cfg.split_large_files is True
    assert cfg.split_threshold_gb == 1.0
    assert cfg.target_chunk_size_mb is None
    assert cfg.staging_path is None


def test_invalid_split_threshold_raises():
    with pytest.raises(ValueError, match="split_threshold_gb"):
        SaltmillConfig(input_path="/data/x.csv", split_threshold_gb=0)


def test_invalid_target_chunk_size_raises():
    with pytest.raises(ValueError, match="target_chunk_size_mb"):
        SaltmillConfig(input_path="/data/x.csv", target_chunk_size_mb=0)


def test_staging_path_scheme_validated():
    with pytest.raises(ValueError, match="unsupported scheme"):
        SaltmillConfig(input_path="/data/x.csv", staging_path="ftp://host/staging/")


def test_from_dict_accepts_split_keys():
    from saltmill.processor import SaltmillProcessor
    proc = SaltmillProcessor.from_dict({
        "input_path": "/data/x.csv",
        "split_threshold_gb": 5.0,
        "staging_path": "/data/staging/",
    })
    assert proc._config.split_threshold_gb == 5.0
    assert proc._config.staging_path == "/data/staging/"


# ── Runaway-cost guardrails ───────────────────────────────────────────────────

def test_guardrail_defaults():
    cfg = SaltmillConfig(input_path="/data/x.csv")
    assert cfg.split_max_file_gb == 50.0
    assert cfg.max_split_chunks == 100_000
    assert cfg.max_runtime_seconds is None
    assert cfg.count_output_rows is True


def test_max_file_below_threshold_raises():
    with pytest.raises(ValueError, match="split_max_file_gb must be >="):
        SaltmillConfig(input_path="/data/x.csv", split_threshold_gb=10.0, split_max_file_gb=5.0)


def test_invalid_max_split_chunks_raises():
    with pytest.raises(ValueError, match="max_split_chunks"):
        SaltmillConfig(input_path="/data/x.csv", max_split_chunks=0)


def test_invalid_max_runtime_seconds_raises():
    with pytest.raises(ValueError, match="max_runtime_seconds"):
        SaltmillConfig(input_path="/data/x.csv", max_runtime_seconds=0)


def test_valid_max_runtime_seconds():
    cfg = SaltmillConfig(input_path="/data/x.csv", max_runtime_seconds=3600)
    assert cfg.max_runtime_seconds == 3600
