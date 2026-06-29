"""Tests for SaltmillConfig validation."""
import pytest
from saltmill.config import SaltmillConfig, WriteFormat, CompressionCodec


def test_minimal_config():
    cfg = SaltmillConfig(input_path="s3://bucket/data.csv")
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
    cfg = SaltmillConfig(input_path="s3://bucket/data.csv", output_path="")
    assert cfg.output_path == ""
