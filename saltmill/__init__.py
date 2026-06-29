"""
saltmill — Efficient large-CSV processing for Apache Spark / Databricks.

Auto-detects skew, tunes salt buckets, partition keys, and Spark config
for 500GB+ CSV files.

Quick start (simple API)::

    import saltmill
    df = saltmill.read(spark, "s3://bucket/large.csv")

Advanced API (full pipeline with write)::

    from saltmill import SaltmillProcessor, SaltmillConfig

    result = SaltmillProcessor(SaltmillConfig(
        input_path="s3://bucket/data/*.csv",
        output_path="s3://bucket/output/delta/",
    )).process()
"""

from saltmill._version import __version__

# ── Advanced API ──────────────────────────────────────────────────────────────
from saltmill.config import CompressionCodec, SaltmillConfig, WriteFormat
from saltmill.exceptions import (
    CheckpointError,
    ConfigurationError,
    SaltmillError,
    SchemaInferenceError,
    SkewDetectionError,
    UnsupportedPathError,
)
from saltmill.models import PartitionPlan, ProcessingResult, SchemaInfo, SkewReport
from saltmill.processor import SaltmillProcessor

# ── Simple backward-compatible API ────────────────────────────────────────────
from saltmill.compat import SaltMill, read

__all__ = [
    # Advanced API
    "SaltmillProcessor",
    "SaltmillConfig",
    "WriteFormat",
    "CompressionCodec",
    "ProcessingResult",
    "PartitionPlan",
    "SchemaInfo",
    "SkewReport",
    "SaltmillError",
    "ConfigurationError",
    "SchemaInferenceError",
    "SkewDetectionError",
    "CheckpointError",
    "UnsupportedPathError",
    # Simple API
    "SaltMill",
    "read",
    "__version__",
]
