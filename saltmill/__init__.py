"""
saltmill — Efficient large-CSV processing for Apache Spark / Databricks.

Auto-detects skew, tunes salt buckets, partition keys, and Spark config
for 500GB+ CSV files.

Quick start (simple API)::

    import saltmill
    df = saltmill.read(spark, "abfss://container@account.dfs.core.windows.net/data/large.csv")

Advanced API (full pipeline with write)::

    from saltmill import SaltmillProcessor, SaltmillConfig

    result = SaltmillProcessor(SaltmillConfig(
        input_path="abfss://raw@account.dfs.core.windows.net/data/*.csv",
        output_path="abfss://curated@account.dfs.core.windows.net/output/delta/",
    )).process()
"""

from saltmill._version import __version__

# ── Simple backward-compatible API ────────────────────────────────────────────
from saltmill.compat import SaltMill, read

# ── Advanced API ──────────────────────────────────────────────────────────────
from saltmill.config import CompressionCodec, SaltmillConfig, WriteFormat
from saltmill.exceptions import (
    CheckpointError,
    ConfigurationError,
    ProcessingTimeoutError,
    SaltmillError,
    SchemaInferenceError,
    SkewDetectionError,
    UnsupportedPathError,
)
from saltmill.models import PartitionPlan, ProcessingResult, SchemaInfo, SkewReport
from saltmill.processor import SaltmillProcessor

__all__ = [
    "CheckpointError",
    "CompressionCodec",
    "ConfigurationError",
    "PartitionPlan",
    "ProcessingResult",
    "ProcessingTimeoutError",
    # Simple API
    "SaltMill",
    "SaltmillConfig",
    "SaltmillError",
    # Advanced API
    "SaltmillProcessor",
    "SchemaInferenceError",
    "SchemaInfo",
    "SkewDetectionError",
    "SkewReport",
    "UnsupportedPathError",
    "WriteFormat",
    "__version__",
    "read",
]
