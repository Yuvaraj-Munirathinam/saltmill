from __future__ import annotations


class SaltmillError(Exception):
    """Base exception for all saltmill errors."""


class ConfigurationError(SaltmillError):
    """Invalid or incompatible configuration."""


class SchemaInferenceError(SaltmillError):
    """Failed to infer schema from CSV sample."""


class SkewDetectionError(SaltmillError):
    """Failed to analyze skew for partition keys."""


class CheckpointError(SaltmillError):
    """Checkpoint read or write failed."""


class UnsupportedPathError(SaltmillError):
    """Path scheme not recognized or not accessible."""
