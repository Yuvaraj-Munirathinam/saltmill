"""
Spark runtime-environment helpers for cross-cluster compatibility.

Databricks shared and serverless clusters run Spark Connect, where the driver
JVM is sandboxed — ``spark.sparkContext``, ``spark._jvm`` and ``sc._jsc`` all
raise. Single-user and job clusters expose the JVM as usual.

This module centralises:
  * ``has_jvm``     — is direct JVM access available on this session?
  * ``list_data_files`` / ``total_size_bytes`` — file metadata via the
    ``binaryFile`` datasource, which works on every cluster type (it reads only
    path/length, never file content).

Features that require the JVM (checkpointing, the runtime watchdog) call
``has_jvm`` and degrade gracefully when it returns False.
"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

log = logging.getLogger("saltmill")

# Data files never start with these (Spark commit markers, CRCs, hidden files).
_NON_DATA_PREFIXES = (".", "_")


def has_jvm(spark: "SparkSession") -> bool:
    """True when the driver JVM is directly accessible (single-user/job cluster).

    Returns False on Spark Connect (shared/serverless), where accessing
    ``sparkContext`` raises JVM_ATTRIBUTE_NOT_SUPPORTED.
    """
    try:
        return spark.sparkContext is not None
    except Exception:
        return False


def list_data_files(spark: "SparkSession", path: str) -> list[tuple[str, int]]:
    """Return ``(path, size_bytes)`` for real data files at ``path``.

    Uses the ``binaryFile`` datasource, selecting only ``path`` and ``length``
    (never ``content``), so it lists metadata cheaply and works on Spark
    Connect. Hidden/metadata files are excluded.
    """
    df = spark.read.format("binaryFile").load(path).select("path", "length")
    out: list[tuple[str, int]] = []
    for row in df.collect():
        p = row["path"]
        name = p.rstrip("/").split("/")[-1]
        if name.startswith(_NON_DATA_PREFIXES):
            continue
        out.append((p, int(row["length"])))
    return out


def total_size_bytes(spark: "SparkSession", path: str) -> int:
    """Total size in bytes of the data files at ``path`` (0 on failure)."""
    try:
        return sum(size for _, size in list_data_files(spark, path))
    except Exception:
        log.debug("[saltmill] could not determine size of %s", path, exc_info=True)
        return 0
