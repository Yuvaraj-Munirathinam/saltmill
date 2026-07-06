"""Tests for Unity Catalog table-write partitioning decisions (no Spark)."""
import pytest

from saltmill.config import SaltmillConfig
from saltmill.exceptions import ConfigurationError
from saltmill.writer import CsvWriter


class _Col:
    def __init__(self, name, is_partition):
        self.name = name
        self.isPartition = is_partition


class _FakeCatalog:
    def __init__(self, exists, partition_cols):
        self._exists = exists
        self._cols = partition_cols

    def tableExists(self, name):  # noqa: N802 (Spark API name)
        return self._exists

    def listColumns(self, name):  # noqa: N802
        cols = [_Col("id", False), _Col("amount", False)]
        cols += [_Col(c, True) for c in self._cols]
        return cols


class _FakeSpark:
    def __init__(self, exists, partition_cols):
        self.catalog = _FakeCatalog(exists, partition_cols)


def _writer(spark, **cfg_kw):
    cfg = SaltmillConfig(input_path="/data/x.csv", table_name="cat.sch.t", **cfg_kw)
    return CsvWriter(spark, cfg)


def test_new_table_uses_requested_partitioning():
    w = _writer(_FakeSpark(exists=False, partition_cols=[]),
                delta_partition_columns=["region"])
    cols, force = w._resolve_table_partitioning()
    assert cols == ["region"]
    assert force is False


def test_existing_same_layout_keeps_columns():
    w = _writer(_FakeSpark(exists=True, partition_cols=["region"]),
                delta_partition_columns=["region"])
    cols, force = w._resolve_table_partitioning()
    assert cols == ["region"]
    assert force is False


def test_existing_unpartitioned_preserved_by_default():
    w = _writer(_FakeSpark(exists=True, partition_cols=[]),
                delta_partition_columns=["region"])
    cols, force = w._resolve_table_partitioning()
    assert cols == []          # requested partitioning ignored to preserve layout
    assert force is False


def test_existing_repartition_opt_in_overwrites():
    w = _writer(_FakeSpark(exists=True, partition_cols=[]),
                delta_partition_columns=["region"],
                repartition_existing_table=True,
                write_mode="overwrite")
    cols, force = w._resolve_table_partitioning()
    assert cols == ["region"]
    assert force is True        # forces overwriteSchema


def test_repartition_requires_overwrite_mode():
    w = _writer(_FakeSpark(exists=True, partition_cols=[]),
                delta_partition_columns=["region"],
                repartition_existing_table=True,
                write_mode="append")
    with pytest.raises(ConfigurationError, match="requires write_mode='overwrite'"):
        w._resolve_table_partitioning()


def test_no_requested_partitioning_preserves_existing():
    w = _writer(_FakeSpark(exists=True, partition_cols=["region"]))
    cols, force = w._resolve_table_partitioning()
    assert cols == []
    assert force is False


def test_table_name_rejects_path():
    with pytest.raises(ValueError, match="Unity Catalog name"):
        SaltmillConfig(input_path="/data/x.csv", table_name="abfss://c@a.dfs.core.windows.net/t")
