"""Tests for schema helpers — dict_to_struct requires PySpark."""
import pytest

pyspark = pytest.importorskip("pyspark", reason="pyspark not installed")
from saltmill.schema import dict_to_struct


def test_dict_to_struct_basic():
    schema = dict_to_struct({"id": "long", "name": "string", "amount": "double"})
    field_names = [f.name for f in schema.fields]
    assert field_names == ["id", "name", "amount"]


def test_dict_to_struct_aliases():
    schema = dict_to_struct({"count": "int", "flag": "bool"})
    types = {f.name: f.dataType.simpleString() for f in schema.fields}
    assert types["count"] == "int"
    assert types["flag"] == "boolean"


def test_dict_to_struct_all_nullable():
    schema = dict_to_struct({"x": "float"})
    assert all(f.nullable for f in schema.fields)


def test_dict_to_struct_timestamp():
    schema = dict_to_struct({"created_at": "timestamp"})
    assert schema.fields[0].dataType.simpleString() == "timestamp"
