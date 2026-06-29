"""
Schema inference for large CSV files.

Samples the first N rows using Spark to avoid reading the full file.
Supports StructType passthrough, dict-based shorthand, and auto-inference.
"""

from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType

# Dict shorthand: map friendly names → Spark SQL type strings
_TYPE_ALIASES: dict[str, str] = {
    "str": "string",
    "string": "string",
    "int": "integer",
    "integer": "integer",
    "long": "long",
    "bigint": "long",
    "float": "float",
    "double": "double",
    "bool": "boolean",
    "boolean": "boolean",
    "date": "date",
    "timestamp": "timestamp",
    "decimal": "decimal(38,10)",
}

_INFER_SAMPLE_ROWS = 10_000


def resolve_schema(spark: "SparkSession", path: str, schema) -> "StructType | None":
    """
    Resolve *schema* to a PySpark StructType or None.

    Accepts:
      - None                    → auto-infer from the first sample rows
      - pyspark StructType      → returned as-is
      - dict {col: type_str}    → converted via dict_to_struct()
    """
    if schema is None:
        return _infer_from_sample(spark, path)
    if isinstance(schema, dict):
        return dict_to_struct(schema)
    # Assume it's already a StructType
    return schema


def dict_to_struct(mapping: dict[str, str]) -> "StructType":
    from pyspark.sql.types import StructType, StructField, _parse_datatype_string

    fields = []
    for col, type_hint in mapping.items():
        sql_type_str = _TYPE_ALIASES.get(type_hint.lower(), type_hint)
        fields.append(StructField(col, _parse_datatype_string(sql_type_str), True))
    return StructType(fields)


def _infer_from_sample(spark: "SparkSession", path: str) -> "StructType":
    """
    Read a small sample with inferSchema=true to build the StructType cheaply.
    Spark will only scan up to spark.sql.files.maxPartitionBytes of the first file.
    """
    sample_df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .option("samplingRatio", "0.001")   # 0.1% of rows — fast on huge files
        .csv(path)
    )
    return sample_df.schema
