from __future__ import annotations

import json
import logging
import time
from typing import TYPE_CHECKING, Optional

from saltmill.exceptions import SchemaInferenceError
from saltmill.models import SchemaInfo

if TYPE_CHECKING:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType

    from saltmill.config import SaltmillConfig

log = logging.getLogger("saltmill")

def _resolve_spark_type(type_hint: str):
    from pyspark.sql.types import (
        BooleanType, DateType, DecimalType, DoubleType,
        FloatType, IntegerType, LongType, StringType, TimestampType,
    )
    _MAP = {
        "str": StringType(), "string": StringType(),
        "int": IntegerType(), "integer": IntegerType(),
        "long": LongType(), "bigint": LongType(),
        "float": FloatType(),
        "double": DoubleType(),
        "bool": BooleanType(), "boolean": BooleanType(),
        "date": DateType(),
        "timestamp": TimestampType(),
        "decimal": DecimalType(38, 10),
    }
    t = type_hint.lower()
    if t not in _MAP:
        raise ValueError(
            f"Unknown type {type_hint!r}. Supported: {list(_MAP)}"
        )
    return _MAP[t]


def dict_to_struct(mapping: dict[str, str]) -> "StructType":
    """Convert a ``{"col": "type"}`` dict to a PySpark StructType (no SparkSession needed)."""
    from pyspark.sql.types import StructField, StructType

    return StructType([
        StructField(col, _resolve_spark_type(type_hint), True)
        for col, type_hint in mapping.items()
    ])


class SchemaInferrer:
    def __init__(self, spark: "SparkSession", config: "SaltmillConfig") -> None:
        self._spark = spark
        self._config = config

    def resolve(self) -> SchemaInfo:
        cfg = self._config
        if cfg.schema is not None:
            nullable = [f.name for f in cfg.schema.fields if f.nullable]
            return SchemaInfo(
                schema=cfg.schema,
                inferred=False,
                sample_rows=0,
                inference_duration_seconds=0.0,
                nullable_columns=nullable,
            )
        return self._infer_from_sample()

    def _infer_from_sample(self) -> SchemaInfo:
        cfg = self._config
        t0 = time.monotonic()
        try:
            sample_df = (
                self._spark.read
                .options(**{**cfg.csv_options, "inferSchema": "true"})
                .csv(cfg.input_path)
                .limit(cfg.schema_sample_max_rows)
            )
            schema = sample_df.schema
            row_count = sample_df.count()
        except Exception as exc:
            raise SchemaInferenceError(
                f"Failed to infer schema from {cfg.input_path!r}: {exc}"
            ) from exc

        nullable = [f.name for f in schema.fields if f.nullable]
        elapsed = time.monotonic() - t0
        log.info(
            "[saltmill] schema inferred: %d columns, %d sample rows, %.2fs",
            len(schema.fields), row_count, elapsed,
        )
        return SchemaInfo(
            schema=schema,
            inferred=True,
            sample_rows=row_count,
            inference_duration_seconds=elapsed,
            nullable_columns=nullable,
        )

    def serialize(self, info: SchemaInfo) -> str:
        return json.dumps({
            "schema_json": info.schema.json(),
            "inferred": info.inferred,
            "sample_rows": info.sample_rows,
            "inference_duration_seconds": info.inference_duration_seconds,
            "nullable_columns": info.nullable_columns,
        })

    def deserialize(self, raw: str) -> Optional[SchemaInfo]:
        try:
            from pyspark.sql.types import StructType as ST
            d = json.loads(raw)
            schema = ST.fromJson(json.loads(d["schema_json"]))
            return SchemaInfo(
                schema=schema,
                inferred=d["inferred"],
                sample_rows=d["sample_rows"],
                inference_duration_seconds=d["inference_duration_seconds"],
                nullable_columns=d["nullable_columns"],
            )
        except Exception:
            log.debug("Failed to deserialize cached schema", exc_info=True)
            return None
