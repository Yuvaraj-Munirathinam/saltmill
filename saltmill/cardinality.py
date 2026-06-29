from __future__ import annotations

import logging
import math
from typing import TYPE_CHECKING

from saltmill.exceptions import SkewDetectionError

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

    from saltmill.config import SaltmillConfig

log = logging.getLogger("saltmill")

_MIN_DISTINCT = 50
_MAX_DISTINCT_FRACTION = 0.5


class CardinalityAnalyzer:
    def __init__(self, spark: "SparkSession", config: "SaltmillConfig") -> None:
        self._spark = spark
        self._config = config

    def detect_partition_keys(self, df: "DataFrame") -> list[str]:
        """
        Score each string/integer column by cardinality and return the top 1-2.
        Caller is responsible for passing a pre-sampled DataFrame;
        this method uses the df as-is without further sampling.
        """
        try:
            total = df.count()
        except Exception as exc:
            raise SkewDetectionError(f"Cardinality count failed: {exc}") from exc

        if total == 0:
            log.warning("[saltmill] Sample for cardinality analysis is empty; using first column")
            return [df.columns[0]]

        candidates = self._score_columns(df, total)
        if not candidates:
            log.warning(
                "[saltmill] No suitable partition key found via cardinality; using first column"
            )
            return [df.columns[0]]

        selected = [col for col, _ in candidates[:2]]
        log.info("[saltmill] auto-selected partition keys: %s", selected)
        return selected

    def _score_columns(self, df: "DataFrame", total: int) -> list[tuple[str, float]]:
        from pyspark.sql import functions as F
        from pyspark.sql.types import BooleanType, NumericType, StringType

        candidate_cols = [
            f.name
            for f in df.schema.fields
            if isinstance(f.dataType, (StringType, NumericType))
            and not isinstance(f.dataType, BooleanType)
        ]

        if not candidate_cols:
            return []

        agg_exprs = [F.approx_count_distinct(F.col(c)).alias(c) for c in candidate_cols]
        counts: dict[str, int] = df.agg(*agg_exprs).collect()[0].asDict()

        scored: list[tuple[str, float]] = []
        for col_name, distinct in counts.items():
            score = self._score(distinct, total)
            if score > 0:
                scored.append((col_name, score))
                log.debug(
                    "[saltmill] column=%s distinct=%d total=%d score=%.3f",
                    col_name, distinct, total, score,
                )

        scored.sort(key=lambda x: x[1], reverse=True)
        return scored

    @staticmethod
    def _score(distinct: int, total: int) -> float:
        if distinct < _MIN_DISTINCT:
            return 0.0
        if distinct > total * _MAX_DISTINCT_FRACTION:
            return 0.0
        ideal = math.sqrt(total)
        ratio = distinct / ideal
        return 1.0 / (1.0 + abs(math.log(max(ratio, 1e-9))))
