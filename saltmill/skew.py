from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from saltmill.exceptions import SkewDetectionError
from saltmill.models import SkewReport

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

    from saltmill.config import SaltmillConfig

log = logging.getLogger("saltmill")

_SKEW_THRESHOLD = 0.10  # top key owns >10% of rows → skewed


class SkewDetector:
    def __init__(self, spark: "SparkSession", config: "SaltmillConfig") -> None:
        self._spark = spark
        self._config = config

    def analyze(self, df: "DataFrame", partition_keys: list[str]) -> list[SkewReport]:
        """
        Analyze each partition key for data skew.
        Caller is responsible for passing a pre-sampled DataFrame;
        this method uses the df as-is without further sampling.
        """
        try:
            total = df.count()
        except Exception as exc:
            raise SkewDetectionError(f"Skew count failed: {exc}") from exc

        if total == 0:
            return [
                SkewReport(
                    column=k,
                    total_rows_sampled=0,
                    top_value_frequency=0.0,
                    recommended_salt_buckets=1,
                    skew_detected=False,
                )
                for k in partition_keys
            ]

        reports: list[SkewReport] = []
        for key in partition_keys:
            report = self._analyze_column(df, key, total)
            reports.append(report)
            log.info(
                "[saltmill] skew analysis: column=%s top_freq=%.2f%% skewed=%s buckets=%d",
                key,
                report.top_value_frequency * 100,
                report.skew_detected,
                report.recommended_salt_buckets,
            )
        return reports

    def _analyze_column(
        self, df: "DataFrame", column: str, total: int
    ) -> SkewReport:
        from pyspark.sql import functions as F

        try:
            top_count = (
                df.groupBy(column)
                .count()
                .agg(F.max("count").alias("max_count"))
                .collect()[0]["max_count"]
            )
        except Exception as exc:
            raise SkewDetectionError(
                f"Failed to compute top-value frequency for column {column!r}: {exc}"
            ) from exc

        top_freq = (top_count or 0) / max(total, 1)
        skewed = top_freq > _SKEW_THRESHOLD
        salt_buckets = self._recommend_buckets(top_freq)

        return SkewReport(
            column=column,
            total_rows_sampled=total,
            top_value_frequency=top_freq,
            recommended_salt_buckets=salt_buckets,
            skew_detected=skewed,
        )

    def _recommend_buckets(self, top_freq: float) -> int:
        cfg = self._config
        worker_count = cfg.worker_count or 4
        total_cores = worker_count * cfg.cores_per_worker

        if top_freq <= _SKEW_THRESHOLD:
            return 1
        elif top_freq <= 0.30:
            raw = max(4, total_cores // 4)
        elif top_freq <= 0.60:
            raw = max(8, total_cores // 2)
        else:
            raw = max(16, total_cores)

        return _next_power_of_two(raw)

    def pick_salt_buckets(self, reports: list[SkewReport]) -> int:
        if not reports:
            return 1
        return max(r.recommended_salt_buckets for r in reports)


def _next_power_of_two(n: int) -> int:
    if n < 1:
        return 1
    return 1 << (n - 1).bit_length()
