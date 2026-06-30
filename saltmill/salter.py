from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from saltmill.config import SaltmillConfig
    from saltmill.models import PartitionPlan

log = logging.getLogger("saltmill")


class Salter:
    def __init__(self, config: SaltmillConfig) -> None:
        self._config = config

    def apply(self, df: DataFrame, plan: PartitionPlan) -> DataFrame:
        """
        Add a salt column and repartition.
        Call drop_salt() on the result before writing or returning to callers.
        """
        from pyspark.sql import functions as F

        cfg = self._config
        salt_col = cfg.salt_column_name

        if plan.salt_buckets <= 1:
            log.info(
                "[saltmill] salt_buckets=1, repartitioning on %s into %d partitions",
                plan.partition_keys,
                plan.target_partitions,
            )
            return df.repartition(
                plan.target_partitions,
                *[F.col(k) for k in plan.partition_keys],
            )

        log.info(
            "[saltmill] applying salt: buckets=%d, keys=%s, target_partitions=%d",
            plan.salt_buckets,
            plan.partition_keys,
            plan.target_partitions,
        )

        if salt_col in df.columns:
            from saltmill.exceptions import ConfigurationError
            raise ConfigurationError(
                f"salt_column_name={salt_col!r} already exists in the DataFrame. "
                "Set a different SaltmillConfig.salt_column_name."
            )

        df = df.withColumn(
            salt_col,
            F.pmod(F.monotonically_increasing_id(), F.lit(plan.salt_buckets)),
        )
        repartition_cols = [F.col(k) for k in plan.partition_keys] + [F.col(salt_col)]
        return df.repartition(plan.target_partitions, *repartition_cols)

    def drop_salt(self, df: DataFrame) -> DataFrame:
        return df.drop(self._config.salt_column_name)
