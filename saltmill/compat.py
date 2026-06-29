"""
Backward-compatible simple API.

Wraps the new SaltmillProcessor to provide the one-liner ``saltmill.read()``
and the class-based ``SaltMill`` interface from the original v0.1 API.
Both return a DataFrame directly — no output_path required.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.types import StructType


class SaltMill:
    """
    Simple one-shot interface for large CSV ingestion with auto salt partitioning.

    Parameters
    ----------
    spark:
        Active SparkSession.
    workers:
        Worker node count override (auto-detected when omitted).
    verbose:
        Print the partition plan before reading (default True).

    Examples
    --------
    ::

        sm = SaltMill(spark)
        df = sm.read("abfss://container@account.dfs.core.windows.net/data/huge.csv")

        sm = SaltMill(spark, workers=32)
        df = sm.read(
            [
                "abfss://raw@account.dfs.core.windows.net/data/part1.csv",
                "abfss://raw@account.dfs.core.windows.net/data/part2.csv",
            ],
            schema={"id": "long", "region": "string", "amount": "double"},
            partition_col="region",
        )
    """

    def __init__(
        self,
        spark: SparkSession,
        *,
        workers: int | None = None,
        verbose: bool = True,
    ) -> None:
        self.spark = spark
        self._workers = workers
        self.verbose = verbose

    def read(
        self,
        paths: str | list[str],
        *,
        schema: StructType | dict | None = None,
        partition_col: str | list[str] | None = None,
        salt_buckets: int | None = None,
        delimiter: str = ",",
        encoding: str = "UTF-8",
        null_value: str = "",
    ) -> DataFrame:
        """
        Read one or more large CSV files with automatic salt partitioning.
        Returns a repartitioned DataFrame ready for processing or Delta writes.
        """
        from saltmill.config import SaltmillConfig
        from saltmill.processor import SaltmillProcessor
        from saltmill.reader import CsvReader
        from saltmill.salter import Salter
        from saltmill.schema import SchemaInferrer, dict_to_struct

        paths_list = [paths] if isinstance(paths, str) else list(paths)

        resolved_schema = None
        if isinstance(schema, dict):
            resolved_schema = dict_to_struct(schema)
        elif schema is not None:
            resolved_schema = schema

        partition_keys: list[str] | None = None
        if partition_col is not None:
            partition_keys = (
                [partition_col] if isinstance(partition_col, str) else list(partition_col)
            )

        cfg = SaltmillConfig(
            input_path=paths_list[0],
            schema=resolved_schema,
            partition_keys=partition_keys,
            salt_buckets=salt_buckets,
            worker_count=self._workers,
            csv_options={
                "header": "true",
                "inferSchema": "false",
                "sep": delimiter,
                "encoding": encoding,
                "nullValue": null_value,
                "mode": "PERMISSIVE",
                "columnNameOfCorruptRecord": "_corrupt_record",
            },
        )

        proc = SaltmillProcessor(cfg)
        plan = proc.analyze(self.spark)

        if self.verbose:
            print(
                f"[saltmill] plan → salt_buckets={plan.salt_buckets}, "
                f"partitions={plan.target_partitions}, "
                f"keys={plan.partition_keys}"
            )

        schema_info = SchemaInferrer(self.spark, cfg).resolve()
        reader = CsvReader(self.spark, cfg)
        df = reader.read(schema_info.schema, paths=paths_list)

        salter = Salter(cfg)
        return salter.drop_salt(salter.apply(df, plan))

    def tune(
        self,
        paths: str | list[str] | None = None,
        *,
        salt_buckets: int | None = None,
        hint_size_gb: float | None = None,
    ):
        """Dry-run: return the PartitionPlan without reading data."""
        from saltmill.config import SaltmillConfig
        from saltmill.processor import SaltmillProcessor

        paths_list = ([paths] if isinstance(paths, str) else list(paths)) if paths else ["."]
        cfg = SaltmillConfig(
            input_path=paths_list[0],
            salt_buckets=salt_buckets,
            worker_count=self._workers,
        )
        return SaltmillProcessor(cfg).analyze(self.spark)

    def write_delta(
        self,
        df: DataFrame,
        path: str,
        *,
        partition_by: str | list[str] | None = None,
        mode: str = "overwrite",
    ) -> None:
        """Write a DataFrame to Delta Lake."""
        writer = df.write.format("delta").mode(mode)
        if partition_by:
            cols = [partition_by] if isinstance(partition_by, str) else list(partition_by)
            writer = writer.partitionBy(*cols)
        writer.save(path)


def read(
    spark: SparkSession,
    paths: str | list[str],
    *,
    schema: StructType | dict | None = None,
    partition_col: str | list[str] | None = None,
    workers: int | None = None,
    salt_buckets: int | None = None,
    delimiter: str = ",",
    encoding: str = "UTF-8",
    null_value: str = "",
    verbose: bool = True,
) -> DataFrame:
    """
    One-call interface: read large CSV file(s) with automatic salt partitioning.

    Returns a repartitioned DataFrame. No output path required.

    Examples
    --------
    ::

        import saltmill

        df = saltmill.read(
            spark,
            "abfss://container@account.dfs.core.windows.net/data/large.csv",
        )

        df = saltmill.read(
            spark,
            [
                "abfss://raw@account.dfs.core.windows.net/data/part1.csv",
                "abfss://raw@account.dfs.core.windows.net/data/part2.csv",
            ],
            schema={"id": "long", "region": "string", "revenue": "double"},
            partition_col="region",
        )
    """
    return SaltMill(spark, workers=workers, verbose=verbose).read(
        paths,
        schema=schema,
        partition_col=partition_col,
        salt_buckets=salt_buckets,
        delimiter=delimiter,
        encoding=encoding,
        null_value=null_value,
    )
