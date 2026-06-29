from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Optional

from saltmill.cardinality import CardinalityAnalyzer
from saltmill.checkpoint import CheckpointManager
from saltmill.config import SaltmillConfig
from saltmill.models import PartitionPlan, ProcessingResult
from saltmill.progress import ProgressReporter
from saltmill.reader import CsvReader
from saltmill.salter import Salter
from saltmill.schema import SchemaInferrer
from saltmill.skew import SkewDetector
from saltmill.spark_conf import SparkConfigurator
from saltmill.writer import CsvWriter

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

log = logging.getLogger("saltmill")


class SaltmillProcessor:
    """
    High-level orchestrator for efficient large-CSV processing in Spark / Databricks.

    Minimal usage::

        from saltmill import SaltmillProcessor, SaltmillConfig

        result = SaltmillProcessor(SaltmillConfig(
            input_path="s3://bucket/large.csv",
            output_path="s3://bucket/output/delta/",
        )).process()

    Dry-run (inspect plan without writing)::

        plan = SaltmillProcessor(config).analyze()
        print(plan.salt_buckets, plan.target_partitions)
    """

    def __init__(self, config: SaltmillConfig) -> None:
        self._config = config
        self._reporter = ProgressReporter(
            callback=config.progress_callback,
            log_level=config.log_level,
        )

    def process(self, spark: Optional[SparkSession] = None) -> ProcessingResult:
        """Run the full pipeline: schema → analysis → salting → write."""
        t0 = time.monotonic()
        spark = self._resolve_spark(spark)
        cfg = self._config

        if not cfg.output_path:
            from saltmill.exceptions import ConfigurationError
            raise ConfigurationError(
                "output_path must be set before calling process(). "
                "Use analyze() for a dry-run without writing."
            )

        checkpoint: Optional[CheckpointManager] = None
        if cfg.checkpoint_path:
            checkpoint = CheckpointManager(spark, cfg.checkpoint_path)
            checkpoint.setup()

        with self._reporter.stage("schema_inference"):
            schema_info = self._resolve_schema(spark, checkpoint)

        configurator = SparkConfigurator(spark, cfg)
        if cfg.worker_count is None:
            cfg.worker_count = configurator.detect_worker_count()
            log.info("[saltmill] detected worker_count=%d", cfg.worker_count)
        if cfg.cores_per_worker == 8:
            cfg.cores_per_worker = configurator.detect_cores_per_worker()

        # Single pre-sample shared by cardinality and skew analysis.
        sample_df = (
            spark.read.schema(schema_info.schema)
            .options(**{**cfg.csv_options, "inferSchema": "false"})
            .csv(cfg.input_path)
            .sample(withReplacement=False, fraction=0.05, seed=42)
            .cache()
        )

        with self._reporter.stage("cardinality_analysis"):
            if cfg.partition_keys is None:
                analyzer = CardinalityAnalyzer(spark, cfg)
                cfg.partition_keys = analyzer.detect_partition_keys(sample_df)

        with self._reporter.stage("skew_detection"):
            detector = SkewDetector(spark, cfg)
            skew_reports = detector.analyze(sample_df, cfg.partition_keys)
            salt_buckets = cfg.salt_buckets or detector.pick_salt_buckets(skew_reports)

        sample_df.unpersist()

        reader = CsvReader(spark, cfg)
        plan = self._build_plan(cfg, salt_buckets, skew_reports, reader)

        with self._reporter.stage("spark_configuration"):
            spark_conf = configurator.apply(plan)

        with self._reporter.stage("csv_read"):
            full_df = reader.read(schema_info.schema)

        with self._reporter.stage("salting"):
            salter = Salter(cfg)
            salted_df = salter.apply(full_df, plan)
            if checkpoint and not checkpoint.is_stage_complete("salting"):
                salted_df = checkpoint.checkpoint_df(salted_df, "salting")
            output_df = salter.drop_salt(salted_df)

        with self._reporter.stage("write"):
            cwriter = CsvWriter(spark, cfg)
            file_count = cwriter.write(output_df, plan)

        total_rows = output_df.count()
        elapsed = time.monotonic() - t0
        return ProcessingResult(
            input_path=cfg.input_path,
            output_path=cfg.output_path,
            schema_info=schema_info,
            partition_plan=plan,
            total_rows=total_rows,
            total_files_written=file_count,
            duration_seconds=elapsed,
            checkpoint_used=checkpoint is not None,
            spark_conf_applied=spark_conf,
        )

    def analyze(self, spark: Optional[SparkSession] = None) -> PartitionPlan:
        """
        Dry-run: resolve schema, detect partition keys, compute salt plan.
        Does NOT read the full file or write any output.
        """
        spark = self._resolve_spark(spark)
        cfg = self._config

        with self._reporter.stage("schema_inference"):
            schema_info = self._resolve_schema(spark, checkpoint=None)

        configurator = SparkConfigurator(spark, cfg)
        if cfg.worker_count is None:
            cfg.worker_count = configurator.detect_worker_count()
        if cfg.cores_per_worker == 8:
            cfg.cores_per_worker = configurator.detect_cores_per_worker()

        sample_df = (
            spark.read.schema(schema_info.schema)
            .options(**{**cfg.csv_options, "inferSchema": "false"})
            .csv(cfg.input_path)
            .sample(withReplacement=False, fraction=0.05, seed=42)
            .cache()
        )

        with self._reporter.stage("cardinality_analysis"):
            if cfg.partition_keys is None:
                analyzer = CardinalityAnalyzer(spark, cfg)
                cfg.partition_keys = analyzer.detect_partition_keys(sample_df)

        with self._reporter.stage("skew_detection"):
            detector = SkewDetector(spark, cfg)
            skew_reports = detector.analyze(sample_df, cfg.partition_keys)
            salt_buckets = cfg.salt_buckets or detector.pick_salt_buckets(skew_reports)

        sample_df.unpersist()

        reader = CsvReader(spark, cfg)
        return self._build_plan(cfg, salt_buckets, skew_reports, reader)

    @classmethod
    def from_dict(cls, d: dict) -> SaltmillProcessor:
        """Construct from a plain dict — handy for Databricks notebook widgets."""
        return cls(SaltmillConfig(**d))

    def _resolve_schema(self, spark: SparkSession, checkpoint: Optional[CheckpointManager]):
        cfg = self._config
        inferrer = SchemaInferrer(spark, cfg)

        if checkpoint and checkpoint.is_stage_complete("schema_inference"):
            raw = checkpoint.load_metadata("schema_info")
            if raw:
                cached = inferrer.deserialize(raw)
                if cached:
                    log.info("[saltmill] schema loaded from checkpoint cache")
                    return cached

        schema_info = inferrer.resolve()

        if checkpoint:
            checkpoint.save_metadata("schema_info", inferrer.serialize(schema_info))
            checkpoint.mark_stage_complete("schema_inference")

        return schema_info

    def _build_plan(
        self,
        cfg: SaltmillConfig,
        salt_buckets: int,
        skew_reports,
        reader: CsvReader,
    ) -> PartitionPlan:
        workers = cfg.worker_count or 4
        total_cores = workers * cfg.cores_per_worker

        target_partitions = max(200, min(20_000, salt_buckets * total_cores))
        shuffle_partitions = max(200, min(20_000, target_partitions * 2))

        size_gb = reader.estimate_size_gb()
        est_mb_per_partition = (
            (size_gb * 1024) / target_partitions if target_partitions > 0 else 0.0
        )

        return PartitionPlan(
            partition_keys=list(cfg.partition_keys or []),
            salt_buckets=salt_buckets,
            target_partitions=target_partitions,
            shuffle_partitions=shuffle_partitions,
            estimated_partition_size_mb=est_mb_per_partition,
            skew_reports=skew_reports,
        )

    @staticmethod
    def _resolve_spark(spark: Optional[SparkSession]) -> SparkSession:
        if spark is not None:
            return spark
        from pyspark.sql import SparkSession as SS

        active = SS.getActiveSession()
        if active:
            return active
        return SS.builder.getOrCreate()
