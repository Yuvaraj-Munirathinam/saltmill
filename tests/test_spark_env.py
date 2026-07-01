"""Tests for JVM detection — the core of cross-cluster compatibility."""
from saltmill.spark_env import has_jvm


class _SparkWithJvm:
    """Single-user/job cluster: sparkContext is accessible."""

    sparkContext = object()


class _SparkConnect:
    """Shared/serverless cluster: accessing sparkContext raises."""

    @property
    def sparkContext(self):
        raise RuntimeError(
            "[JVM_ATTRIBUTE_NOT_SUPPORTED] Directly accessing the underlying "
            "Spark driver JVM using the attribute 'sparkContext' is not supported "
            "on shared clusters."
        )


def test_has_jvm_true_on_classic_cluster():
    assert has_jvm(_SparkWithJvm()) is True


def test_has_jvm_false_on_spark_connect():
    assert has_jvm(_SparkConnect()) is False
