"""Tests for JVM detection — the core of cross-cluster compatibility."""
import saltmill.spark_env as se
from saltmill.spark_env import has_jvm, supports_cache


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


class _RangeDF:
    def __init__(self, ok):
        self._ok = ok

    def cache(self):
        return self

    def count(self):
        if not self._ok:
            raise RuntimeError("[NOT_SUPPORTED_WITH_SERVERLESS] PERSIST TABLE ...")
        return 1

    def unpersist(self):
        return self


class _SparkCacheable:
    def range(self, n):
        return _RangeDF(ok=True)


class _SparkServerless:
    def range(self, n):
        return _RangeDF(ok=False)


def test_supports_cache_true(monkeypatch):
    se._CACHE_SUPPORT.clear()
    assert supports_cache(_SparkCacheable()) is True


def test_supports_cache_false_on_serverless(monkeypatch):
    se._CACHE_SUPPORT.clear()
    assert supports_cache(_SparkServerless()) is False


def test_supports_cache_is_memoized(monkeypatch):
    se._CACHE_SUPPORT.clear()
    spark = _SparkCacheable()
    assert supports_cache(spark) is True
    # Second call must hit the memo, not re-probe.
    assert se._CACHE_SUPPORT[id(spark)] is True
