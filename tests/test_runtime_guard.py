"""Tests for the wall-clock runtime guard, using a fake SparkContext.

No real Spark needed — we only assert job-group lifecycle and timeout handling.
"""
import pytest

from saltmill.config import SaltmillConfig
from saltmill.exceptions import ProcessingTimeoutError
from saltmill.processor import SaltmillProcessor


class _FakeSparkContext:
    def __init__(self):
        self.set_calls = []
        self.cleared = 0
        self.cancelled = []

    def setJobGroup(self, group, desc, interrupt):  # noqa: N802 (Spark API name)
        self.set_calls.append((group, desc, interrupt))

    def clearJobGroup(self):  # noqa: N802
        self.cleared += 1

    def cancelJobGroup(self, group):  # noqa: N802
        self.cancelled.append(group)


class _FakeSpark:
    def __init__(self):
        self.sparkContext = _FakeSparkContext()


def _proc(**kw):
    cfg = SaltmillConfig(input_path="/data/x.csv", **kw)
    return SaltmillProcessor(cfg), _FakeSpark()


def test_guard_noop_when_disabled():
    proc, spark = _proc(max_runtime_seconds=None)
    with proc._runtime_guard(spark):
        pass
    # No job group should be touched when the guard is off.
    assert spark.sparkContext.set_calls == []
    assert spark.sparkContext.cleared == 0


def test_guard_sets_and_clears_job_group():
    proc, spark = _proc(max_runtime_seconds=3600)
    with proc._runtime_guard(spark):
        pass
    assert spark.sparkContext.set_calls == [("saltmill", "saltmill large-CSV processing", True)]
    assert spark.sparkContext.cleared == 1
    assert spark.sparkContext.cancelled == []  # quick body → no cancel


def test_guard_clears_job_group_on_exception():
    proc, spark = _proc(max_runtime_seconds=3600)
    with pytest.raises(ValueError):
        with proc._runtime_guard(spark):
            raise ValueError("boom")
    # Non-timeout error propagates unchanged, but cleanup still runs.
    assert spark.sparkContext.cleared == 1


def test_guard_translates_to_timeout_error():
    # Real watchdog: 1s timeout, body runs longer then raises as a cancelled
    # Spark action would. The guard must translate to ProcessingTimeoutError
    # and have cancelled the job group.
    import time

    proc, spark = _proc(max_runtime_seconds=1)
    sc = spark.sparkContext
    with pytest.raises(ProcessingTimeoutError):
        with proc._runtime_guard(spark):
            time.sleep(1.3)  # let the timer fire and cancel the group
            raise RuntimeError("simulated cancelled job")
    assert sc.cancelled == ["saltmill"]
    assert sc.cleared == 1


def test_guard_timeout_raised_even_if_body_returns():
    # If the timer fires but the body finishes without error, the guard still
    # raises afterwards so a timed-out run is never reported as success.
    import time

    proc, spark = _proc(max_runtime_seconds=1)
    with pytest.raises(ProcessingTimeoutError):
        with proc._runtime_guard(spark):
            time.sleep(1.3)
    assert spark.sparkContext.cancelled == ["saltmill"]
