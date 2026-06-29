from __future__ import annotations

import logging
import time
from contextlib import contextmanager
from typing import Callable, Generator, Optional

log = logging.getLogger("saltmill")


class ProgressReporter:
    STAGES = [
        "schema_inference",
        "cardinality_analysis",
        "skew_detection",
        "spark_configuration",
        "csv_read",
        "salting",
        "write",
    ]

    def __init__(
        self,
        callback: Optional[Callable[[str, float], None]] = None,
        log_level: str = "INFO",
    ) -> None:
        self._callback = callback
        log.setLevel(getattr(logging, log_level.upper(), logging.INFO))

    def report(self, stage: str, pct: float, message: str = "") -> None:
        log.info("[saltmill] stage=%s progress=%.0f%% %s", stage, pct * 100, message)
        if self._callback:
            try:
                self._callback(stage, pct)
            except Exception:
                log.debug("progress_callback raised an exception", exc_info=True)

    def stage_start(self, stage: str) -> None:
        self.report(stage, 0.0, "started")

    def stage_done(self, stage: str) -> None:
        self.report(stage, 1.0, "done")

    @contextmanager
    def stage(self, name: str) -> Generator[None, None, None]:
        self.stage_start(name)
        t0 = time.monotonic()
        yield
        elapsed = time.monotonic() - t0
        self.stage_done(name)
        log.info("[saltmill] stage=%s elapsed=%.2fs", name, elapsed)
