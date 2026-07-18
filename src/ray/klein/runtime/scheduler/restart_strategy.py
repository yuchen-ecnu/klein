# SPDX-License-Identifier: Apache-2.0
"""Pluggable restart policies for the JobMaster."""

import threading
import time
from abc import ABC, abstractmethod
from collections import deque
from math import isfinite
from numbers import Real

from ray.klein.config.configuration import Configuration
from ray.klein.config.restart_strategy_options import (
    RestartStrategyOptions,
)


class RestartStrategy(ABC):
    """Decides whether a restart should be suppressed and how long to back off."""

    @abstractmethod
    def record_and_should_suppress(self, now: int) -> tuple[bool, int]:
        """Record a restart trigger at ``now`` (epoch seconds).

        Returns ``(should_suppress, attempts_in_window)``. When ``should_suppress``
        is True the job is failing too fast and must be failed permanently.
        """

    @abstractmethod
    def window_view(self, now: int) -> tuple[int, int, int]:
        """``(attempts_in_window, max_attempts, window_seconds)`` for UI.

        Prunes expired entries (older than ``now - count_interval``) before
        counting so the caller always gets a current view."""

    @property
    @abstractmethod
    def delay(self) -> float:
        """Fixed back-off (seconds) the caller should wait between attempts."""


class FixedDelayRestartStrategy(RestartStrategy):
    """Fixed-delay back-off with sliding-window failure-rate suppression.

    Every restart trigger appends a timestamp; entries older than ``now - W``
    (W = count-interval) are evicted before counting. If more than N triggers
    (N = max-attempts) land within W, the job is failing too fast and the
    restart is suppressed.

    The window is NOT cleared on a successful reschedule: a reschedule only means
    the graph was re-deployed, not that the job is healthy — a deterministic UDF
    error would re-fail seconds later. The window instead slides naturally, so a
    job that runs healthily longer than W evicts its earliest trigger and an isolated
    later failure starts fresh; only a sustained high failure-rate trips
    suppression.

    record_and_should_suppress() (supervisor) and window_view() (progress UI) are
    both invoked from asyncio.to_thread worker threads, and the JobManager runs
    with max_concurrency=8, so the non-atomic prune loop is guarded by a lock.
    """

    def __init__(self, max_attempts: int, count_interval_seconds: float, delay_seconds: float) -> None:
        if isinstance(max_attempts, bool) or not isinstance(max_attempts, int):
            raise TypeError("max_attempts must be an integer")
        if max_attempts < 0:
            raise ValueError("max_attempts must be non-negative")
        if isinstance(count_interval_seconds, bool) or not isinstance(count_interval_seconds, Real):
            raise TypeError("count_interval_seconds must be a real number")
        if isinstance(delay_seconds, bool) or not isinstance(delay_seconds, Real):
            raise TypeError("delay_seconds must be a real number")
        count_interval_seconds = float(count_interval_seconds)
        delay_seconds = float(delay_seconds)
        if not isfinite(count_interval_seconds):
            raise ValueError("count_interval_seconds must be finite")
        if not isfinite(delay_seconds):
            raise ValueError("delay_seconds must be finite")
        if count_interval_seconds <= 0:
            raise ValueError("count_interval_seconds must be positive")
        if delay_seconds < 0:
            raise ValueError("delay_seconds must be non-negative")
        self._max_attempts = max_attempts
        self._count_interval = count_interval_seconds
        self._delay = delay_seconds
        self._window: deque[int] = deque()
        self._lock = threading.Lock()

    @classmethod
    def from_config(cls, config: Configuration) -> "FixedDelayRestartStrategy":
        return cls(
            max_attempts=config.get(RestartStrategyOptions.MAX_ATTEMPTS),
            count_interval_seconds=config.get(RestartStrategyOptions.COUNT_INTERVAL).total_seconds(),
            delay_seconds=config.get(RestartStrategyOptions.DELAY).total_seconds(),
        )

    def record_and_should_suppress(self, now: int) -> tuple[bool, int]:
        with self._lock:
            self._prune(now)
            self._window.append(now)
            attempts = len(self._window)
        return attempts > self._max_attempts, attempts

    def window_view(self, now: int) -> tuple[int, int, int]:
        """``(attempts_in_window, max_attempts, window_seconds)`` for UI.

        Prunes expired entries (older than ``now - count_interval``) before
        counting so the caller always gets a current view."""
        with self._lock:
            self._prune(now)
            attempts = len(self._window)
        return attempts, int(self._max_attempts), int(self._count_interval)

    @property
    def delay(self) -> float:
        return self._delay

    def _prune(self, now: int) -> None:
        window_start = now - self._count_interval
        while self._window and self._window[0] < window_start:
            self._window.popleft()


def create_restart_strategy(config: Configuration) -> RestartStrategy:
    """Build the configured restart strategy.

    Fixed delay is the runtime's restart policy.
    """
    return FixedDelayRestartStrategy.from_config(config)


def now_seconds() -> int:
    """Epoch seconds. Isolated so callers/tests have one clock to reason about."""
    return int(time.time())
