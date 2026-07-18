# SPDX-License-Identifier: Apache-2.0
"""Unit tests for the JobMaster's restart policy.

Drives FixedDelayRestartStrategy directly with an explicit clock so the
sliding-window suppression, pruning, and the read-only window_view (used by the
progress UI) are tested deterministically — no scheduler, no Ray, no real time.
"""

import unittest

from ray.klein.config.configuration import Configuration
from ray.klein.runtime.scheduler.restart_result import RestartResult, RestartStatus
from ray.klein.runtime.scheduler.restart_strategy import (
    FixedDelayRestartStrategy,
    create_restart_strategy,
    now_seconds,
)


class FixedDelayRestartStrategyTest(unittest.TestCase):
    def _strategy(self, max_attempts=3, window=100, delay=10):
        return FixedDelayRestartStrategy(max_attempts=max_attempts, count_interval_seconds=window, delay_seconds=delay)

    def test_under_limit_not_suppressed(self):
        s = self._strategy(max_attempts=3, window=100)
        # max_attempts triggers within the window are allowed; the (N+1)th trips.
        for i in range(3):
            suppress, attempts = s.record_and_should_suppress(now=i)
            self.assertFalse(suppress)
            self.assertEqual(attempts, i + 1)

    def test_exceeding_limit_suppresses(self):
        s = self._strategy(max_attempts=3, window=100)
        for i in range(3):
            s.record_and_should_suppress(now=i)
        suppress, attempts = s.record_and_should_suppress(now=3)
        self.assertTrue(suppress)
        self.assertEqual(attempts, 4)

    def test_old_triggers_pruned_outside_window(self):
        # Triggers older than (now - window) are evicted before counting, so a
        # job healthy longer than the window starts its failure count fresh.
        s = self._strategy(max_attempts=3, window=100)
        for i in range(3):
            s.record_and_should_suppress(now=i)  # t=0,1,2
        # Jump past the window: all three triggers fall outside it.
        suppress, attempts = s.record_and_should_suppress(now=200)
        self.assertFalse(suppress)
        self.assertEqual(attempts, 1)

    def test_window_boundary_is_inclusive_of_recent(self):
        # _prune evicts entries strictly older than window_start = now - window.
        # An entry exactly at window_start is kept.
        s = self._strategy(max_attempts=2, window=100)
        s.record_and_should_suppress(now=0)
        s.record_and_should_suppress(now=50)
        # now=100 -> window_start=0; the t=0 entry is NOT < 0, so it survives.
        suppress, attempts = s.record_and_should_suppress(now=100)
        self.assertEqual(attempts, 3)
        self.assertTrue(suppress)

    def test_window_view_does_not_mutate(self):
        s = self._strategy(max_attempts=3, window=100)
        s.record_and_should_suppress(now=0)
        s.record_and_should_suppress(now=1)
        attempts, max_attempts, window = s.window_view(now=2)
        self.assertEqual(attempts, 2)
        self.assertEqual(max_attempts, 3)
        self.assertEqual(window, 100)
        # Calling window_view again yields the same count (no append side effect).
        attempts2, _, _ = s.window_view(now=2)
        self.assertEqual(attempts2, 2)

    def test_window_view_prunes_for_reporting(self):
        s = self._strategy(max_attempts=3, window=100)
        s.record_and_should_suppress(now=0)
        # A far-future read should report zero live attempts.
        attempts, _, _ = s.window_view(now=10_000)
        self.assertEqual(attempts, 0)

    def test_delay_exposed(self):
        s = self._strategy(delay=7)
        self.assertEqual(s.delay, 7)

    def test_isolated_failures_never_suppress(self):
        # One failure every 2*window: each is alone in its window -> never trips.
        s = self._strategy(max_attempts=1, window=100)
        for k in range(5):
            suppress, attempts = s.record_and_should_suppress(now=k * 300)
            self.assertFalse(suppress)
            self.assertEqual(attempts, 1)

    def test_from_config_uses_defaults(self):
        s = FixedDelayRestartStrategy.from_config(Configuration())
        # Defaults: 3 attempts, 10s delay, 600s window.
        self.assertEqual(s.delay, 10)
        _, max_attempts, window = s.window_view(now=0)
        self.assertEqual(max_attempts, 3)
        self.assertEqual(window, 600)

    def test_create_restart_strategy_factory(self):
        s = create_restart_strategy(Configuration())
        self.assertIsInstance(s, FixedDelayRestartStrategy)

    def test_now_seconds_is_int(self):
        self.assertIsInstance(now_seconds(), int)


class RestartResultTest(unittest.TestCase):
    def test_fields(self):
        r = RestartResult(RestartStatus.SUCCESS, "ok")
        self.assertEqual(r.status, RestartStatus.SUCCESS)
        self.assertEqual(r.message, "ok")

    def test_status_values(self):
        self.assertEqual(
            {s.value for s in RestartStatus},
            {"SUCCESS", "FAILED", "SUPPRESSED"},
        )
