# SPDX-License-Identifier: Apache-2.0
"""Unit tests for the composite checkpoint-barrier trigger (records OR time)."""

import unittest

import pytest

from ray.klein.runtime.coordinator.checkpoint_trigger import (
    CheckpointTrigger,
)


class _FakeClock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t

    def advance(self, dt):
        self.t += dt


class CheckpointTriggerTest(unittest.TestCase):
    def test_record_threshold_fires_every_n(self):
        clock = _FakeClock()
        trig = CheckpointTrigger(interval_records=3, interval_seconds=0, clock=clock)
        fires = [trig.should_trigger(record_emitted=True) for _ in range(7)]
        # Fires on the 3rd and 6th record.
        self.assertEqual(fires, [False, False, True, False, False, True, False])

    def test_columnar_batch_counts_rows_but_fires_one_barrier(self):
        trig = CheckpointTrigger(interval_records=3, interval_seconds=0, clock=_FakeClock())

        self.assertTrue(trig.should_trigger(record_emitted=True, record_count=5))
        self.assertFalse(trig.should_trigger(record_emitted=True, record_count=1))

    def test_time_threshold_fires_while_idle(self):
        clock = _FakeClock()
        trig = CheckpointTrigger(interval_records=0, interval_seconds=10, clock=clock)
        # First idle call sets the baseline, never fires.
        self.assertFalse(trig.should_trigger(record_emitted=False))
        clock.advance(9)
        self.assertFalse(trig.should_trigger(record_emitted=False))
        clock.advance(1)  # now 10s elapsed
        self.assertTrue(trig.should_trigger(record_emitted=False))
        # Resets after firing.
        clock.advance(10)
        self.assertTrue(trig.should_trigger(record_emitted=False))

    def test_whichever_first_records_then_time(self):
        clock = _FakeClock()
        trig = CheckpointTrigger(interval_records=100, interval_seconds=5, clock=clock)
        # Low volume: never hits 100 records, but time fires.
        for _ in range(10):
            self.assertFalse(trig.should_trigger(record_emitted=True))
        clock.advance(5)
        self.assertTrue(trig.should_trigger(record_emitted=True))

    def test_record_fire_resets_timer(self):
        clock = _FakeClock()
        trig = CheckpointTrigger(interval_records=2, interval_seconds=10, clock=clock)
        clock.advance(8)
        self.assertFalse(trig.should_trigger(record_emitted=True))  # 1 record, 8s
        self.assertTrue(trig.should_trigger(record_emitted=True))  # 2 records -> fire, timer reset
        # Timer was reset by the record fire: 8s+ already passed but clock is now baseline.
        clock.advance(9)
        self.assertFalse(trig.should_trigger(record_emitted=False))  # only 9s since reset
        clock.advance(1)
        self.assertTrue(trig.should_trigger(record_emitted=False))  # 10s since reset

    def test_both_disabled_never_fires(self):
        clock = _FakeClock()
        trig = CheckpointTrigger(interval_records=0, interval_seconds=0, clock=clock)
        clock.advance(10_000)
        self.assertFalse(trig.should_trigger(record_emitted=True))
        self.assertFalse(trig.should_trigger(record_emitted=False))

    def test_idle_call_does_not_count_records(self):
        clock = _FakeClock()
        trig = CheckpointTrigger(interval_records=2, interval_seconds=0, clock=clock)
        # Idle calls must not advance the record counter.
        for _ in range(5):
            self.assertFalse(trig.should_trigger(record_emitted=False))
        self.assertFalse(trig.should_trigger(record_emitted=True))  # 1st real record
        self.assertTrue(trig.should_trigger(record_emitted=True))  # 2nd -> fire


@pytest.mark.parametrize("records, seconds", [(-1, 1), (1, -1)])
def test_trigger_rejects_negative_thresholds(records, seconds):
    with pytest.raises(ValueError):
        CheckpointTrigger(records, seconds)
