# SPDX-License-Identifier: Apache-2.0
from datetime import timedelta

from ray.klein.api.session_window import SessionWindow
from ray.klein.api.sliding_window import SlidingWindow
from ray.klein.api.time_window import TimeWindow
from ray.klein.api.tumbling_window import TumblingWindow


def test_tumbling_window_uses_half_open_intervals():
    assigner = TumblingWindow(timedelta(seconds=10))

    assert assigner.assign_windows(0) == (TimeWindow(0, 10_000),)
    assert assigner.assign_windows(10_000) == (TimeWindow(10_000, 20_000),)


def test_sliding_window_assigns_every_overlapping_window():
    assigner = SlidingWindow(
        timedelta(seconds=10),
        timedelta(seconds=5),
    )

    assert assigner.assign_windows(7_000) == (
        TimeWindow(5_000, 15_000),
        TimeWindow(0, 10_000),
    )


def test_session_window_is_marked_for_runtime_merging():
    assigner = SessionWindow(timedelta(seconds=3))

    assert assigner.is_merging
    assert assigner.assign_windows(2_000) == (TimeWindow(2_000, 5_000),)
