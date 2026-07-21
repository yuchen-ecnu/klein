# SPDX-License-Identifier: Apache-2.0
from datetime import timedelta

import pytest

from ray.klein.api.session_window import SessionWindow
from ray.klein.api.sliding_window import SlidingWindow
from ray.klein.api.time_window import TimeWindow
from ray.klein.api.tumbling_window import TumblingWindow


def test_tumbling_window_uses_half_open_intervals():
    assigner = TumblingWindow(timedelta(seconds=10))

    assert assigner.assign_windows(0) == (TimeWindow(0, 10_000),)
    assert assigner.assign_windows(10_000) == (TimeWindow(10_000, 20_000),)


def test_tumbling_window_offset_is_aligned_across_the_epoch_boundary():
    assigner = TumblingWindow(
        timedelta(seconds=10),
        offset=timedelta(seconds=3),
    )

    assert assigner.assign_windows(0) == (TimeWindow(-7_000, 3_000),)
    assert assigner.assign_windows(2_999) == (TimeWindow(-7_000, 3_000),)
    assert assigner.assign_windows(3_000) == (TimeWindow(3_000, 13_000),)


def test_sliding_window_assigns_every_overlapping_window():
    assigner = SlidingWindow(
        timedelta(seconds=10),
        timedelta(seconds=5),
    )

    assert assigner.assign_windows(7_000) == (
        TimeWindow(5_000, 15_000),
        TimeWindow(0, 10_000),
    )


def test_sliding_window_includes_negative_start_windows_at_epoch_boundary():
    assigner = SlidingWindow(
        timedelta(seconds=10),
        timedelta(seconds=5),
    )

    assert assigner.assign_windows(0) == (
        TimeWindow(0, 10_000),
        TimeWindow(-5_000, 5_000),
    )


def test_sliding_window_offset_and_end_boundary_are_half_open():
    assigner = SlidingWindow(
        timedelta(seconds=10),
        timedelta(seconds=5),
        offset=timedelta(seconds=2),
    )

    assert assigner.assign_windows(7_000) == (
        TimeWindow(7_000, 17_000),
        TimeWindow(2_000, 12_000),
    )


def test_session_window_is_marked_for_runtime_merging():
    assigner = SessionWindow(timedelta(seconds=3))

    assert assigner.is_merging
    assert assigner.assign_windows(2_000) == (TimeWindow(2_000, 5_000),)


def test_time_window_overlap_respects_half_open_boundaries():
    left = TimeWindow(0, 10)
    overlapping = TimeWindow(9, 20)
    touching = TimeWindow(10, 20)

    assert left.overlaps(overlapping)
    assert overlapping.overlaps(left)
    assert not left.overlaps(touching)
    assert not touching.overlaps(left)
    assert left.merge(overlapping) == TimeWindow(0, 20)


@pytest.mark.parametrize(
    ("factory", "parameter"),
    [
        (lambda: TumblingWindow(timedelta(0)), "size"),
        (lambda: TumblingWindow(timedelta(microseconds=999)), "size"),
        (
            lambda: SlidingWindow(timedelta(seconds=1), timedelta(0)),
            "slide",
        ),
        (lambda: SessionWindow(timedelta(seconds=-1)), "gap"),
    ],
)
def test_window_assigners_reject_non_positive_millisecond_parameters(factory, parameter):
    with pytest.raises(ValueError, match=rf"window {parameter} must be positive"):
        factory()


@pytest.mark.parametrize(("start", "end"), [(1, 1), (2, 1)])
def test_time_window_rejects_empty_or_reversed_intervals(start, end):
    with pytest.raises(ValueError, match="window end must be greater than start"):
        TimeWindow(start, end)
