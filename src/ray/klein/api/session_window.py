# SPDX-License-Identifier: Apache-2.0
from datetime import timedelta

from ray.klein.api.time_window import TimeWindow
from ray.klein.api.tumbling_window import _positive_milliseconds
from ray.klein.api.window_assigner import WindowAssigner


class SessionWindow(WindowAssigner):
    def __init__(self, gap: timedelta) -> None:
        self._gap = _positive_milliseconds(gap, "gap")

    @property
    def is_merging(self) -> bool:
        return True

    def assign_windows(self, timestamp: int) -> tuple[TimeWindow, ...]:
        return (TimeWindow(timestamp, timestamp + self._gap),)
