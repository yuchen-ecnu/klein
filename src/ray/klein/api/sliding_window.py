# SPDX-License-Identifier: Apache-2.0
from datetime import timedelta

from ray.klein.api.time_window import TimeWindow
from ray.klein.api.tumbling_window import _positive_milliseconds
from ray.klein.api.window_assigner import WindowAssigner


class SlidingWindow(WindowAssigner):
    def __init__(
        self,
        size: timedelta,
        slide: timedelta,
        offset: timedelta = timedelta(0),
    ) -> None:
        self._size = _positive_milliseconds(size, "size")
        self._slide = _positive_milliseconds(slide, "slide")
        self._offset = int(offset.total_seconds() * 1000) % self._slide

    def assign_windows(self, timestamp: int) -> tuple[TimeWindow, ...]:
        last_start = ((timestamp - self._offset) // self._slide) * self._slide + self._offset
        windows = []
        start = last_start
        while start + self._size > timestamp:
            windows.append(TimeWindow(start, start + self._size))
            start -= self._slide
        return tuple(windows)
