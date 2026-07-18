# SPDX-License-Identifier: Apache-2.0
from datetime import timedelta

from ray.klein.api.time_window import TimeWindow
from ray.klein.api.window_assigner import WindowAssigner


class TumblingWindow(WindowAssigner):
    def __init__(self, size: timedelta, offset: timedelta = timedelta(0)) -> None:
        self._size = _positive_milliseconds(size, "size")
        self._offset = int(offset.total_seconds() * 1000) % self._size

    def assign_windows(self, timestamp: int) -> tuple[TimeWindow, ...]:
        start = ((timestamp - self._offset) // self._size) * self._size + self._offset
        return (TimeWindow(start, start + self._size),)


def _positive_milliseconds(value: timedelta, name: str) -> int:
    milliseconds = int(value.total_seconds() * 1000)
    if milliseconds <= 0:
        raise ValueError(f"window {name} must be positive")
    return milliseconds
