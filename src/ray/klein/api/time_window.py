# SPDX-License-Identifier: Apache-2.0
from dataclasses import dataclass


@dataclass(frozen=True, order=True, slots=True)
class TimeWindow:
    """Half-open event-time interval ``[start, end)`` in milliseconds."""

    start: int
    end: int

    def __post_init__(self) -> None:
        if self.end <= self.start:
            raise ValueError("window end must be greater than start")

    def overlaps(self, other: "TimeWindow") -> bool:
        return self.start < other.end and other.start < self.end

    def merge(self, other: "TimeWindow") -> "TimeWindow":
        return TimeWindow(min(self.start, other.start), max(self.end, other.end))
