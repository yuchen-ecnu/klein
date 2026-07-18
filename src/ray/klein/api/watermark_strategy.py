# SPDX-License-Identifier: Apache-2.0
"""Public event-time watermark strategy."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from datetime import timedelta
from typing import Any


@dataclass(frozen=True, slots=True)
class WatermarkStrategy:
    """Assign event timestamps and generate bounded-out-of-order watermarks."""

    timestamp_assigner: Callable[[Mapping[str, Any]], int]
    max_out_of_orderness_ms: int = 0
    idle_timeout_ms: int | None = None

    def __post_init__(self) -> None:
        if not callable(self.timestamp_assigner):
            raise TypeError("timestamp_assigner must be callable")
        if self.max_out_of_orderness_ms < 0:
            raise ValueError("max out-of-orderness must not be negative")
        if self.idle_timeout_ms is not None and self.idle_timeout_ms <= 0:
            raise ValueError("idle timeout must be greater than zero")

    @classmethod
    def for_monotonous_timestamps(
        cls,
        timestamp_assigner: Callable[[Mapping[str, Any]], int],
    ) -> WatermarkStrategy:
        return cls(timestamp_assigner)

    @classmethod
    def for_bounded_out_of_orderness(
        cls,
        max_out_of_orderness: timedelta,
        timestamp_assigner: Callable[[Mapping[str, Any]], int],
    ) -> WatermarkStrategy:
        milliseconds = int(max_out_of_orderness.total_seconds() * 1000)
        return cls(timestamp_assigner, max_out_of_orderness_ms=milliseconds)

    def with_idleness(self, idle_timeout: timedelta) -> WatermarkStrategy:
        milliseconds = int(idle_timeout.total_seconds() * 1000)
        return replace(self, idle_timeout_ms=milliseconds)
