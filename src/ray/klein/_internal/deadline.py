# SPDX-License-Identifier: Apache-2.0
"""Monotonic deadline shared by multi-step blocking operations."""

import time


class Deadline:
    """Track one total timeout budget across multiple blocking steps."""

    def __init__(self, budget: float) -> None:
        self._end = time.monotonic() + max(0.0, float(budget))

    def remaining(self) -> float:
        return max(0.0, self._end - time.monotonic())

    def step(self, cap: float) -> float:
        """Return the smaller of the remaining budget and a per-step cap."""

        return min(self.remaining(), max(0.0, float(cap)))

    def expired(self) -> bool:
        return self.remaining() <= 0.0
