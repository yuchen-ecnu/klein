# SPDX-License-Identifier: Apache-2.0
"""Small asyncio FIFO whose capacity is measured by item weight, not envelopes."""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Callable
from typing import Generic, TypeVar

T = TypeVar("T")


class WeightedQueue(Generic[T]):
    """A cancellation-safe weighted FIFO.

    An item larger than the configured capacity is admitted only into an empty
    queue. This preserves progress while ensuring one large columnar block cannot
    be accompanied by more buffered data.
    """

    def __init__(
        self,
        max_weight: int,
        weigh: Callable[[T], int],
        *,
        max_bytes: int | None = None,
        size_bytes: Callable[[T], int] | None = None,
    ) -> None:
        if max_weight <= 0:
            raise ValueError("weighted queue capacity must be greater than zero")
        if max_bytes is not None and max_bytes <= 0:
            raise ValueError("weighted queue byte capacity must be greater than zero")
        if (max_bytes is None) != (size_bytes is None):
            raise ValueError("max_bytes and size_bytes must be configured together")
        self._max_weight = max_weight
        self._weigh = weigh
        self._max_bytes = max_bytes
        self._size_bytes = size_bytes
        self._items: deque[tuple[T, int, int]] = deque()
        self._weight = 0
        self._bytes = 0
        self._condition = asyncio.Condition()

    async def put(self, item: T) -> None:
        weight, size_bytes = self._measure(item)
        async with self._condition:
            await self._condition.wait_for(lambda: self._can_admit(weight, size_bytes))
            self._admit(item, weight, size_bytes)

    async def try_put(self, item: T) -> bool:
        """Admit immediately when capacity is available, without timeout RPCs."""
        weight, size_bytes = self._measure(item)
        async with self._condition:
            if not self._can_admit(weight, size_bytes):
                return False
            self._admit(item, weight, size_bytes)
            return True

    async def get(self) -> T:
        async with self._condition:
            await self._condition.wait_for(lambda: bool(self._items))
            item, weight, size_bytes = self._items.popleft()
            self._weight -= weight
            self._bytes -= size_bytes
            self._condition.notify_all()
            return item

    def _measure(self, item: T) -> tuple[int, int]:
        weight = self._weigh(item)
        if weight <= 0:
            raise ValueError(f"queue item weight must be greater than zero, got {weight}")
        size_bytes = 0 if self._size_bytes is None else self._size_bytes(item)
        if size_bytes < 0:
            raise ValueError(f"queue item size must not be negative, got {size_bytes}")
        return weight, size_bytes

    def _can_admit(self, weight: int, size_bytes: int) -> bool:
        if not self._items:
            # One oversized item is exclusive so the queue always makes progress.
            return True
        if self._weight + weight > self._max_weight:
            return False
        return self._max_bytes is None or self._bytes + size_bytes <= self._max_bytes

    def _admit(self, item: T, weight: int, size_bytes: int) -> None:
        self._items.append((item, weight, size_bytes))
        self._weight += weight
        self._bytes += size_bytes
        self._condition.notify_all()

    def qsize(self) -> int:
        """Return queued logical weight (rows for a StreamTask inbox)."""
        return self._weight

    @property
    def byte_size(self) -> int:
        return self._bytes

    @property
    def envelope_count(self) -> int:
        return len(self._items)
