# SPDX-License-Identifier: Apache-2.0
"""Output-side micro-batching for the OutputCollector.

One ``OutputBatcher`` buffers emitted records per downstream target index and
releases them in two ways:

* ``pop_when_reach_limit`` — size-triggered: as soon as a target accumulates
  ``internal_batch_size`` records, that target's batch is released (the hot path
  in ``collect``).
* ``pop`` — flush-triggered (barrier / idle / teardown): release every target's
  buffered records. ``force=True`` flushes unconditionally; ``force=False`` only
  flushes once ``idle_flush_seconds`` have elapsed since the last flush, so a
  low-rate stream still releases buffered micro-batches without pinning them.

This is purely in-memory bookkeeping: it performs no routing or network I/O.
"""

import time
from collections.abc import Iterator

from ray.klein.runtime.message import Record


class OutputBatcher:
    """Per-downstream-target micro-batch buffer (executor-thread only)."""

    def __init__(
        self,
        target_count: int,
        internal_batch_size: int,
        idle_flush_seconds: float = 3.0,
    ) -> None:
        self._holder: dict[int, list[Record]] = {index: [] for index in range(target_count)}
        self._internal_batch_size: int = internal_batch_size
        self._idle_flush_seconds: float = idle_flush_seconds
        self._last_flush_time: float = time.perf_counter()

    def push(self, target_index: int, record: Record) -> None:
        self._holder[target_index].append(record)

    def pop(self, force: bool = True) -> Iterator[tuple[int, list[Record]]]:
        if force or (time.perf_counter() - self._last_flush_time > self._idle_flush_seconds):
            yield from self._pop()

    def pop_when_reach_limit(self, target_index: int) -> list[Record]:
        batches = self._holder[target_index]
        if len(batches) >= self._internal_batch_size:
            self._holder[target_index] = []
            return batches
        return []

    def _pop(self) -> Iterator[tuple[int, list[Record]]]:
        for index, batches in self._holder.items():
            self._holder[index] = []
            if batches:
                yield index, batches
        self._last_flush_time = time.perf_counter()
