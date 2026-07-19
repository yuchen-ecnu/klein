# SPDX-License-Identifier: Apache-2.0
"""Per-edge, per-target micro-batching before downstream delivery."""

import time
from collections.abc import Iterator

from ray.klein._internal.memory import estimate_retained_size
from ray.klein.runtime.message import Record


class DownstreamBatcher:
    """Executor-thread-only record batches with per-target idle clocks."""

    def __init__(
        self,
        target_count: int,
        batch_size: int,
        idle_timeout: float = 3.0,
        *,
        max_rows: int | None = None,
        max_bytes: int | None = None,
    ) -> None:
        if target_count <= 0:
            raise ValueError("downstream batcher target count must be positive")
        if batch_size < 0:
            raise ValueError("downstream batch size cannot be negative")
        if idle_timeout <= 0:
            raise ValueError("downstream batch idle timeout must be positive")
        if max_rows is not None and max_rows <= 0:
            raise ValueError("downstream batch row limit must be positive")
        if max_bytes is not None and max_bytes <= 0:
            raise ValueError("downstream batch byte limit must be positive")
        self._batches: list[list[Record]] = [[] for _ in range(target_count)]
        self._started_at: list[float | None] = [None] * target_count
        self._rows: list[int] = [0] * target_count
        self._bytes: list[int] = [0] * target_count
        # Zero is the documented unbatched mode.
        self._batch_size = max(1, batch_size)
        self._max_rows = max_rows
        self._max_bytes = max_bytes
        self._idle_timeout = idle_timeout

    def append(self, target_index: int, record: Record) -> None:
        batch = self._batches[target_index]
        if not batch:
            self._started_at[target_index] = time.monotonic()
        batch.append(record)
        self._rows[target_index] += 1 if record.num_rows is None else record.num_rows
        self._bytes[target_index] += estimate_retained_size(record)

    def take_full(self, target_index: int) -> tuple[Record, ...]:
        batch = self._batches[target_index]
        full = len(batch) >= self._batch_size
        full = full or (self._max_rows is not None and self._rows[target_index] >= self._max_rows)
        full = full or (self._max_bytes is not None and self._bytes[target_index] >= self._max_bytes)
        if not full:
            return ()
        return self._take(target_index)

    def drain(self, force: bool = True) -> Iterator[tuple[int, tuple[Record, ...]]]:
        now = time.monotonic()
        for target_index, batch in enumerate(self._batches):
            started_at = self._started_at[target_index]
            if batch and (force or (started_at is not None and now - started_at >= self._idle_timeout)):
                yield target_index, self._take(target_index)

    def _take(self, target_index: int) -> tuple[Record, ...]:
        batch = tuple(self._batches[target_index])
        self._batches[target_index] = []
        self._started_at[target_index] = None
        self._rows[target_index] = 0
        self._bytes[target_index] = 0
        return batch
