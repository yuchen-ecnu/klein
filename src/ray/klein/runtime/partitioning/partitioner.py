# SPDX-License-Identifier: Apache-2.0
from abc import ABC, abstractmethod

from ray.klein.api.runtime_context import RuntimeContext
from ray.klein.runtime.actor import KleinActorHandle
from ray.klein.runtime.message import Record


class Partitioner(ABC):
    """Routes records and control messages to downstream partitions."""

    def __init__(self) -> None:
        self._partition_count: int | None = None
        self._runtime_context: RuntimeContext | None = None
        self._target_tasks: list[KleinActorHandle] = []

    def open(self, runtime_context: RuntimeContext, target_tasks: list[KleinActorHandle]) -> None:
        self._target_tasks = target_tasks
        self._partition_count = len(target_tasks)
        self._runtime_context = runtime_context

    @abstractmethod
    def partition(self, record: Record) -> list[int]:
        """Return the downstream partition indices for one record."""

    def partition_columnar(self, record: Record, num_rows: int) -> list[tuple[int, list[int] | None]]:
        """Route a *columnar* batch without exploding it into per-row records.

        Returns ``(target_index, row_indices)`` pairs. ``row_indices is None`` is a
        fast-path meaning "the WHOLE batch goes to this target" (no slicing) —
        used by content-independent partitioners (forward / broadcast /
        round-robin / worker-pool), where routing doesn't depend on row values,
        so the batch travels as one unit and balancing granularity is per-batch.

        Key/hash partitioning overrides this to group rows by key and return one
        ``(target_index, [row...])`` pair per distinct target (see KeyPartitioner).

        Default implementation defers to :meth:`partition` (called once with the
        batch record, content ignored by these partitioners) and ships the whole
        batch to each returned target.
        """
        return [(index, None) for index in self.partition(record)]

    def target_tasks(self, source_parallelism: int, target_parallelism: int, source_index: int) -> list[int]:
        """Return control-message targets for one upstream subtask."""
        return list(range(target_parallelism))

    def on_record_emitted(self, target_task: int, buffer_size: int) -> None:
        """Observe a successful emit and the target's resulting buffer size."""
        return

    def on_barrier_emitted(self, buffer_sizes: list[int]) -> None:
        """Observe buffer sizes returned by a barrier broadcast."""
        return

    def on_record_emit_timeout(self, record: Record, target_task: int, buffer_size: int) -> int:
        """Return the partition index to retry after backpressure."""
        return target_task

    @property
    def can_reroute(self) -> bool:
        """Whether a timed-out emit may be retried on a *different* downstream.

        True for load-balancing partitioners (worker-pool / round-robin): a full
        downstream can be skipped. False for partitioners with a fixed routing
        contract (key/hash, forward) — rerouting would break key affinity, so a
        full downstream must be waited on and retried with the same target.
        """
        return False
