# SPDX-License-Identifier: Apache-2.0
"""Routing layer over a Partitioner for the OutputCollector.

Turns one record into ``(target_index, payload)`` pairs and selects the
broadcast targets for a barrier. Pure routing + columnar slicing — no network,
no batching, no replay state. All partitioner decisions happen here on the
executor thread, which is what lets the loop-side emitter stay lock-free.
"""

from collections.abc import Iterator

from ray.klein._internal.block import slice_block_rows
from ray.klein.runtime.actor import KleinActorHandle
from ray.klein.runtime.context.runtime_context import OperatorRuntimeContext
from ray.klein.runtime.message import Record
from ray.klein.runtime.partitioning.partitioner import Partitioner


class Router:
    """Wraps a Partitioner: record -> (target_index, payload) and barrier targets."""

    def __init__(self, partitioner: Partitioner, target_tasks: list[KleinActorHandle]) -> None:
        self._partitioner = partitioner
        self._target_tasks = target_tasks

    def open(self, op_runtime_context: OperatorRuntimeContext) -> None:
        self._partitioner.open(op_runtime_context, self._target_tasks)

    def route(self, record: Record) -> Iterator[tuple[int, Record]]:
        """Yield ``(target_index, record_or_slice)`` for one data record.

        A row record fans out whole to each partitioned target; a columnar batch
        is sliced per target (or passed whole when ``row_indices is None``).
        """
        if not record.is_columnar:
            for target_index in self._partitioner.partition(record):
                yield target_index, record
            return
        for target_index, row_indices in self._partitioner.partition_columnar(record, record.num_rows):
            if row_indices is None:
                yield target_index, record
            else:
                sub_block = slice_block_rows(record.block, row_indices)
                yield target_index, Record(sub_block, num_rows=len(row_indices))

    def barrier_target_indices(self, source_parallelism: int, source_index: int) -> list[int]:
        """Indices that a barrier broadcasts to (delegated to the partitioner)."""
        return self._partitioner.target_tasks(source_parallelism, len(self._target_tasks), source_index)

    # --- congestion / cadence feedback (executor thread only) ---

    def on_record_emitted(self, target_index: int, buffer_size: int) -> None:
        self._partitioner.on_record_emitted(target_index, buffer_size)

    def on_record_emit_timeout(self, record: Record, target_index: int, buffer_size: int) -> int:
        return self._partitioner.on_record_emit_timeout(record, target_index, buffer_size)

    def on_barrier_emitted(self, buffer_sizes: list[int]) -> None:
        self._partitioner.on_barrier_emitted(buffer_sizes)

    @property
    def can_reroute(self) -> bool:
        return self._partitioner.can_reroute
