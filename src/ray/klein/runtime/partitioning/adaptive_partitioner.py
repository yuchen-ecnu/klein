# SPDX-License-Identifier: Apache-2.0

from ray.klein.api.runtime_context import RuntimeContext
from ray.klein.runtime.actor import KleinActorHandle
from ray.klein.runtime.message import Record
from ray.klein.runtime.partitioning.partitioner import Partitioner
from ray.klein.runtime.partitioning.worker_pool_dispatcher import WorkerPoolDispatcher


class AdaptivePartitioner(Partitioner):
    """Worker-pool partitioner.

    Routes each batch to the next downstream task in a ring; backpressure (a full
    downstream inbox -> ``put`` timeout -> :meth:`on_record_emit_timeout`) shifts
    load off busy tasks. No PriorityQueue, no synchronous buffer-size RPCs, no
    cold-start ghost-zero bias.
    """

    def __init__(self) -> None:
        super().__init__()
        self._dispatcher: WorkerPoolDispatcher | None = None

    def open(self, runtime_context: RuntimeContext, target_tasks: list[KleinActorHandle]) -> None:
        super().open(runtime_context, target_tasks)
        self._dispatcher = WorkerPoolDispatcher(list(range(self._partition_count)))

    def partition(self, record: Record) -> list[int]:
        return [self._dispatcher.current()]

    def on_record_emitted(self, target_task: int, buffer_size: int) -> None:
        # Advance the ring after each successful emit so load spreads evenly.
        self._dispatcher.advance()

    def on_record_emit_timeout(self, record: Record, target_task: int, buffer_size: int) -> int:
        # Downstream inbox is full — move to the next worker immediately.
        return self._dispatcher.advance()

    @property
    def can_reroute(self) -> bool:
        return True

    def __str__(self) -> str:
        return "ADAPTIVE"
