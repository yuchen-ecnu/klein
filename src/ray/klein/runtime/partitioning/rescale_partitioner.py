# SPDX-License-Identifier: Apache-2.0

from ray.klein.api.runtime_context import RuntimeContext
from ray.klein.runtime.actor import KleinActorHandle
from ray.klein.runtime.message import Record
from ray.klein.runtime.partitioning.partitioner import Partitioner
from ray.klein.runtime.partitioning.worker_pool_dispatcher import WorkerPoolDispatcher


class RescalePartitioner(Partitioner):
    """Worker-pool partitioner over a statically-assigned subset of downstreams.

    Each upstream instance owns a fixed slice of the downstream tasks (round-robin
    distribution) and dispatches within that slice with the same backpressure-driven
    ring as :class:`AdaptivePartitioner`.
    """

    def __init__(self) -> None:
        super().__init__()
        self._current_assignment: list[int] = []
        self._dispatcher: WorkerPoolDispatcher | None = None

    def open(self, runtime_context: RuntimeContext, target_tasks: list[KleinActorHandle]) -> None:
        super().open(runtime_context, target_tasks)
        self._current_assignment = RescalePartitioner.distribute_tasks(
            runtime_context.parallelism,
            self._partition_count,
            runtime_context.task_index,
        )
        if len(self._current_assignment) <= 0:
            raise RuntimeError(
                f"[{runtime_context.task_name}]: unexpected assignment in rescale partitioner. "
                f"completed assignments is {self._current_assignment}"
            )
        self._dispatcher = WorkerPoolDispatcher(self._current_assignment)

    def partition(self, record: Record) -> list[int]:
        return [self._dispatcher.current()]

    def target_tasks(self, source_parallelism: int, target_parallelism: int, source_index: int) -> list[int]:
        return RescalePartitioner.distribute_tasks(source_parallelism, target_parallelism, source_index)

    def on_record_emitted(self, target_task: int, buffer_size: int) -> None:
        self._dispatcher.advance()

    def on_record_emit_timeout(self, record: Record, target_task: int, buffer_size: int) -> int:
        return self._dispatcher.advance()

    @property
    def can_reroute(self) -> bool:
        return True

    def __str__(self) -> str:
        return "RESCALE"

    @staticmethod
    def distribute_tasks(source_parallelism: int, target_parallelism: int, source_index: int) -> list[int]:
        if source_parallelism >= target_parallelism:
            # N->1
            return [source_index % target_parallelism]
        # 1->N: range(start_pod, end_pod, step_size)
        return list(range(source_index, target_parallelism, source_parallelism))
