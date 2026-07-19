# SPDX-License-Identifier: Apache-2.0

from ray.klein.api.runtime_context import RuntimeContext
from ray.klein.runtime.message import Record
from ray.klein.runtime.partitioning.channel_topology import RESCALE
from ray.klein.runtime.partitioning.partitioner import Partitioner
from ray.klein.runtime.partitioning.partitioner_spec import PartitionerSpec
from ray.klein.runtime.partitioning.worker_pool_dispatcher import WorkerPoolDispatcher


class RescalePartitioner(Partitioner):
    """Worker-pool partitioner over a statically-assigned subset of downstreams.

    Each upstream instance owns a fixed slice of the downstream tasks (round-robin
    distribution) and dispatches within that slice with the same backpressure-driven
    ring as :class:`AdaptivePartitioner`.
    """

    topology = RESCALE

    def __init__(self) -> None:
        super().__init__()
        self._current_assignment: list[int] = []
        self._dispatcher: WorkerPoolDispatcher | None = None

    def open(self, runtime_context: RuntimeContext, partition_count: int) -> None:
        super().open(runtime_context, partition_count)
        self._current_assignment = list(
            self.topology.target_indices(
                runtime_context.parallelism,
                self._partition_count,
                runtime_context.task_index,
            )
        )
        if len(self._current_assignment) <= 0:
            raise RuntimeError(
                f"[{runtime_context.task_name}]: unexpected assignment in rescale partitioner. "
                f"completed assignments is {self._current_assignment}"
            )
        self._dispatcher = WorkerPoolDispatcher(self._current_assignment)

    def partition(self, record: Record) -> list[int]:
        if self._dispatcher is None:
            raise RuntimeError("RescalePartitioner must be opened before routing records")
        return [self._dispatcher.take()]

    def retry_targets(self, initial_target: int) -> tuple[int, ...]:
        if self._dispatcher is None:
            raise RuntimeError("RescalePartitioner must be opened before routing records")
        return self._dispatcher.ring_from(initial_target)

    def to_spec(self) -> PartitionerSpec:
        return PartitionerSpec(type(self), name=str(self), topology=self.topology)

    def __str__(self) -> str:
        return "RESCALE"

    @staticmethod
    def distribute_tasks(source_parallelism: int, target_parallelism: int, source_index: int) -> list[int]:
        return list(RESCALE.target_indices(source_parallelism, target_parallelism, source_index))
