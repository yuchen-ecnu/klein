# SPDX-License-Identifier: Apache-2.0

from ray.klein.api.runtime_context import RuntimeContext
from ray.klein.runtime.message import Record
from ray.klein.runtime.partitioning.channel_topology import FORWARD
from ray.klein.runtime.partitioning.partitioner import Partitioner
from ray.klein.runtime.partitioning.partitioner_spec import PartitionerSpec


class ForwardPartitioner(Partitioner):
    """Default partition for operator if the operator can be chained with
    succeeding operators."""

    topology = FORWARD

    def __init__(self) -> None:
        super().__init__()
        self._partitions: list[int] = [0]

    def open(self, runtime_context: RuntimeContext, partition_count: int) -> None:
        super().open(runtime_context, partition_count)
        if runtime_context.task_index >= partition_count:
            raise ValueError(
                f"forward task index {runtime_context.task_index} is outside downstream parallelism {partition_count}"
            )
        self._partitions = [runtime_context.task_index]

    def partition(self, record: Record) -> list[int]:
        return self._partitions

    def to_spec(self) -> PartitionerSpec:
        return PartitionerSpec(type(self), name=str(self), topology=self.topology)

    def __str__(self) -> str:
        return "FORWARD"
