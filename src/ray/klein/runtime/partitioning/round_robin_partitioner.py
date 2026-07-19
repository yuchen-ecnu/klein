# SPDX-License-Identifier: Apache-2.0

from ray.klein.runtime.message import Record
from ray.klein.runtime.partitioning.partitioner import Partitioner
from ray.klein.runtime.partitioning.partitioner_spec import PartitionerSpec


class RoundRobinPartitioner(Partitioner):
    """Partition record to downstream tasks in a round-robin matter."""

    def __init__(self) -> None:
        super().__init__()
        self._partitions: list[int] = [0]
        self._cursor: int = 0

    def partition(self, record: Record) -> list[int]:
        if self._partition_count is None or self._partition_count <= 0:
            raise RuntimeError("RoundRobinPartitioner must be opened before routing records")
        self._partitions[0] = self._cursor
        self._cursor = (self._cursor + 1) % self._partition_count
        return self._partitions

    def retry_targets(self, initial_target: int) -> tuple[int, ...]:
        if self._partition_count is None or self._partition_count <= 0:
            raise RuntimeError("RoundRobinPartitioner must be opened before routing records")
        return tuple((initial_target + offset) % self._partition_count for offset in range(self._partition_count))

    def to_spec(self) -> PartitionerSpec:
        return PartitionerSpec(type(self), name=str(self), topology=self.topology)

    def __str__(self) -> str:
        return "REBALANCE"
