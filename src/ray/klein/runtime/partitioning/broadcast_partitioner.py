# SPDX-License-Identifier: Apache-2.0

from ray.klein.runtime.message import Record
from ray.klein.runtime.partitioning.partitioner import Partitioner
from ray.klein.runtime.partitioning.partitioner_spec import PartitionerSpec


class BroadcastPartitioner(Partitioner):
    """Broadcast the record to all downstream partitions."""

    def __init__(self) -> None:
        super().__init__()
        self._partitions: list[int] = []

    def partition(self, record: Record) -> list[int]:
        if self._partition_count is None or self._partition_count <= 0:
            raise RuntimeError("BroadcastPartitioner must be opened before routing records")
        if len(self._partitions) != self._partition_count:
            self._partitions = list(range(self._partition_count))
        return self._partitions

    def to_spec(self) -> PartitionerSpec:
        return PartitionerSpec(type(self), name=str(self), topology=self.topology)

    def __str__(self) -> str:
        return "BROADCAST"
