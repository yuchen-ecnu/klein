# SPDX-License-Identifier: Apache-2.0

from ray.klein.api.runtime_context import RuntimeContext
from ray.klein.runtime.actor import KleinActorHandle
from ray.klein.runtime.message import Record
from ray.klein.runtime.partitioning.partitioner import Partitioner


class ForwardPartitioner(Partitioner):
    """Default partition for operator if the operator can be chained with
    succeeding operators."""

    def __init__(self) -> None:
        super().__init__()
        self._partitions: list[int] = [0]

    def open(self, runtime_context: RuntimeContext, target_tasks: list[KleinActorHandle]) -> None:
        super().open(runtime_context, target_tasks)
        self._partitions = [runtime_context.task_index]

    def partition(self, record: Record) -> list[int]:
        return self._partitions

    def target_tasks(self, source_parallelism: int, target_parallelism: int, source_index: int) -> list[int]:
        return [source_index]

    def __str__(self) -> str:
        return "FORWARD"
