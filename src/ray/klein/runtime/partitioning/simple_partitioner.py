# SPDX-License-Identifier: Apache-2.0
from collections.abc import Callable

from ray.klein._internal.block import block_row_dict
from ray.klein.runtime.message import Record
from ray.klein.runtime.partitioning.partitioner import Partitioner


class SimplePartitioner(Partitioner):
    """Wrap a python function as subclass of :class:`Partition`"""

    def __init__(self, fn: Callable) -> None:
        super().__init__()
        self.fn = fn

    def partition(self, record: Record) -> list[int]:
        return self.fn(record, self._partition_count)

    def partition_columnar(self, record: Record, num_rows: int) -> list[tuple[int, list[int] | None]]:
        """Per-row custom routing, bucketed by target (preserves fn semantics).

        A user partition fn is inherently per-record, so apply it to each row's
        own record and group the resulting targets — never assume the whole
        batch shares a target. A row may fan out to multiple targets (the fn
        returns a list), so each is bucketed independently.
        """
        block = record.block
        buckets: dict = {}
        order: list[int] = []
        for row_index in range(num_rows):
            row_record = Record(block_row_dict(block, row_index))
            for target in self.fn(row_record, self._partition_count):
                if target not in buckets:
                    buckets[target] = []
                    order.append(target)
                buckets[target].append(row_index)
        if len(order) == 1 and len(buckets[order[0]]) == num_rows:
            return [(order[0], None)]
        return [(target, buckets[target]) for target in order]

    def __str__(self) -> str:
        return "CUSTOM"
