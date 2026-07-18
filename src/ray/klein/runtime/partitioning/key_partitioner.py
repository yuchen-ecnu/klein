# SPDX-License-Identifier: Apache-2.0
from collections.abc import Callable

from ray.klein._internal.block import block_row_dict
from ray.klein.runtime.message import Record
from ray.klein.runtime.partitioning.partitioner import Partitioner
from ray.klein.state.key_group_range import key_group_for_key, key_group_owner


class KeyPartitioner(Partitioner):
    """Partition the record by the key."""

    def __init__(
        self,
        key_selector: Callable,
        *,
        max_parallelism: int | None = None,
    ) -> None:
        super().__init__()
        if not callable(key_selector):
            raise TypeError("key_selector must be callable")
        self._partitions: list[int] = [-1]
        self._key_selector = key_selector
        self._configured_max_parallelism = max_parallelism
        self._max_parallelism: int | None = None

    def open(self, runtime_context, target_tasks) -> None:
        from ray.klein.config.state_options import StateOptions

        super().open(runtime_context, target_tasks)
        configured = self._configured_max_parallelism
        self._max_parallelism = (
            configured if configured is not None else runtime_context.config.get(StateOptions.MAX_PARALLELISM)
        )
        if isinstance(self._max_parallelism, bool) or not isinstance(self._max_parallelism, int):
            raise TypeError("state.keyed.max-parallelism must be an integer")
        if self._max_parallelism < self._partition_count:
            raise ValueError("state.keyed.max-parallelism must be >= downstream parallelism")

    def partition(self, record: Record) -> list[int]:
        if self._partition_count is None or self._partition_count <= 0:
            raise RuntimeError("KeyPartitioner must be opened before routing records")
        self._partitions[0] = self._target_for_key(self._key_selector(record.block))
        return self._partitions

    def partition_columnar(self, record: Record, num_rows: int) -> list[tuple[int, list[int] | None]]:
        """Group a columnar batch's rows by hashed key into per-target slices.

        Each row's key is computed from its own row-dict (the contract the
        key_selector expects), hashed to a target, and the row index appended to
        that target's bucket. Rows with different keys therefore land on
        different downstream tasks — key affinity is preserved even though the
        batch arrived as a single columnar record. A single-key batch yields one
        whole-batch pair (row_indices None) so the common case stays copy-free.
        """
        block = record.block
        buckets: dict = {}
        order: list[int] = []
        for row_index in range(num_rows):
            target = self._target_for_key(self._key_selector(block_row_dict(block, row_index)))
            if target not in buckets:
                buckets[target] = []
                order.append(target)
            buckets[target].append(row_index)
        if len(order) == 1:
            # All rows hash to the same target — ship the whole batch, no slice.
            return [(order[0], None)]
        return [(target, buckets[target]) for target in order]

    def __str__(self) -> str:
        return "HASH"

    def _target_for_key(self, key) -> int:
        if self._max_parallelism is None or self._partition_count is None:
            raise RuntimeError("KeyPartitioner must be opened before routing records")
        key_group = key_group_for_key(key, self._max_parallelism)
        return key_group_owner(key_group, self._max_parallelism, self._partition_count)
