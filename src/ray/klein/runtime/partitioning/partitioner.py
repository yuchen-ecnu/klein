# SPDX-License-Identifier: Apache-2.0
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from ray.klein.api.runtime_context import RuntimeContext
from ray.klein.runtime.message import Record
from ray.klein.runtime.partitioning.channel_topology import ALL_TO_ALL, ChannelTopology

if TYPE_CHECKING:
    from ray.klein.runtime.partitioning.partitioner_spec import PartitionerSpec


class Partitioner(ABC):
    """Task-local data routing policy.

    Physical/control topology belongs to :attr:`topology`; retry candidates are
    decided while still on the operator thread and are immutable afterwards.
    The send layer therefore never calls back into a mutable partitioner.
    """

    topology: ChannelTopology = ALL_TO_ALL

    def __init__(self) -> None:
        self._partition_count: int | None = None

    def open(self, runtime_context: RuntimeContext, partition_count: int) -> None:
        if partition_count <= 0:
            raise ValueError("partition count must be greater than zero")
        self._partition_count = partition_count

    @abstractmethod
    def to_spec(self) -> "PartitionerSpec":
        """Return an immutable recipe containing every constructor argument."""

    @abstractmethod
    def partition(self, record: Record) -> list[int]:
        """Return the downstream partition indices for one record."""

    def partition_columnar(self, record: Record, num_rows: int) -> list[tuple[int, list[int] | None]]:
        """Route a *columnar* batch without exploding it into per-row records.

        Returns ``(target_index, row_indices)`` pairs. ``row_indices is None`` is a
        fast-path meaning "the WHOLE batch goes to this target" (no slicing) —
        used by content-independent partitioners (forward / broadcast /
        round-robin / worker-pool), where routing doesn't depend on row values,
        so the batch travels as one unit and balancing granularity is per-batch.

        Key/hash partitioning overrides this to group rows by key and return one
        ``(target_index, [row...])`` pair per distinct target (see KeyPartitioner).

        Default implementation defers to :meth:`partition` (called once with the
        batch record, content ignored by these partitioners) and ships the whole
        batch to each returned target.
        """
        return [(index, None) for index in self.partition(record)]

    def retry_targets(self, initial_target: int) -> tuple[int, ...]:
        """Return the finite retry ring, starting with ``initial_target``.

        The default preserves affinity. Load-balancing partitioners override it
        with their static eligible channel ring. The RecordRouter validates and freezes
        this decision before it crosses from the operator thread to the emit loop.
        """
        return (initial_target,)
