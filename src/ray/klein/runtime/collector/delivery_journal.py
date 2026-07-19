# SPDX-License-Identifier: Apache-2.0
"""Single-fault at-least-once delivery journal for one downstream edge.

This class owns the complete per-downstream replay protocol:

* a monotonic per-target sequence assigned when a put lands,
* a per-target FIFO of ``(sequence, records)`` still awaiting the downstream's
  forwarded-watermark ack,
* a per-target highest acknowledged sequence.

The sequence travels with the batch without allowing rerouting to leave gaps:

1. ``next_sequence(target_index)`` returns the next sequence without
   committing it. The value travels downstream as ``batch_sequence``.
2. Send the batch with that sequence.
3. On a successful landing, ``record_delivery`` advances the target's sequence
   and, when enabled, appends to its replay FIFO. A put that is
   rerouted to another target never commits this index, so reroute punches no
   hole that would wedge the watermark.
4. the downstream echoes its forwarded watermark on the put ack;
   ``acknowledge`` drops every entry at or below the forwarded sequence.

Thread-safety: the inline (source) emit path mutates this on the executor thread
while replay-command extraction may read it on the actor loop, so a lock guards the
buffer. Uncontended for the common pipelined case (single-threaded on the loop).
"""

import threading
from collections import deque
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any

from ray.klein._internal.memory import estimate_retained_size
from ray.klein.runtime.message import DeliveryChannel, Record


@dataclass(frozen=True, slots=True)
class _ReplayEntry:
    sequence: int
    records: tuple[Record, ...]
    row_count: int
    size_bytes: int


class DeliveryJournalCapacityError(MemoryError):
    """Replay retention reached its configured guard before process OOM."""


class DeliveryJournal:
    """Own sequence assignment, replay retention, and forwarded acknowledgements."""

    def __init__(self, target_count: int) -> None:
        # Disabled until enable(); a source/sink or a job with the feature off
        # keeps these inert and pays nothing.
        self._enabled: bool = False
        # vertex_id of THIS task, stamped on every put so the downstream can key its
        # per-sender forwarded-watermark. Set in enable().
        self._sender_vertex_id = None
        self._sender_task_name: str | None = None
        self._edge_index: int | None = None
        self._topology_epoch: str | None = None
        # Per-target monotonic batch sequence. Peeked before the put (travels to
        # the downstream as batch_sequence) and committed only on landing on the ACTUAL
        # index (post-reroute), so worker-pool reroute can't punch holes that wedge
        # the watermark. Index == target task index.
        self._sequence: list[int] = [0] * target_count
        # Per-target FIFO of (batch_sequence, records) still awaiting the downstream's
        # forwarded-watermark acknowledgement, in sequence order.
        self._buffer: list[deque[_ReplayEntry]] = [deque() for _ in range(target_count)]
        # Per-target highest acknowledged sequence.
        self._forwarded: list[int] = [-1] * target_count
        self._lock = threading.Lock()
        self._size_observer: Callable[[int], None] | None = None
        self._byte_size_observer: Callable[[int], None] | None = None
        self._buffered_bytes = 0
        self._buffered_records = 0
        self._max_bytes = 0

    def configure(
        self,
        enabled: bool,
        sender_vertex_id: Any = None,
        max_bytes: int = 0,
        *,
        sender_task_name: str | None = None,
        edge_index: int | None = None,
        topology_epoch: str | None = None,
    ) -> None:
        """Configure replay retention and stamp this task's sender id."""
        if enabled and max_bytes <= 0:
            raise ValueError("replay buffer max bytes must be greater than zero when replay is enabled")
        if max_bytes < 0:
            raise ValueError("replay buffer max bytes cannot be negative")
        self._enabled = enabled
        self._sender_vertex_id = sender_vertex_id
        self._sender_task_name = sender_task_name
        self._edge_index = edge_index
        self._topology_epoch = topology_epoch
        self._max_bytes = max_bytes
        self._publish_size()

    def attach_observers(
        self,
        observer: Callable[[int], None] | None,
        byte_observer: Callable[[int], None] | None = None,
    ) -> None:
        self._size_observer = observer
        self._byte_size_observer = byte_observer
        self._publish_size()

    @property
    def enabled(self) -> bool:
        return self._enabled

    @property
    def sender_vertex_id(self) -> Any:
        return self._sender_vertex_id

    def delivery_channel(self, target_index: int) -> DeliveryChannel | None:
        if self._sender_vertex_id is None or self._sender_task_name is None or self._edge_index is None:
            return None
        return DeliveryChannel(
            self._sender_vertex_id,
            self._sender_task_name,
            self._edge_index,
            target_index,
            self._topology_epoch,
        )

    def next_sequence(self, target_index: int) -> int:
        """Return the next sequence for a target without committing it."""
        with self._lock:
            return self._sequence[target_index] + 1

    def record_delivery(self, target_index: int, records: Sequence[Record], sequence: int) -> None:
        """Commit a landed batch's sequence and buffer it for possible replay.

        The buffer is drained by the forwarded watermark. A hard retained-memory
        guard fails into normal task recovery before a stalled watermark can OOM
        the worker process.
        """
        with self._lock:
            if self._enabled:
                retained_records = tuple(records)
                row_count = sum(1 if record.num_rows is None else record.num_rows for record in retained_records)
                size_bytes = estimate_retained_size(retained_records)
                next_size = self._buffered_bytes + size_bytes
                if self._max_bytes > 0 and next_size > self._max_bytes:
                    raise DeliveryJournalCapacityError(
                        f"replay buffer would retain {next_size} bytes, above configured "
                        f"pipeline.replay-buffer.max-bytes={self._max_bytes}"
                    )
                self._buffer[target_index].append(_ReplayEntry(sequence, retained_records, row_count, size_bytes))
                self._buffered_records += row_count
                self._buffered_bytes = next_size
            self._sequence[target_index] = sequence
        self._publish_size()

    def acknowledge(self, target_index: int, forwarded_sequence: int) -> None:
        """Drop replay-buffer entries the downstream has confirmed forwarding.

        Idempotent and monotonic: a stale/older watermark is ignored. Driven by
        the watermark carried back on each put() ack.
        """
        changed = False
        with self._lock:
            if not self._enabled or forwarded_sequence <= self._forwarded[target_index]:
                return
            self._forwarded[target_index] = forwarded_sequence
            buffer = self._buffer[target_index]
            while buffer and buffer[0].sequence <= forwarded_sequence:
                entry = buffer.popleft()
                self._buffered_records -= entry.row_count
                self._buffered_bytes -= entry.size_bytes
                changed = True
        if changed:
            self._publish_size()

    def pending_for(self, target_index: int) -> tuple[tuple[int, tuple[Record, ...]], ...]:
        """Return buffered ``(sequence, records)`` pairs in sequence order."""
        with self._lock:
            return tuple((entry.sequence, entry.records) for entry in self._buffer[target_index])

    @property
    def buffered_record_count(self) -> int:
        with self._lock:
            return self._buffered_records

    @property
    def buffered_bytes(self) -> int:
        with self._lock:
            return self._buffered_bytes

    def _publish_size(self) -> None:
        observer = self._size_observer
        if observer is not None:
            observer(self.buffered_record_count)
        byte_observer = self._byte_size_observer
        if byte_observer is not None:
            byte_observer(self.buffered_bytes)
