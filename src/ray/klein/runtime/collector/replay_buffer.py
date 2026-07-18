# SPDX-License-Identifier: Apache-2.0
"""Single-fault at-least-once replay buffer for the OutputCollector.

This class owns the complete per-downstream replay protocol:

* a monotonic per-target sequence assigned when a put lands,
* a per-target FIFO of ``(sequence, records)`` still awaiting the downstream's
  forwarded-watermark ack,
* a per-target highest acknowledged sequence.

The sequence travels with the batch without allowing rerouting to leave gaps:

1. ``next_sequence_for(target_index)`` returns the next sequence without
   committing it. The value travels downstream as ``batch_sequence``.
2. Send the batch with that sequence.
3. On a successful landing, ``commit_landed`` advances the target's sequence
   and, when enabled, appends to its replay FIFO. A put that is
   rerouted to another target never commits this index, so reroute punches no
   hole that would wedge the watermark.
4. the downstream echoes its forwarded watermark on the put ack;
   ``advance_forwarded`` drops every entry at or below the forwarded sequence.

Thread-safety: the inline (source) emit path mutates this on the executor thread
while ``replay_ops_for_name`` may read it on the actor loop, so a lock guards the
buffer. Uncontended for the common pipelined case (single-threaded on the loop).
"""

import threading
from collections import deque
from collections.abc import Callable, Sequence
from typing import Any

from ray.klein.runtime.message import Record


class ReplayBuffer:
    """Owns sequence assignment, the replay FIFO, and forwarded watermarks."""

    def __init__(self, target_count: int) -> None:
        # Disabled until enable(); a source/sink or a job with the feature off
        # keeps these inert and pays nothing.
        self._enabled: bool = False
        # vertex_id of THIS task, stamped on every put so the downstream can key its
        # per-sender forwarded-watermark. Set in enable().
        self._sender_vertex_id = None
        # Per-target monotonic batch sequence. Peeked before the put (travels to
        # the downstream as batch_sequence) and committed only on landing on the ACTUAL
        # index (post-reroute), so worker-pool reroute can't punch holes that wedge
        # the watermark. Index == target task index.
        self._sequence: list[int] = [0] * target_count
        # Per-target FIFO of (batch_sequence, records) still awaiting the downstream's
        # forwarded-watermark acknowledgement, in sequence order.
        self._buffer: list[deque[tuple[int, tuple[Record, ...]]]] = [deque() for _ in range(target_count)]
        # Per-target highest acknowledged sequence.
        self._forwarded: list[int] = [-1] * target_count
        self._lock = threading.Lock()
        self._size_observer: Callable[[int], None] | None = None

    def enable(self, enabled: bool, sender_vertex_id: Any = None) -> None:
        """Turn on replay buffering and stamp this task's sender id."""
        self._enabled = enabled
        self._sender_vertex_id = sender_vertex_id
        self._publish_size()

    def observe_size_with(self, observer: Callable[[int], None] | None) -> None:
        self._size_observer = observer
        self._publish_size()

    @property
    def enabled(self) -> bool:
        return self._enabled

    @property
    def sender_vertex_id(self) -> Any:
        return self._sender_vertex_id

    def next_sequence_for(self, target_index: int) -> int:
        """Return the next sequence for a target without committing it."""
        with self._lock:
            return self._sequence[target_index] + 1

    def commit_landed(self, target_index: int, records: Sequence[Record], sequence: int) -> None:
        """Commit a landed batch's sequence and buffer it for possible replay.

        The buffer is bounded by backpressure (a full downstream blocks put
        upstream) and drained by the forwarded watermark — no explicit cap: a
        slow downstream must not fail the job, and a wedged watermark (a bug)
        surfaces as OOM -> Ray rebuild like any other crash.
        """
        with self._lock:
            self._sequence[target_index] = sequence
            if self._enabled:
                self._buffer[target_index].append((sequence, tuple(records)))
        self._publish_size()

    def advance_forwarded(self, target_index: int, forwarded_sequence: int) -> None:
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
            while buffer and buffer[0][0] <= forwarded_sequence:
                buffer.popleft()
                changed = True
        if changed:
            self._publish_size()

    def buffered_for(self, target_index: int) -> tuple[tuple[int, tuple[Record, ...]], ...]:
        """Return buffered ``(sequence, records)`` pairs in sequence order."""
        with self._lock:
            return tuple(self._buffer[target_index])

    @property
    def buffered_record_count(self) -> int:
        with self._lock:
            return sum(
                1 if record.num_rows is None else record.num_rows
                for target in self._buffer
                for _, records in target
                for record in records
            )

    def _publish_size(self) -> None:
        observer = self._size_observer
        if observer is not None:
            observer(self.buffered_record_count)
