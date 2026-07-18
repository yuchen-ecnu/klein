# SPDX-License-Identifier: Apache-2.0
"""The OutputCollector send layer and its downstream retry policies.

The two entry points share sequence assignment, put handling, replay-buffer
commit, and forwarded-watermark advancement:

* :meth:`send_sync` — inline (source / teardown) emit on the executor thread.
  A full inbox is handled by the partitioner's ``on_record_emit_timeout``
  reselect; blocks via ``klein.get``.
* :meth:`send_async` — pipelined emit on the actor loop. Adds Ray
  ActorUnavailable re-resolve-and-retry and backpressure-driven reroute
  (worker-pool) / bounded backoff; awaits via ``klein.aget``.

Barrier broadcast (sync + async) lives here too, since it is also a send.
"""

import asyncio
import time
from collections import deque
from collections.abc import Sequence
from typing import Any

from ray.exceptions import ActorUnavailableError

import ray.klein as klein
from ray.klein._internal.logging import get_logger
from ray.klein.observability.metrics.metrics import Counter, Histogram
from ray.klein.runtime.actor import KleinActorHandle
from ray.klein.runtime.collector.replay_buffer import ReplayBuffer
from ray.klein.runtime.collector.router import Router
from ray.klein.runtime.message import Barrier, PutAck, Record, StreamControl

logger = get_logger(__name__)


class EmitEngine:
    """Sends batches to downstream tasks, with retry/reroute and replay commit."""

    def __init__(
        self,
        target_tasks: list[KleinActorHandle],
        target_operator_names: list[str],
        router: Router,
        replay_buffer: ReplayBuffer,
        put_timeout: int,
        reresolve_max_wait: float = 30.0,
    ) -> None:
        # target_tasks is mutated in place by reresolve_target (the collector and
        # this engine share the same list object).
        self._target_tasks = target_tasks
        self._target_operator_names = target_operator_names
        self._router = router
        self._replay = replay_buffer
        self._put_timeout = put_timeout
        self._reresolve_max_wait = reresolve_max_wait
        # Congestion feedback for the worker-pool ring in pipelined mode. The
        # loop-side send_async (a different thread from collect()) appends the
        # timed-out record here; the collector drains it on the executor thread
        # and advances the partitioner's dispatcher. This keeps the invariant
        # that ONLY the executor thread mutates the partitioner — no lock needed.
        # deque.append/popleft are individually atomic under CPython, so the
        # cross-thread handoff is safe without a lock.
        self._emit_timeouts: deque[Record] = deque()
        self._backpressure_events: Counter | None = None
        self._backpressure_duration_ms: Histogram | None = None
        self._backpressure_event_count = 0
        self._backpressure_duration_ns = 0

    def attach_backpressure_metrics(self, events: Counter, duration_ms: Histogram) -> None:
        self._backpressure_events = events
        self._backpressure_duration_ms = duration_ms

    def _record_backpressure(self) -> None:
        self._backpressure_event_count += 1
        if self._backpressure_events is not None:
            self._backpressure_events.inc()

    def _observe_backpressure(self, started_at: float, encountered: bool) -> None:
        if not encountered:
            return
        elapsed_seconds = max(0.0, time.monotonic() - started_at)
        self._backpressure_duration_ns += int(elapsed_seconds * 1_000_000_000)
        if self._backpressure_duration_ms is not None:
            self._backpressure_duration_ms.observe(elapsed_seconds * 1_000)

    @property
    def backpressure_events(self) -> int:
        return self._backpressure_event_count

    @property
    def backpressure_duration_ns(self) -> int:
        return self._backpressure_duration_ns

    # --- shared per-attempt helpers ---

    def _put_ref(
        self,
        target_index: int,
        records: Sequence[Record],
        sequence: int,
    ) -> Any:
        return self._target_tasks[target_index].put(
            records,
            timeout=self._put_timeout,
            sender_vertex_id=self._replay.sender_vertex_id,
            batch_sequence=sequence,
        )

    def _on_success(
        self,
        target_index: int,
        records: Sequence[Record],
        sequence: int,
        acknowledgement: PutAck,
        is_replay: bool,
    ) -> None:
        if not is_replay:
            self._replay.commit_landed(target_index, records, sequence)
        # Drop replay-buffer entries the downstream has now confirmed forwarding
        # onward (records exist on a second node).
        self._replay.advance_forwarded(target_index, acknowledgement.forwarded_sequence)

    # --- inline (executor thread) send ---

    def send_sync(self, target_index: int, records: Sequence[Record], is_replay: bool = False) -> None:
        success, buffer_size = False, 0
        sequence = self._replay.next_sequence_for(target_index)
        backoff = 0.0
        unavailable_waited = 0.0
        started_at = time.monotonic()
        encountered_backpressure = False
        acknowledgement: PutAck | None = None
        while success is False:
            try:
                acknowledgement = klein.get(self._put_ref(target_index, records, sequence))
            except ActorUnavailableError:
                # Ray is rebuilding the downstream (single-task recovery in
                # flight). This is the inline (source) path: mirror send_async's
                # re-resolve + bounded backoff so a source survives a downstream
                # single-point restart instead of failing and forcing a global
                # restart. Blocking sleep is fine — we're on the executor thread.
                self.reresolve_target(target_index)
                backoff = min(1.0, backoff * 2 + 0.05)
                unavailable_waited += backoff
                if unavailable_waited > self._reresolve_max_wait:
                    self._observe_backpressure(started_at, encountered_backpressure)
                    raise
                time.sleep(backoff)
                continue
            success, buffer_size = acknowledgement.accepted, acknowledgement.buffer_size
            if not success:
                if not encountered_backpressure:
                    started_at = time.monotonic()
                encountered_backpressure = True
                self._record_backpressure()
                target_index = self._router.on_record_emit_timeout(records[0], target_index, buffer_size)
                # A reselected target needs its own next sequence.
                sequence = self._replay.next_sequence_for(target_index)
                logger.debug(
                    "reselected target actors: %s",
                    self._target_tasks[target_index],
                )
        if acknowledgement is None:
            raise AssertionError("successful send must have a downstream acknowledgement")
        self._on_success(target_index, records, sequence, acknowledgement, is_replay)
        self._observe_backpressure(started_at, encountered_backpressure)
        self._router.on_record_emitted(target_index, buffer_size)

    # --- pipelined (actor loop) send ---

    async def send_async(
        self,
        target_index: int,
        records: Sequence[Record],
        is_replay: bool = False,
        replay_sequence: int | None = None,
    ) -> None:
        # Three distinct outcomes of a put, each handled differently:
        #  * accepted        -> landed; commit replay buffer, drop acked entries.
        #  * NOT accepted     -> downstream inbox FULL (backpressure): reroute
        #                        (worker-pool) or backoff-retry same target.
        #  * ActorUnavailable -> downstream is being rebuilt by Ray: re-resolve
        #                        the handle by name and backoff-retry, bounded.
        #  * ActorDied        -> permanently gone: raise -> task FAILED -> full
        #                        restart (rollback to last checkpoint).
        can_reroute = self._router.can_reroute
        target_count = len(self._target_tasks)
        backoff = 0.0
        unavailable_waited = 0.0
        started_at = time.monotonic()
        encountered_backpressure = False
        while True:
            sequence = replay_sequence if is_replay else self._replay.next_sequence_for(target_index)
            if sequence is None:
                raise ValueError("Replay sends require their original sequence number")
            try:
                acknowledgement = await klein.aget(self._put_ref(target_index, records, sequence))
            except ActorUnavailableError:
                # Ray is rebuilding the downstream (single-task recovery in
                # flight). Re-resolve by name and retry; only give up after a
                # bounded wait so a genuinely stuck rebuild still escalates.
                self.reresolve_target(target_index)
                backoff = min(1.0, backoff * 2 + 0.05)
                unavailable_waited += backoff
                if unavailable_waited > self._reresolve_max_wait:
                    self._observe_backpressure(started_at, encountered_backpressure)
                    raise
                await asyncio.sleep(backoff)
                continue
            # ActorDiedError (and any other error) intentionally propagates:
            # the downstream is permanently gone, so fail the task.

            if acknowledgement.accepted:
                self._on_success(target_index, records, sequence, acknowledgement, is_replay)
                self._observe_backpressure(started_at, encountered_backpressure)
                return
            # Inbox full (backpressure). Hand congestion to the executor thread.
            self._emit_timeouts.append(records[0])
            if not encountered_backpressure:
                started_at = time.monotonic()
            encountered_backpressure = True
            self._record_backpressure()
            if can_reroute:
                # Probe the next downstream for THIS batch immediately.
                target_index = (target_index + 1) % target_count
            else:
                # Fixed target (key/forward/broadcast): wait for it to drain.
                # Bounded exponential backoff so a sustained-full inbox doesn't
                # spin the loop.
                backoff = min(0.1, backoff * 2 + 0.001)
                await asyncio.sleep(backoff)

    def drain_emit_timeouts(self) -> None:
        """Executor-thread: replay loop-side congestion onto the partitioner.

        Each entry is a record whose put timed out on the loop; replaying it
        through ``on_record_emit_timeout`` advances the worker-pool dispatcher so
        the next ``partition()`` picks a different (hopefully free) downstream.
        """
        while self._emit_timeouts:
            record = self._emit_timeouts.popleft()
            self._router.on_record_emit_timeout(record, -1, 0)

    # --- barrier broadcast (target selection identical; only the I/O verb differs) ---

    def _barrier_refs(self, barrier: Barrier, source_parallelism: int, source_index: int) -> list:
        return [
            self._target_tasks[target_index].emit_barrier(barrier)
            for target_index in self._router.barrier_target_indices(source_parallelism, source_index)
        ]

    def broadcast_sync(self, barrier: Barrier, source_parallelism: int, source_index: int) -> None:
        self._router.on_barrier_emitted(klein.get(self._barrier_refs(barrier, source_parallelism, source_index)))

    async def broadcast_async(self, barrier: Barrier, source_parallelism: int, source_index: int) -> None:
        self._router.on_barrier_emitted(await klein.aget(self._barrier_refs(barrier, source_parallelism, source_index)))

    def _control_refs(self, control: StreamControl, source_parallelism: int, source_index: int) -> list:
        return [
            self._target_tasks[target_index].emit_stream_control(
                control,
                sender_vertex_id=self._replay.sender_vertex_id,
            )
            for target_index in self._router.barrier_target_indices(source_parallelism, source_index)
        ]

    def broadcast_control_sync(
        self,
        control: StreamControl,
        source_parallelism: int,
        source_index: int,
    ) -> None:
        klein.get(self._control_refs(control, source_parallelism, source_index))

    async def broadcast_control_async(
        self,
        control: StreamControl,
        source_parallelism: int,
        source_index: int,
    ) -> None:
        await klein.aget(self._control_refs(control, source_parallelism, source_index))

    # --- handle re-resolution after a downstream rebuild ---

    def reresolve_target(self, target_index: int) -> None:
        """Re-resolve a (possibly rebuilt) downstream handle by name.

        Ray named-actor routing usually makes this transparent, but after a
        rebuild the cached handle can be stale; re-looking-up by name picks up
        the live actor. No-op if the name can't be resolved yet (still rebuilding).
        """
        name = self._target_operator_names[target_index]
        handle = klein.get_actor_by_name(name)
        if handle is not None:
            self._target_tasks[target_index] = handle

    def reresolve_by_name(self, downstream_name: str) -> None:
        """Re-resolve a downstream handle given its actor name (no-op if absent)."""
        try:
            target_index = self._target_operator_names.index(downstream_name)
        except ValueError:
            return
        self.reresolve_target(target_index)
