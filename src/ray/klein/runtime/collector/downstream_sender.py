# SPDX-License-Identifier: Apache-2.0
"""Unified sync/async downstream delivery with immutable retry decisions."""

from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import Callable, Sequence
from contextlib import suppress
from typing import Any

from ray.exceptions import ActorUnavailableError

import ray.klein as klein
from ray.klein._internal.logging import get_logger
from ray.klein._internal.memory import estimate_retained_size
from ray.klein.observability.metrics.metrics import Counter, Histogram
from ray.klein.runtime.actor import KleinActorHandle
from ray.klein.runtime.collector.delivery_journal import DeliveryJournal
from ray.klein.runtime.message import Barrier, PutAck, Record, StreamControl

logger = get_logger(__name__)


_DELIVERY_ABORTED = PutAck(True, 0)


class DownstreamSender:
    """Deliver batches using the same target/sequence state machine in both modes."""

    def __init__(
        self,
        target_tasks: list[KleinActorHandle],
        target_operator_names: list[str],
        control_target_indices: tuple[int, ...],
        delivery_journal: DeliveryJournal,
        put_timeout: float,
        namespace: str,
        reresolve_max_wait: float = 30.0,
    ) -> None:
        if put_timeout <= 0:
            raise ValueError("input-buffer put timeout must be greater than zero")
        self._target_tasks = target_tasks
        self._target_operator_names = target_operator_names
        self._control_target_indices = control_target_indices
        self._journal = delivery_journal
        self._put_timeout = put_timeout
        self._namespace = namespace
        self._reresolve_max_wait = reresolve_max_wait
        self._backpressure_events: Counter | None = None
        self._backpressure_duration_ms: Histogram | None = None
        self._backpressure_event_count = 0
        self._backpressure_duration_ns = 0
        # EdgeOutput may drive independent target lanes concurrently. Retry rings
        # can make two lanes converge on the same actual target, so sequence
        # allocation + put + journal commit must remain single-flight per target.
        self._target_locks: list[asyncio.Lock] = [asyncio.Lock() for _ in target_tasks]
        self._target_buffer_size: list[int] = [0] * len(target_tasks)
        self._transport_requests: Counter | None = None
        self._transport_batch_rows: Histogram | None = None
        self._transport_batch_bytes: Histogram | None = None
        self._transport_send_duration_ms: Histogram | None = None
        self._transport_inflight_observer: Callable[[int], None] | None = None
        self._inflight_requests = 0
        # Force-killing a debug actor still closes it in-process; unlike a Ray
        # worker exit, retry loops therefore need an explicit cross-thread fence.
        self._delivery_aborted = threading.Event()
        self._delivery_waiters_lock = threading.Lock()
        self._delivery_waiters: set[tuple[asyncio.AbstractEventLoop, asyncio.Task[Any]]] = set()

    def abort_delivery(self) -> None:
        """Release debug retry loops and cancel any in-flight local RPC wait."""

        self._delivery_aborted.set()
        with self._delivery_waiters_lock:
            waiters = tuple(self._delivery_waiters)
        for loop, waiter in waiters:
            with suppress(RuntimeError):
                loop.call_soon_threadsafe(waiter.cancel)

    async def _await_delivery(self, request: Any) -> Any:
        """Make local debug RPC waits cancellable from the force-stop thread.

        Real Ray actors are killed at the process boundary and retain the direct
        ``klein.aget`` path. Only the in-process debug runtime registers a task
        that :meth:`abort_delivery` can cancel cross-thread.
        """

        if not klein.is_debug_mode():
            return await klein.aget(request)
        if self._delivery_aborted.is_set():
            return _DELIVERY_ABORTED

        loop = asyncio.get_running_loop()
        waiter = asyncio.create_task(klein.aget(request))
        registration = (loop, waiter)
        with self._delivery_waiters_lock:
            if self._delivery_aborted.is_set():
                cancel = True
            else:
                self._delivery_waiters.add(registration)
                cancel = False
        if cancel:
            waiter.cancel()
        try:
            return await waiter
        except asyncio.CancelledError:
            if self._delivery_aborted.is_set():
                return _DELIVERY_ABORTED
            raise
        finally:
            with self._delivery_waiters_lock:
                self._delivery_waiters.discard(registration)

    def attach_backpressure_metrics(self, events: Counter, duration_ms: Histogram) -> None:
        self._backpressure_events = events
        self._backpressure_duration_ms = duration_ms

    def attach_transport_metrics(
        self,
        requests: Counter,
        batch_rows: Histogram,
        batch_bytes: Histogram,
        send_duration_ms: Histogram,
        inflight_observer: Callable[[int], None],
    ) -> None:
        self._transport_requests = requests
        self._transport_batch_rows = batch_rows
        self._transport_batch_bytes = batch_bytes
        self._transport_send_duration_ms = send_duration_ms
        self._transport_inflight_observer = inflight_observer
        self._publish_inflight()

    @property
    def backpressure_events(self) -> int:
        return self._backpressure_event_count

    @property
    def backpressure_duration_ns(self) -> int:
        return self._backpressure_duration_ns

    @property
    def inflight_requests(self) -> int:
        return self._inflight_requests

    def _begin_request(self) -> float:
        if self._transport_requests is not None:
            self._transport_requests.inc()
        self._inflight_requests += 1
        self._publish_inflight()
        return time.monotonic()

    def _finish_request(self, started_at: float) -> None:
        if self._transport_send_duration_ms is not None:
            self._transport_send_duration_ms.observe(max(0.0, time.monotonic() - started_at) * 1_000)
        self._inflight_requests -= 1
        if self._inflight_requests < 0:
            raise AssertionError("transport in-flight accounting became negative")
        self._publish_inflight()

    def _publish_inflight(self) -> None:
        if self._transport_inflight_observer is not None:
            self._transport_inflight_observer(self._inflight_requests)

    def _observe_batch(self, records: Sequence[Record]) -> None:
        if self._transport_batch_rows is not None:
            self._transport_batch_rows.observe(
                sum(1 if record.num_rows is None else record.num_rows for record in records)
            )
        if self._transport_batch_bytes is not None:
            self._transport_batch_bytes.observe(estimate_retained_size(tuple(records)))

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

    def _put_request(self, target_index: int, records: Any, sequence: int) -> Any:
        kwargs = self._delivery_kwargs(target_index, sequence)
        return self._target_tasks[target_index].put(records, timeout=self._put_timeout, **kwargs)

    def _delivery_kwargs(self, target_index: int, sequence: int) -> dict[str, Any]:
        kwargs: dict[str, Any] = {
            "sender_vertex_id": self._journal.sender_vertex_id,
            "batch_sequence": sequence,
        }
        channel = self._journal.delivery_channel(target_index)
        if channel is not None:
            kwargs["delivery_channel"] = channel
        return kwargs

    def _admission_request(self, target_index: int, records: Any, sequence: int) -> Any:
        """Use immediate capacity admission when the target supports it."""
        target = self._target_tasks[target_index]
        try_put = getattr(target, "try_put", None)
        if callable(try_put):
            return try_put(records, **self._delivery_kwargs(target_index, sequence))
        # Compatibility for custom/test actor facades from before try_put.
        return self._put_request(target_index, records, sequence)

    def _on_success(
        self,
        target_index: int,
        records: Sequence[Record],
        sequence: int,
        acknowledgement: PutAck,
        is_replay: bool,
    ) -> None:
        if acknowledgement is _DELIVERY_ABORTED:
            return
        self._journal.acknowledge(target_index, acknowledgement.forwarded_sequence)
        self._target_buffer_size[target_index] = acknowledgement.buffer_size
        self._observe_batch(records)
        if not is_replay:
            self._journal.record_delivery(target_index, records, sequence)

    @staticmethod
    def _validate_retry_ring(initial_target: int, retry_targets: tuple[int, ...]) -> tuple[int, ...]:
        if not retry_targets or retry_targets[0] != initial_target:
            raise ValueError(f"retry ring must begin with target {initial_target}: {retry_targets}")
        return retry_targets

    def _order_retry_ring(self, ring: tuple[int, ...]) -> tuple[int, ...]:
        """Keep affinity first, then prefer targets reporting less queued work."""
        if len(ring) < 3:
            return ring
        return (ring[0], *sorted(ring[1:], key=self._target_buffer_size.__getitem__))

    def send_sync(
        self,
        target_index: int,
        records: Sequence[Record],
        retry_targets: tuple[int, ...] | None = None,
        *,
        is_replay: bool = False,
        replay_sequence: int | None = None,
        wire_records: Any = None,
    ) -> None:
        ring = self._order_retry_ring(self._validate_retry_ring(target_index, retry_targets or (target_index,)))
        if is_replay:
            ring = (target_index,)
        ring_index = 0
        backoff = 0.0
        unavailable_waited = 0.0
        started_at = time.monotonic()
        encountered_backpressure = False
        while not self._delivery_aborted.is_set():
            actual_target = ring[ring_index]
            sequence = replay_sequence if is_replay else self._journal.next_sequence(actual_target)
            if sequence is None:
                raise ValueError("replay sends require their original sequence number")
            try:
                request_started_at = self._begin_request()
                try:
                    acknowledgement = klein.get(
                        self._admission_request(
                            actual_target,
                            records if wire_records is None else wire_records,
                            sequence,
                        )
                    )
                finally:
                    self._finish_request(request_started_at)
            except ActorUnavailableError:
                self.refresh_target(actual_target)
                backoff = min(1.0, backoff * 2 + 0.05)
                unavailable_waited += backoff
                if unavailable_waited > self._reresolve_max_wait:
                    self._observe_backpressure(started_at, encountered_backpressure)
                    raise
                self._delivery_aborted.wait(backoff)
                continue
            if acknowledgement.accepted:
                self._on_success(actual_target, records, sequence, acknowledgement, is_replay)
                self._observe_backpressure(started_at, encountered_backpressure)
                return
            if not encountered_backpressure:
                started_at = time.monotonic()
            encountered_backpressure = True
            self._record_backpressure()
            self._target_buffer_size[actual_target] = acknowledgement.buffer_size
            ring_index = (ring_index + 1) % len(ring)
            if ring_index == 0:
                backoff = min(0.1, backoff * 2 + 0.001)
                self._delivery_aborted.wait(backoff)

    async def send_async(
        self,
        target_index: int,
        records: Sequence[Record],
        retry_targets: tuple[int, ...] | None = None,
        *,
        is_replay: bool = False,
        replay_sequence: int | None = None,
        wire_records: Any = None,
    ) -> None:
        ring = self._order_retry_ring(self._validate_retry_ring(target_index, retry_targets or (target_index,)))
        if is_replay:
            ring = (target_index,)
        ring_index = 0
        backoff = 0.0
        unavailable_waited = 0.0
        started_at = time.monotonic()
        encountered_backpressure = False
        while not self._delivery_aborted.is_set():
            actual_target = ring[ring_index]
            try:
                async with self._target_locks[actual_target]:
                    sequence = replay_sequence if is_replay else self._journal.next_sequence(actual_target)
                    if sequence is None:
                        raise ValueError("replay sends require their original sequence number")
                    request_started_at = self._begin_request()
                    try:
                        acknowledgement = await self._await_delivery(
                            self._admission_request(
                                actual_target,
                                records if wire_records is None else wire_records,
                                sequence,
                            )
                        )
                    finally:
                        self._finish_request(request_started_at)
                    if acknowledgement.accepted:
                        self._on_success(actual_target, records, sequence, acknowledgement, is_replay)
            except ActorUnavailableError:
                self.refresh_target(actual_target)
                backoff = min(1.0, backoff * 2 + 0.05)
                unavailable_waited += backoff
                if unavailable_waited > self._reresolve_max_wait:
                    self._observe_backpressure(started_at, encountered_backpressure)
                    raise
                await asyncio.sleep(backoff)
                continue
            if acknowledgement.accepted:
                self._observe_backpressure(started_at, encountered_backpressure)
                return
            if not encountered_backpressure:
                started_at = time.monotonic()
            encountered_backpressure = True
            self._record_backpressure()
            self._target_buffer_size[actual_target] = acknowledgement.buffer_size
            ring_index = (ring_index + 1) % len(ring)
            if ring_index == 0:
                backoff = min(0.1, backoff * 2 + 0.001)
                await asyncio.sleep(backoff)

    def _barrier_requests(self, barrier: Barrier) -> list:
        return [
            self._target_tasks[index].emit_barrier(
                barrier,
                sender_vertex_id=self._journal.sender_vertex_id,
                delivery_channel=self._journal.delivery_channel(index),
            )
            for index in self._control_target_indices
        ]

    def send_barrier_sync(self, barrier: Barrier) -> None:
        if self._delivery_aborted.is_set():
            return
        klein.get(self._barrier_requests(barrier))

    async def send_barrier_async(self, barrier: Barrier) -> None:
        if self._delivery_aborted.is_set():
            return
        await self._await_delivery(self._barrier_requests(barrier))

    def _control_requests(self, control: StreamControl) -> list:
        return [
            self._target_tasks[index].emit_stream_control(
                control,
                sender_vertex_id=self._journal.sender_vertex_id,
                delivery_channel=self._journal.delivery_channel(index),
            )
            for index in self._control_target_indices
        ]

    def send_control_sync(self, control: StreamControl) -> None:
        if self._delivery_aborted.is_set():
            return
        klein.get(self._control_requests(control))

    async def send_control_async(self, control: StreamControl) -> None:
        if self._delivery_aborted.is_set():
            return
        await self._await_delivery(self._control_requests(control))

    def refresh_target(self, target_index: int) -> None:
        name = self._target_operator_names[target_index]
        handle = klein.get_actor_by_name(name, namespace=self._namespace)
        if handle is not None:
            self._target_tasks[target_index] = handle

    def refresh_by_name(self, downstream_name: str) -> None:
        try:
            target_index = self._target_operator_names.index(downstream_name)
        except ValueError:
            return
        self.refresh_target(target_index)
