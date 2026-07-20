# SPDX-License-Identifier: Apache-2.0
"""The inbox pump for a StreamTask: consume the inbox, drive the operator.

One asyncio task (the AsyncWorker loop) calls :meth:`run_once` repeatedly.
Each iteration pulls one :class:`InboxEnvelope` from the inbox and:

* runs the operator on it — sync UDFs on the single executor thread (operators
  aren't thread-safe / may block), async UDFs awaited on the loop;
* hands the resulting buffered emit-ops to the :class:`EmitPipeline`;
* advances the replay watermark at its configured batch interval;
* checks end-of-stream (a bounded sink like ``take(n)`` requests a graceful
  drain, then the EndOfData barrier drives the job to FINISHED).

An idle inbox (no envelope within the timeout) still flushes the input batcher
and forces a watermark flush, so a low-rate stream doesn't strand buffered
records or pin the upstream's replay buffer.

Barriers are data-plane control: a barrier flows through the batcher (which
flushes accumulated data first), is aligned by the snapshot strategy, and on the
final EndOfData drives the FINISHED transition. All of this runs on the executor
thread for the sync path so the coordinator ``klein.get`` stays off the loop.
"""

import asyncio
from collections import Counter, deque
from collections.abc import Sequence
from contextlib import suppress
from dataclasses import dataclass
from typing import TYPE_CHECKING

from ray.klein._internal.memory import estimate_retained_size
from ray.klein.runtime.execution_graph.execution_vertex_id import ExecutionVertexId
from ray.klein.runtime.message import (
    Barrier,
    DeliveryChannel,
    EndOfData,
    InputActive,
    InputIdle,
    Record,
    RescaleBarrier,
    StreamControl,
    Watermark,
)

if TYPE_CHECKING:
    from ray.klein.runtime.worker.emit_pipeline import EmitPipeline
    from ray.klein.runtime.worker.stream_task import StreamTask, _RuntimeState
    from ray.klein.runtime.worker.watermark import WatermarkController


@dataclass(frozen=True, slots=True)
class InboxEnvelope:
    """One ordered inbox item and its replay identity."""

    payload: Record | Sequence[Record] | Barrier | StreamControl
    sender_vertex_id: object | None = None
    sequence: int | None = None
    delivery_channel: DeliveryChannel | None = None

    def __post_init__(self) -> None:
        if isinstance(self.payload, Sequence):
            object.__setattr__(self, "payload", tuple(self.payload))


def inbox_envelope_rows(envelope: InboxEnvelope) -> int:
    """Logical rows retained by one inbox envelope; controls weighted capacity."""
    payload = envelope.payload
    if isinstance(payload, Barrier | StreamControl):
        return 1
    records = payload if isinstance(payload, Sequence) else (payload,)
    return max(1, sum(1 if record.num_rows is None else record.num_rows for record in records))


def inbox_envelope_bytes(envelope: InboxEnvelope) -> int:
    """Estimated bytes retained by one inbox envelope."""
    return estimate_retained_size(envelope.payload)


class InboxPump:
    """Drives one operator from its inbox; owns the per-iteration pump logic."""

    def __init__(
        self,
        task: "StreamTask",
        state: "_RuntimeState",
        watermark: "WatermarkController",
        emit_pipeline: "EmitPipeline",
        inbox_timeout: float = 3.0,
        input_vertex_ids: tuple[ExecutionVertexId, ...] = (),
    ) -> None:
        self._task = task
        self._state = state
        self._watermark = watermark
        self._emit = emit_pipeline
        self._inbox_timeout = inbox_timeout
        self._checkpoint_expected_inputs = Counter(input_vertex_ids)
        self._checkpoint_barrier_id: int | None = None
        self._checkpoint_seen_inputs: Counter[ExecutionVertexId] = Counter()
        self._checkpoint_seen_channels: set[object] = set()
        # Admission is fenced as soon as the barrier RPC reaches this actor,
        # before the barrier itself waits for shared-inbox capacity.  Keep that
        # separate from the processing gate below: records already in the inbox
        # are ordered before the barrier and must remain consumable.
        self._checkpoint_announced_channels: set[object] = set()
        self._checkpoint_admission_resumes: dict[object, asyncio.Event] = {}
        self._checkpoint_input_resumes: dict[object, asyncio.Event] = {}
        self._checkpoint_held: deque[InboxEnvelope] = deque()
        self._checkpoint_ready: deque[InboxEnvelope] = deque()
        self._last_coordinated_barrier_id = -1

    async def run_once(self) -> None:
        """One pump iteration (the body of the AsyncWorker loop)."""
        loop = asyncio.get_running_loop()
        try:
            envelope = await asyncio.wait_for(
                self._next_envelope(),
                timeout=self._inbox_timeout,
            )
        except asyncio.TimeoutError:
            await loop.run_in_executor(self._state.executor, self._idle_flush)
            await self._emit.drain_pending()
            # Idle: force a watermark flush so a low-rate stream still releases
            # the upstream's replay buffer instead of pinning it indefinitely.
            await self._watermark.advance()
            return

        payload = envelope.payload
        if self._hold_for_coordinated_checkpoint(envelope):
            self._checkpoint_held.append(envelope)
            return
        checkpoint_aligned = self._observe_coordinated_barrier(envelope)
        if isinstance(payload, RescaleBarrier):
            await self._task.handle_rescale_barrier(payload, envelope.sender_vertex_id)
            return
        if self._state.is_async_operator:
            # Async path: feed the concurrency window and return immediately so
            # the NEXT envelope can start its requests while these are still in
            # flight — that overlap is the whole point. Emit + watermark run in
            # the runner's FIFO consumer (in order). The eof check + stop must
            # run on the pump loop (stop() awaits the consumer, so running it
            # inside would deadlock), so only on EndOfData do we drain the runner
            # via barrier() and then finalize here. EndOfData is terminal, so the
            # one-time stall there costs nothing.
            await self._handle_async(
                payload,
                envelope.sender_vertex_id,
                envelope.sequence,
                envelope.delivery_channel,
            )
            if isinstance(payload, EndOfData):
                await self._state.async_runner.barrier()
                if checkpoint_aligned:
                    self._finish_coordinated_checkpoint(payload.id)
                await self._check_eof_and_stop()
            elif checkpoint_aligned:
                # The final barrier is an ordered runner control. Wait until its
                # snapshot/forward action and emit drain have completed before
                # releasing post-barrier records from earlier inputs.
                await self._state.async_runner.barrier()
                self._finish_coordinated_checkpoint(payload.id)
            return
        await loop.run_in_executor(
            self._state.executor,
            self._handle_sync,
            payload,
            envelope.sender_vertex_id,
            envelope.delivery_channel,
        )
        await self._emit_drain_and_watermark(
            payload,
            envelope.sender_vertex_id,
            envelope.sequence,
            envelope.delivery_channel,
        )
        if checkpoint_aligned:
            self._finish_coordinated_checkpoint(payload.id)
        await self._check_eof_and_stop()

    async def _next_envelope(self) -> InboxEnvelope:
        if self._checkpoint_ready:
            return self._checkpoint_ready.popleft()
        get_matching = getattr(self._state.inbox, "get_matching", None)
        if callable(get_matching):
            return await get_matching(lambda envelope: not self._hold_for_coordinated_checkpoint(envelope))
        return await self._state.inbox.get()

    def _hold_for_coordinated_checkpoint(self, envelope: InboxEnvelope) -> bool:
        channel = envelope.delivery_channel or envelope.sender_vertex_id
        if channel not in self._checkpoint_input_resumes:
            return False
        payload = envelope.payload
        return not (isinstance(payload, Barrier) and payload.coordinated and payload.id == self._checkpoint_barrier_id)

    def _observe_coordinated_barrier(self, envelope: InboxEnvelope) -> bool:
        payload = envelope.payload
        if not isinstance(payload, Barrier) or not payload.coordinated:
            return False
        if payload.id <= self._last_coordinated_barrier_id:
            return False
        sender = envelope.sender_vertex_id
        if not isinstance(sender, ExecutionVertexId):
            raise ValueError("a coordinated checkpoint barrier requires a physical sender")
        if self._checkpoint_expected_inputs and sender not in self._checkpoint_expected_inputs:
            raise ValueError(f"unexpected coordinated checkpoint sender {sender}")
        if self._checkpoint_barrier_id is None:
            self._checkpoint_barrier_id = payload.id
        elif self._checkpoint_barrier_id != payload.id:
            raise RuntimeError(
                f"coordinated checkpoint {payload.id} arrived while {self._checkpoint_barrier_id} is aligning"
            )
        channel = envelope.delivery_channel or sender
        if channel in self._checkpoint_seen_channels:
            return False
        self._checkpoint_seen_channels.add(channel)
        self._checkpoint_seen_inputs[sender] += 1
        self._block_checkpoint_input(channel)
        expected = self._checkpoint_expected_inputs or Counter({sender: 1})
        return all(self._checkpoint_seen_inputs[input_id] >= count for input_id, count in expected.items())

    def announce_coordinated_barrier(
        self,
        barrier: Barrier,
        sender: ExecutionVertexId | None,
        delivery_channel: DeliveryChannel | None = None,
    ) -> bool:
        """Fence later admissions before a coordinated barrier queues.

        ``emit_barrier`` and later data RPCs from one output lane are ordered,
        but the shared weighted inbox can already be full of pre-barrier data.
        Recording the cut here prevents post-barrier data from racing into the
        freed slot while still allowing those older queued records to drain.
        The method returns ``False`` for a stale/retried channel barrier so a
        retry cannot consume another bounded inbox slot.
        """

        if not barrier.coordinated:
            return True
        if barrier.id <= self._last_coordinated_barrier_id:
            return False
        if not isinstance(sender, ExecutionVertexId):
            raise ValueError("a coordinated checkpoint barrier requires a physical sender")
        if self._checkpoint_expected_inputs and sender not in self._checkpoint_expected_inputs:
            raise ValueError(f"unexpected coordinated checkpoint sender {sender}")
        if self._checkpoint_barrier_id is None:
            self._checkpoint_barrier_id = barrier.id
        elif self._checkpoint_barrier_id != barrier.id:
            raise RuntimeError(
                f"coordinated checkpoint {barrier.id} arrived while {self._checkpoint_barrier_id} is aligning"
            )
        channel = self._input_channel(sender, delivery_channel)
        if channel in self._checkpoint_announced_channels:
            return False
        self._checkpoint_announced_channels.add(channel)
        self._checkpoint_admission_resumes[channel] = asyncio.Event()
        return True

    def _block_checkpoint_input(self, channel: object) -> None:
        if channel not in self._checkpoint_input_resumes:
            self._checkpoint_input_resumes[channel] = self._checkpoint_admission_resumes.get(
                channel,
                asyncio.Event(),
            )

    @staticmethod
    def _input_channel(sender: object, delivery_channel: DeliveryChannel | None) -> object:
        return delivery_channel or sender

    def checkpoint_input_blocked(
        self,
        sender: object,
        delivery_channel: DeliveryChannel | None = None,
    ) -> bool:
        channel = self._input_channel(sender, delivery_channel)
        return channel in self._checkpoint_admission_resumes or channel in self._checkpoint_input_resumes

    async def wait_for_checkpoint_input(
        self,
        sender: object,
        payload: object | None = None,
        delivery_channel: DeliveryChannel | None = None,
    ) -> None:
        """Backpressure one post-barrier channel while its peers catch up."""

        channel = self._input_channel(sender, delivery_channel)
        resume = self._checkpoint_admission_resumes.get(channel) or self._checkpoint_input_resumes.get(channel)
        if resume is None:
            return
        if isinstance(payload, Barrier) and payload.coordinated and payload.id == self._checkpoint_barrier_id:
            return
        await resume.wait()

    def _finish_coordinated_checkpoint(self, barrier_id: int) -> None:
        if barrier_id != self._checkpoint_barrier_id:
            return
        self._last_coordinated_barrier_id = max(self._last_coordinated_barrier_id, barrier_id)
        self._release_checkpoint_inputs()

    def discard_checkpoint(self, barrier_id: int) -> int:
        self._last_coordinated_barrier_id = max(self._last_coordinated_barrier_id, barrier_id)
        held = 0
        if barrier_id == self._checkpoint_barrier_id:
            held = len(self._checkpoint_held)
            self._release_checkpoint_inputs()
        return held

    def reset_inflight_before(self, cutoff_barrier_id: int) -> int:
        self._last_coordinated_barrier_id = max(
            self._last_coordinated_barrier_id,
            cutoff_barrier_id,
        )
        if self._checkpoint_barrier_id is None or self._checkpoint_barrier_id > cutoff_barrier_id:
            return 0
        held = len(self._checkpoint_held)
        self._release_checkpoint_inputs()
        return held

    def release_all_checkpoint_inputs(self) -> None:
        self._release_checkpoint_inputs()

    def validate_checkpoint_reconfiguration(self) -> None:
        """Reject an input-topology swap while a shared epoch is aligning."""

        if (
            self._checkpoint_barrier_id is not None
            or self._checkpoint_announced_channels
            or self._checkpoint_input_resumes
        ):
            raise RuntimeError("cannot reconfigure checkpoint inputs with a barrier in flight")

    def reconfigure_checkpoint_inputs(
        self,
        input_vertex_ids: tuple[ExecutionVertexId, ...],
    ) -> None:
        """Install the physical input multiplicity for the committed topology."""

        self.validate_checkpoint_reconfiguration()
        self._checkpoint_expected_inputs = Counter(input_vertex_ids)

    def _release_checkpoint_inputs(self) -> None:
        for resume in (*self._checkpoint_admission_resumes.values(), *self._checkpoint_input_resumes.values()):
            resume.set()
        self._checkpoint_announced_channels.clear()
        self._checkpoint_admission_resumes.clear()
        self._checkpoint_input_resumes.clear()
        self._checkpoint_ready.extend(self._checkpoint_held)
        self._checkpoint_held.clear()
        self._checkpoint_seen_inputs.clear()
        self._checkpoint_seen_channels.clear()
        self._checkpoint_barrier_id = None
        wake_waiters = getattr(self._state.inbox, "wake_waiters", None)
        if callable(wake_waiters):
            # Actor teardown can release a gate after its loop stopped.
            with suppress(RuntimeError):
                asyncio.get_running_loop().create_task(wake_waiters())

    async def _emit_drain_and_watermark(
        self,
        payload,
        sender_vertex_id: object | None,
        sequence: int | None,
        delivery_channel: DeliveryChannel | None,
    ) -> None:
        """Drain buffered emit-ops and advance the replay watermark after process.

        Shared by the sync path (inline) and the async path (run from the ordered
        runner's consumer once a record's output has been emitted), so the replay
        watermark only advances after the output has physically left.
        """
        # Hand this iteration's buffered emit-ops to the emit-worker. Enqueue
        # (bounded -> backpressure) overlaps with the NEXT iteration's process.
        await self._emit.drain_pending()
        # Replay watermark: remember the last data batch and, every
        # configured number of batches, force a flush + emit a watermark marker so the
        # upstream learns this batch's output has left the task.
        if not isinstance(payload, Barrier | StreamControl) and self._watermark.note_processed(
            delivery_channel or sender_vertex_id, sequence
        ):
            await self._watermark.advance()

    async def _check_eof_and_stop(self) -> None:
        """End-of-stream check + stop. Always runs on the pump loop task."""
        loop = asyncio.get_running_loop()
        # Operators like a bounded sink (take(n)) signal end-of-stream after
        # processing; check on the executor thread (it calls job_manager.drain).
        if not self._task.eof_reached:
            await loop.run_in_executor(self._state.executor, self._task._check_end_of_stream)
        if self._task.eof_reached:
            await self._task.stop()

    # --- operator dispatch ---

    def _handle_sync(
        self,
        payload,
        sender_vertex_id: object | None,
        delivery_channel: DeliveryChannel | None = None,
    ) -> None:
        """Runs in the executor thread — accumulator + operator + output commands."""
        for record in payload if isinstance(payload, Sequence) else (payload,):
            if isinstance(record, StreamControl):
                self.flush_input()
                self.handle_stream_control(record, sender_vertex_id)
                continue
            if not isinstance(record, StreamControl):
                record.sender = sender_vertex_id
                record.delivery_channel = delivery_channel
            for emitted in self._state.input_batches.accept(record):
                self.data_handler(emitted)
        if isinstance(payload, EndOfData):
            self.flush_input()

    async def _handle_async(
        self,
        payload,
        sender_vertex_id: object | None,
        sequence: int | None,
        delivery_channel: DeliveryChannel | None,
    ) -> None:
        # Async path producer: input flows through the batcher first (same as the
        # sync path) so a batched async operator — e.g. the serve
        # EmbeddedProxyClient — receives batch_size-shaped blocks. Each flushed
        # data record is submitted to the ordered runner as a *concurrent*
        # compute; barriers and the per-envelope post-process bookkeeping are
        # submitted as in-order control actions. The runner's FIFO consumer then
        # emits results in submit order (ORDERED), so up to async_buffer_size
        # requests are in flight at once without racing TaskOutput.
        loop = asyncio.get_running_loop()
        runner = self._state.async_runner
        for record in payload if isinstance(payload, Sequence) else (payload,):
            if isinstance(record, StreamControl):
                emitted = await loop.run_in_executor(
                    self._state.executor,
                    lambda: self._state.input_batches.flush(force=True),
                )
                await self._dispatch_async(emitted)
                await runner.submit_control(
                    lambda control=record, sender=sender_vertex_id: loop.run_in_executor(
                        self._state.executor,
                        self.handle_stream_control,
                        control,
                        sender,
                    )
                )
                continue
            if not isinstance(record, StreamControl):
                record.sender = sender_vertex_id
                record.delivery_channel = delivery_channel
            emitted = await loop.run_in_executor(
                self._state.executor,
                self._state.input_batches.accept,
                record,
            )
            await self._dispatch_async(emitted)
        if isinstance(payload, EndOfData):
            emitted = await loop.run_in_executor(
                self._state.executor,
                lambda: self._state.input_batches.flush(force=True),
            )
            await self._dispatch_async(emitted)
        # Per-envelope emit-drain + watermark runs in order behind this
        # envelope's data, once its output has actually been emitted. The eof
        # check + stop is NOT submitted here — it runs on the pump loop after
        # runner.barrier() (see run_once) to avoid a stop()/consumer deadlock.
        await runner.submit_control(
            lambda: self._emit_drain_and_watermark(
                payload,
                sender_vertex_id,
                sequence,
                delivery_channel,
            )
        )

    async def _dispatch_async(self, records) -> None:
        """Route batcher-flushed records to the ordered runner.

        Data records become concurrent computes; barriers become in-order
        control actions (alignment/snapshot run on the executor to keep the
        coordinator ``klein.get`` off the loop). FIFO ordering in the runner
        guarantees a barrier observes all preceding data already emitted.
        """
        loop = asyncio.get_running_loop()
        runner = self._state.async_runner
        for record in records:
            if isinstance(record, Barrier):
                await runner.submit_control(
                    lambda barrier=record: loop.run_in_executor(
                        self._state.executor,
                        self.handle_barrier,
                        barrier,
                        barrier.sender,
                        getattr(barrier, "delivery_channel", None),
                    )
                )
            elif isinstance(record, StreamControl):
                await runner.submit_control(
                    lambda control=record: loop.run_in_executor(
                        self._state.executor,
                        self.handle_stream_control,
                        control,
                        None,
                    )
                )
            else:
                await runner.submit_compute(self._state.runner.process_async(record))

    async def on_async_result(self, records) -> None:
        """Emit a finished async compute's records, in FIFO (input) order.

        The runner's consumer calls this for each completed compute. ``collect``
        applies the operator's batch-expand / columnar / validation logic and
        buffers delivery commands; it runs on the executor thread because the
        edge command buffers are not loop-safe. Because the consumer is a single
        task, these collects are serialized — emission order == submit order.
        """
        if not records:
            return

        def collect_all() -> None:
            for record in records:
                self._state.operator.collect(record)

        await asyncio.get_running_loop().run_in_executor(self._state.executor, collect_all)

    def data_handler(self, record) -> None:
        """Route one accumulator output to the operator or barrier handler.

        Runs on the executor thread (sync path) or the loop (async barrier path).
        """
        if isinstance(record, Barrier):
            self.handle_barrier(
                record,
                record.sender,
                getattr(record, "delivery_channel", None),
            )
        elif isinstance(record, StreamControl):
            self.handle_stream_control(record, None)
        else:
            self._state.runner.process(record)

    def flush_input(self, force: bool = True) -> None:
        """Drain ready input batches and dispatch them on the executor thread."""
        for record in self._state.input_batches.flush(force=force):
            self.data_handler(record)

    async def flush_input_async(self, force: bool = True) -> None:
        """Drain a partial batch through the ordered async-operator path."""

        emitted = await asyncio.get_running_loop().run_in_executor(
            self._state.executor,
            lambda: self._state.input_batches.flush(force=force),
        )
        await self._dispatch_async(emitted)

    def handle_stream_control(self, control: StreamControl, sender_vertex_id: object | None) -> None:
        tracker = self._state.event_time_tracker
        if tracker is None:
            return
        outputs = tracker.on_control(sender_vertex_id, control)
        for output in outputs:
            if isinstance(output, Watermark):
                self._state.operator.on_event_time_watermark(output.timestamp)
            elif isinstance(output, InputIdle):
                self._state.operator.on_input_idle()
            elif isinstance(output, InputActive):
                self._state.operator.on_input_active()
            self._state.operator.collect(output)
        self._state.metrics.update_watermarks(
            tracker.current_watermark,
            max(
                (output.timestamp for output in outputs if isinstance(output, Watermark)),
                default=tracker.current_watermark,
            ),
            tracker.idle_input_count,
        )

    def handle_barrier(
        self,
        barrier: Barrier,
        sender_vertex_id: ExecutionVertexId | None = None,
        delivery_channel: DeliveryChannel | None = None,
    ) -> None:
        def on_barrier_aligned() -> None:
            # Flush any sink so buffered records land at the barrier, not only on
            # teardown. The operator may be a SinkOperator OR a ChainedOperator
            # wrapping one (map -> write_redis); the operator interface forwards
            # the flush to whichever component owns buffered side effects.
            self._state.operator.flush()
            self._task.prepare_sink_commit(barrier.id)
            if isinstance(barrier, EndOfData):
                self._state.operator.finish()
            state_size_bytes = self._task.snapshot_operator_state(barrier.id)
            self._task.register_checkpoint_metrics(barrier, state_size_bytes)

        if self._state.checkpoint_strategy.on_barrier_received(
            barrier,
            on_barrier_aligned,
            sender_vertex_id,
            delivery_channel,
        ):
            self._state.metrics.barriers_out.inc()
            self._state.operator.collect(barrier)
            if isinstance(barrier, EndOfData) and self._state.checkpoint_strategy.on_eof_received(barrier):
                self._task.report_eof_finished()

        self._state.metrics.observe_barrier(barrier.timestamp)

    def _idle_flush(self) -> None:
        """Executor thread on inbox idle: flush input batcher + buffered micro-batches."""
        self.flush_input(force=False)
        self._state.operator.on_idle()
        if self._state.output is not None:
            self._state.output.flush()
