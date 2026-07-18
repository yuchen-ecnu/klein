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
from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

from ray.klein.runtime.message import (
    Barrier,
    EndOfData,
    InputActive,
    InputIdle,
    Record,
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

    def __post_init__(self) -> None:
        if isinstance(self.payload, Sequence):
            object.__setattr__(self, "payload", tuple(self.payload))


class InboxPump:
    """Drives one operator from its inbox; owns the per-iteration pump logic."""

    def __init__(
        self,
        task: "StreamTask",
        state: "_RuntimeState",
        watermark: "WatermarkController",
        emit_pipeline: "EmitPipeline",
        inbox_timeout: float = 3.0,
    ) -> None:
        self._task = task
        self._state = state
        self._watermark = watermark
        self._emit = emit_pipeline
        self._inbox_timeout = inbox_timeout

    async def run_once(self) -> None:
        """One pump iteration (the body of the AsyncWorker loop)."""
        loop = asyncio.get_running_loop()
        try:
            envelope = await asyncio.wait_for(self._state.inbox.get(), timeout=self._inbox_timeout)
        except asyncio.TimeoutError:
            await loop.run_in_executor(self._state.executor, self._idle_flush)
            await self._emit.drain_pending()
            # Idle: force a watermark flush so a low-rate stream still releases
            # the upstream's replay buffer instead of pinning it indefinitely.
            await self._watermark.advance()
            return

        payload = envelope.payload
        if self._state.is_async_operator:
            # Async path: feed the concurrency window and return immediately so
            # the NEXT envelope can start its requests while these are still in
            # flight — that overlap is the whole point. Emit + watermark run in
            # the runner's FIFO consumer (in order). The eof check + stop must
            # run on the pump loop (stop() awaits the consumer, so running it
            # inside would deadlock), so only on EndOfData do we drain the runner
            # via barrier() and then finalize here. EndOfData is terminal, so the
            # one-time stall there costs nothing.
            await self._handle_async(payload, envelope.sender_vertex_id, envelope.sequence)
            if isinstance(payload, EndOfData):
                await self._state.async_runner.barrier()
                await self._check_eof_and_stop()
            return
        await loop.run_in_executor(
            self._state.executor,
            self._handle_sync,
            payload,
            envelope.sender_vertex_id,
        )
        await self._emit_drain_and_watermark(payload, envelope.sender_vertex_id, envelope.sequence)
        await self._check_eof_and_stop()

    async def _emit_drain_and_watermark(
        self,
        payload,
        sender_vertex_id: object | None,
        sequence: int | None,
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
            sender_vertex_id, sequence
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

    def _handle_sync(self, payload, sender_vertex_id: object | None) -> None:
        """Runs in the executor thread — batcher + operator (buffers emit-ops)."""
        for record in payload if isinstance(payload, Sequence) else (payload,):
            if isinstance(record, StreamControl):
                self._state.batching_strategy.force_flush()
                self.handle_stream_control(record, sender_vertex_id)
                continue
            if not isinstance(record, Barrier | StreamControl):
                record.sender = sender_vertex_id
            self._state.batching_strategy.accumulate_then_flush(record)
        if isinstance(payload, EndOfData):
            self._state.batching_strategy.force_flush()

    async def _handle_async(
        self,
        payload,
        sender_vertex_id: object | None,
        sequence: int | None,
    ) -> None:
        # Async path producer: input flows through the batcher first (same as the
        # sync path) so a batched async operator — e.g. the serve
        # EmbeddedProxyClient — receives batch_size-shaped blocks. Each flushed
        # data record is submitted to the ordered runner as a *concurrent*
        # compute; barriers and the per-envelope post-process bookkeeping are
        # submitted as in-order control actions. The runner's FIFO consumer then
        # emits results in submit order (ORDERED), so up to async_buffer_size
        # requests are in flight at once without racing the collector.
        loop = asyncio.get_running_loop()
        runner = self._state.async_runner
        for record in payload if isinstance(payload, Sequence) else (payload,):
            if isinstance(record, StreamControl):
                emitted = await loop.run_in_executor(
                    self._state.executor,
                    self._state.batching_strategy.collect_force_flush,
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
            if not isinstance(record, Barrier | StreamControl):
                record.sender = sender_vertex_id
            emitted = await loop.run_in_executor(
                self._state.executor,
                self._state.batching_strategy.collect_accumulate_then_flush,
                record,
            )
            await self._dispatch_async(emitted)
        if isinstance(payload, EndOfData):
            emitted = await loop.run_in_executor(
                self._state.executor,
                self._state.batching_strategy.collect_force_flush,
            )
            await self._dispatch_async(emitted)
        # Per-envelope emit-drain + watermark runs in order behind this
        # envelope's data, once its output has actually been emitted. The eof
        # check + stop is NOT submitted here — it runs on the pump loop after
        # runner.barrier() (see run_once) to avoid a stop()/consumer deadlock.
        await runner.submit_control(lambda: self._emit_drain_and_watermark(payload, sender_vertex_id, sequence))

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
        buffers emit-ops; it runs on the executor thread because the collector's
        ``_pending`` buffer is not loop-safe. Because the consumer is a single
        task, these collects are serialized — emission order == submit order.
        """
        if not records:
            return

        def collect_all() -> None:
            for record in records:
                self._state.operator.collect(record)

        await asyncio.get_running_loop().run_in_executor(self._state.executor, collect_all)

    def data_handler(self, record) -> None:
        """The InputBatcher's sink: route a flushed record/barrier to the operator.

        Runs on the executor thread (sync path) or the loop (async barrier path).
        """
        if isinstance(record, Barrier):
            self.handle_barrier(record)
        elif isinstance(record, StreamControl):
            self.handle_stream_control(record, None)
        else:
            self._state.runner.process(record)

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

    def handle_barrier(self, barrier: Barrier) -> None:
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

        if self._state.checkpoint_strategy.on_barrier_received(barrier, on_barrier_aligned):
            self._state.metrics.barriers_out.inc()
            self._state.operator.collect(barrier)
            if isinstance(barrier, EndOfData) and self._state.checkpoint_strategy.on_eof_received(barrier):
                self._task.report_eof_finished()

        self._state.metrics.observe_barrier(barrier.timestamp)

    def _idle_flush(self) -> None:
        """Executor thread on inbox idle: flush input batcher + buffered micro-batches."""
        self._state.batching_strategy.flush()
        self._state.operator.on_idle()
        if self._state.collector is not None:
            self._state.collector.flush()
