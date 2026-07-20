# SPDX-License-Identifier: Apache-2.0
"""Pure unit tests for the inbox pump state machine."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, call

import pytest

from ray.klein.runtime.message import (
    Barrier,
    DeliveryChannel,
    EndOfData,
    InputActive,
    InputIdle,
    Record,
    RescaleBarrier,
    Watermark,
)
from ray.klein.runtime.worker.pump import (
    InboxEnvelope,
    InboxPump,
    inbox_envelope_bytes,
    inbox_envelope_rows,
)


def _runtime(*, async_operator: bool = False, inbox_timeout: float = 0.01):
    input_batches = MagicMock()
    input_batches.accept.side_effect = lambda record: (record,)
    input_batches.flush.return_value = ()
    operator = MagicMock()
    runner = MagicMock()
    runner.process_async = AsyncMock(return_value=())
    async_runner = SimpleNamespace(
        barrier=AsyncMock(),
        submit_control=AsyncMock(),
        submit_compute=AsyncMock(),
    )
    metrics = SimpleNamespace(
        barriers_out=SimpleNamespace(inc=MagicMock()),
        observe_barrier=MagicMock(),
        update_watermarks=MagicMock(),
    )
    strategy = MagicMock()
    strategy.last_alignment_is_terminal = False
    state = SimpleNamespace(
        inbox=asyncio.Queue(),
        executor=None,
        is_async_operator=async_operator,
        input_batches=input_batches,
        operator=operator,
        runner=runner,
        async_runner=async_runner,
        event_time_tracker=None,
        metrics=metrics,
        checkpoint_strategy=strategy,
        output=None,
    )
    task = SimpleNamespace(
        eof_reached=False,
        _check_end_of_stream=MagicMock(),
        stop=AsyncMock(),
        handle_rescale_barrier=AsyncMock(),
        prepare_sink_commit=MagicMock(),
        snapshot_operator_state=MagicMock(return_value=0),
        register_checkpoint_metrics=MagicMock(),
        mark_eof_finished=MagicMock(),
        report_eof_finished=AsyncMock(),
        checkpoint_barrier_aligned=MagicMock(),
    )
    watermark = SimpleNamespace(note_processed=MagicMock(return_value=False), advance=AsyncMock())
    emit = SimpleNamespace(drain_pending=AsyncMock())
    pump = InboxPump(task, state, watermark, emit, inbox_timeout=inbox_timeout)
    return pump, task, state, watermark, emit


def test_envelope_normalizes_sequences_and_reports_weight(monkeypatch) -> None:
    first = Record({"id": [1, 2]}, num_rows=2)
    second = Record({"id": 3})
    envelope = InboxEnvelope([first, second])

    monkeypatch.setattr("ray.klein.runtime.worker.pump.estimate_retained_size", lambda payload: len(payload) * 10)

    assert envelope.payload == (first, second)
    assert inbox_envelope_rows(envelope) == 3
    assert inbox_envelope_rows(InboxEnvelope([])) == 1
    assert inbox_envelope_rows(InboxEnvelope(Record({}, num_rows=0))) == 1
    assert inbox_envelope_rows(InboxEnvelope(Watermark(1))) == 1
    assert inbox_envelope_bytes(envelope) == 20


@pytest.mark.asyncio
async def test_idle_iteration_flushes_batches_output_and_watermark() -> None:
    pump, task, state, watermark, emit = _runtime()
    buffered = Record({"id": 1})
    state.input_batches.flush.return_value = (buffered,)
    state.output = MagicMock()

    await pump.run_once()

    state.input_batches.flush.assert_called_once_with(force=False)
    state.runner.process.assert_called_once_with(buffered)
    state.operator.on_idle.assert_called_once_with()
    state.output.flush.assert_called_once_with()
    emit.drain_pending.assert_awaited_once_with()
    watermark.advance.assert_awaited_once_with()
    task._check_end_of_stream.assert_not_called()


@pytest.mark.asyncio
async def test_async_idle_flush_is_ordered_behind_async_compute() -> None:
    pump, task, state, watermark, emit = _runtime(async_operator=True)
    state.output = MagicMock()
    buffered = Record({"id": 1})
    state.input_batches.flush.return_value = (buffered,)
    pump._dispatch_async = AsyncMock()

    await pump.run_once()

    state.input_batches.flush.assert_called_once_with(force=False)
    pump._dispatch_async.assert_awaited_once_with((buffered,))
    state.runner.process.assert_not_called()
    state.async_runner.submit_control.assert_awaited_once_with(pump._complete_async_idle)
    state.operator.on_idle.assert_not_called()
    emit.drain_pending.assert_not_awaited()
    watermark.advance.assert_not_awaited()
    task._check_end_of_stream.assert_not_called()

    ordered_idle = state.async_runner.submit_control.await_args.args[0]
    await ordered_idle()
    state.operator.on_idle.assert_called_once_with()
    state.output.flush.assert_called_once_with()
    emit.drain_pending.assert_awaited_once_with()
    watermark.advance.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_sync_iteration_processes_then_advances_replay_watermark() -> None:
    pump, task, state, watermark, emit = _runtime()
    sender = object()
    channel = DeliveryChannel(sender, "upstream", 0, 1)
    record = Record({"id": 1})
    watermark.note_processed.return_value = True
    await state.inbox.put(InboxEnvelope(record, sender, 7, channel))

    await pump.run_once()

    assert record.sender is sender
    state.runner.process.assert_called_once_with(record)
    emit.drain_pending.assert_awaited_once_with()
    watermark.note_processed.assert_called_once_with(channel, 7)
    watermark.advance.assert_awaited_once_with()
    task._check_end_of_stream.assert_called_once_with()
    task.stop.assert_not_awaited()


@pytest.mark.asyncio
async def test_rescale_barrier_bypasses_operator_and_emit_pipeline() -> None:
    pump, task, state, watermark, emit = _runtime()
    sender = object()
    barrier = RescaleBarrier("scale-1", 4)
    await state.inbox.put(InboxEnvelope(barrier, sender))

    await pump.run_once()

    task.handle_rescale_barrier.assert_awaited_once_with(barrier, sender)
    state.input_batches.accept.assert_not_called()
    emit.drain_pending.assert_not_awaited()
    watermark.advance.assert_not_awaited()


@pytest.mark.asyncio
async def test_sync_operator_failure_propagates_without_acknowledging_input() -> None:
    pump, task, state, watermark, emit = _runtime()
    state.input_batches.accept.side_effect = RuntimeError("operator failed")
    await state.inbox.put(InboxEnvelope(Record({"id": 1}), object(), 3))

    with pytest.raises(RuntimeError, match="operator failed"):
        await pump.run_once()

    emit.drain_pending.assert_not_awaited()
    watermark.note_processed.assert_not_called()
    task._check_end_of_stream.assert_not_called()


@pytest.mark.asyncio
async def test_cancelling_an_idle_iteration_does_not_run_idle_actions() -> None:
    pump, task, state, watermark, emit = _runtime(inbox_timeout=60)

    pending = asyncio.create_task(pump.run_once())
    await asyncio.sleep(0)
    pending.cancel()
    with pytest.raises(asyncio.CancelledError):
        await pending

    state.input_batches.flush.assert_not_called()
    emit.drain_pending.assert_not_awaited()
    watermark.advance.assert_not_awaited()
    task.stop.assert_not_awaited()


@pytest.mark.asyncio
async def test_emit_backpressure_blocks_watermark_and_eof_until_released() -> None:
    pump, task, state, watermark, emit = _runtime()
    entered = asyncio.Event()
    release = asyncio.Event()

    async def blocked_drain() -> None:
        entered.set()
        await release.wait()

    emit.drain_pending.side_effect = blocked_drain
    watermark.note_processed.return_value = True
    await state.inbox.put(InboxEnvelope(Record({"id": 1}), object(), 9))

    pending = asyncio.create_task(pump.run_once())
    await asyncio.wait_for(entered.wait(), timeout=1)
    watermark.note_processed.assert_not_called()
    task._check_end_of_stream.assert_not_called()

    release.set()
    await asyncio.wait_for(pending, timeout=1)
    watermark.note_processed.assert_called_once()
    watermark.advance.assert_awaited_once_with()
    task._check_end_of_stream.assert_called_once_with()


@pytest.mark.asyncio
async def test_async_end_of_data_drains_runner_before_stopping() -> None:
    pump, task, state, watermark, emit = _runtime(async_operator=True)
    barrier = EndOfData(4)
    task.eof_reached = True
    pump._handle_async = AsyncMock()
    await state.inbox.put(InboxEnvelope(barrier, object(), 2))

    await pump.run_once()

    pump._handle_async.assert_awaited_once()
    state.async_runner.barrier.assert_awaited_once_with()
    task._check_end_of_stream.assert_not_called()
    task.stop.assert_awaited_once_with()
    emit.drain_pending.assert_not_awaited()
    watermark.advance.assert_not_awaited()


def test_sync_dispatch_preserves_record_and_barrier_metadata() -> None:
    pump, _, state, _, _ = _runtime()
    sender = object()
    channel = DeliveryChannel(sender, "upstream", 1, 0)
    data = Record({"id": 1})
    control = Watermark(8)
    barrier = Barrier(3)
    pump.flush_input = MagicMock()
    pump.handle_stream_control = MagicMock()
    pump.data_handler = MagicMock()

    pump._handle_sync((data, control, barrier), sender, channel)

    assert data.sender is sender
    state.input_batches.accept.assert_has_calls([call(data), call(barrier)])
    pump.flush_input.assert_called_once_with()
    pump.handle_stream_control.assert_called_once_with(control, sender)
    assert pump.data_handler.call_args_list == [
        call(data, sender_vertex_id=None, delivery_channel=None),
        call(barrier, sender_vertex_id=sender, delivery_channel=channel),
    ]


def test_sync_end_of_data_forces_a_final_input_flush() -> None:
    pump, _, _, _, _ = _runtime()
    barrier = EndOfData(5)
    pump.data_handler = MagicMock()
    pump.flush_input = MagicMock()

    pump._handle_sync(barrier, None)

    pump.data_handler.assert_called_once_with(barrier, sender_vertex_id=None, delivery_channel=None)
    pump.flush_input.assert_called_once_with()


@pytest.mark.asyncio
async def test_emit_watermark_ignores_control_and_unflushed_intervals() -> None:
    pump, _, _, watermark, emit = _runtime()
    sender = object()

    await pump._emit_drain_and_watermark(Watermark(1), sender, 1, None)
    watermark.note_processed.assert_not_called()
    watermark.advance.assert_not_awaited()

    await pump._emit_drain_and_watermark(Record({"id": 1}), sender, 2, None)
    watermark.note_processed.assert_called_once_with(sender, 2)
    watermark.advance.assert_not_awaited()
    assert emit.drain_pending.await_count == 2


@pytest.mark.asyncio
async def test_eof_check_stops_when_operator_reaches_end() -> None:
    pump, task, _, _, _ = _runtime()

    def reach_eof() -> None:
        task.eof_reached = True

    task._check_end_of_stream.side_effect = reach_eof

    await pump._check_eof_and_stop()

    task._check_end_of_stream.assert_called_once_with()
    task.report_eof_finished.assert_awaited_once_with()
    task.stop.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_async_handler_flushes_control_before_ordered_callback() -> None:
    pump, _, state, _, _ = _runtime(async_operator=True)
    control = InputIdle()
    buffered = Record({"id": 1})
    state.input_batches.flush.return_value = (buffered,)
    pump._dispatch_async = AsyncMock()
    pump.handle_stream_control = MagicMock()
    sender = object()

    await pump._handle_async(control, sender, 6, None)

    state.input_batches.flush.assert_called_once_with(force=True)
    pump._dispatch_async.assert_awaited_once_with((buffered,))
    assert state.async_runner.submit_control.await_count == 2
    ordered_control = state.async_runner.submit_control.await_args_list[0].args[0]
    await ordered_control()
    pump.handle_stream_control.assert_called_once_with(control, sender)


@pytest.mark.asyncio
async def test_async_handler_flushes_tail_after_end_of_data() -> None:
    pump, _, state, _, _ = _runtime(async_operator=True)
    barrier = EndOfData(9)
    tail = Record({"id": 2})
    state.input_batches.flush.return_value = (tail,)
    pump._dispatch_async = AsyncMock()
    sender = object()
    channel = DeliveryChannel(sender, "upstream", 0, 0)

    await pump._handle_async(barrier, sender, 4, channel)

    assert pump._dispatch_async.await_args_list == [
        call((barrier,), sender_vertex_id=sender, delivery_channel=channel),
        call((tail,)),
    ]
    state.input_batches.flush.assert_called_once_with(force=True)
    state.async_runner.submit_control.assert_awaited_once()


@pytest.mark.asyncio
async def test_async_durability_boundary_processes_partial_batch_without_sync_runner() -> None:
    pump, _, state, _, _ = _runtime(async_operator=True)
    buffered = Record({"id": 1})
    result = Record({"id": 2})
    state.input_batches.flush.return_value = (buffered,)
    state.runner.process_async.return_value = (result,)

    await pump.flush_input_async_boundary()

    state.input_batches.flush.assert_called_once_with(force=True)
    state.runner.process.assert_not_called()
    state.runner.process_async.assert_awaited_once_with(buffered)
    state.operator.collect.assert_called_once_with(result)


@pytest.mark.asyncio
async def test_async_dispatch_routes_data_barriers_and_controls_in_order() -> None:
    pump, _, state, _, _ = _runtime(async_operator=True)
    barrier = Barrier(2)
    control = InputActive(7)
    data = Record({"id": 3})
    sender = object()
    channel = DeliveryChannel(sender, "upstream", 0, 0)
    controls = []
    compute_results = []

    async def submit_control(action) -> None:
        controls.append(action)

    async def submit_compute(compute) -> None:
        compute_results.append(await compute)

    state.async_runner.submit_control.side_effect = submit_control
    state.async_runner.submit_compute.side_effect = submit_compute
    state.runner.process_async.return_value = [Record({"done": 3})]
    pump.handle_barrier = MagicMock()
    pump.handle_stream_control = MagicMock()

    await pump._dispatch_async(
        (barrier, control, data),
        sender_vertex_id=sender,
        delivery_channel=channel,
    )

    assert len(controls) == 2
    await controls[0]()
    await controls[1]()
    pump.handle_barrier.assert_called_once_with(barrier, sender, channel)
    pump.handle_stream_control.assert_called_once_with(control, None)
    state.runner.process_async.assert_awaited_once_with(data)
    assert compute_results == [[Record({"done": 3})]]


@pytest.mark.asyncio
async def test_async_result_collection_is_serialized_on_executor() -> None:
    pump, _, state, _, _ = _runtime(async_operator=True)
    first = Record({"id": 1})
    second = Record({"id": 2})

    await pump.on_async_result(())
    state.operator.collect.assert_not_called()

    await pump.on_async_result((first, second))
    assert state.operator.collect.call_args_list == [call(first), call(second)]


def test_stream_control_updates_callbacks_outputs_and_metrics() -> None:
    pump, _, state, _, _ = _runtime()
    sender = object()
    outputs = (Watermark(12), InputIdle(), InputActive(11))
    tracker = SimpleNamespace(
        on_control=MagicMock(return_value=outputs),
        current_watermark=10,
        idle_input_count=2,
    )
    state.event_time_tracker = tracker

    pump.handle_stream_control(Watermark(9), sender)

    tracker.on_control.assert_called_once_with(sender, Watermark(9))
    state.operator.on_event_time_watermark.assert_called_once_with(12)
    state.operator.on_input_idle.assert_called_once_with()
    state.operator.on_input_active.assert_called_once_with()
    assert state.operator.collect.call_args_list == [call(output) for output in outputs]
    state.metrics.update_watermarks.assert_called_once_with(10, 12, 2)


def test_stream_control_without_tracker_is_a_noop() -> None:
    pump, _, state, _, _ = _runtime()

    pump.handle_stream_control(Watermark(1), object())

    state.operator.collect.assert_not_called()
    state.metrics.update_watermarks.assert_not_called()


def test_aligned_terminal_barrier_snapshots_forwards_and_finishes() -> None:
    pump, task, state, _, _ = _runtime()
    barrier = EndOfData(7)
    forwarded = EndOfData(7)

    def align(received, callback) -> bool:
        assert received is barrier
        callback()
        return True

    state.checkpoint_strategy.on_barrier_received.side_effect = align
    state.checkpoint_strategy.barrier_to_forward.return_value = forwarded
    state.checkpoint_strategy.on_eof_received.return_value = True
    task.snapshot_operator_state.return_value = 42

    pump.handle_barrier(barrier)

    state.operator.flush.assert_called_once_with()
    task.prepare_sink_commit.assert_called_once_with(7)
    state.operator.finish.assert_called_once_with()
    task.snapshot_operator_state.assert_called_once_with(7)
    task.register_checkpoint_metrics.assert_called_once_with(barrier, 42)
    state.metrics.barriers_out.inc.assert_called_once_with()
    state.operator.collect.assert_called_once_with(forwarded)
    task.mark_eof_finished.assert_called_once_with()
    task.checkpoint_barrier_aligned.assert_called_once_with(7)
    state.metrics.observe_barrier.assert_called_once_with(barrier.timestamp)


def test_unaligned_barrier_only_records_observation() -> None:
    pump, task, state, _, _ = _runtime()
    barrier = Barrier(8)
    sender = object()
    state.checkpoint_strategy.on_barrier_received.return_value = False

    pump.handle_barrier(barrier, sender_vertex_id=sender)

    state.checkpoint_strategy.on_barrier_received.assert_called_once_with(
        barrier,
        state.checkpoint_strategy.on_barrier_received.call_args.args[1],
        sender_vertex_id=sender,
    )
    state.operator.flush.assert_not_called()
    state.operator.collect.assert_not_called()
    task.checkpoint_barrier_aligned.assert_not_called()
    state.metrics.observe_barrier.assert_called_once_with(barrier.timestamp)


def test_idle_flush_without_output_still_notifies_operator() -> None:
    pump, _, state, _, _ = _runtime()

    pump._idle_flush()

    state.input_batches.flush.assert_called_once_with(force=False)
    state.operator.on_idle.assert_called_once_with()
