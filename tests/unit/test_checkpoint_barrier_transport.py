# SPDX-License-Identifier: Apache-2.0

import asyncio
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ray.klein.config.configuration import Configuration
from ray.klein.runtime.collector.delivery_journal import DeliveryJournal
from ray.klein.runtime.collector.downstream_sender import DownstreamSender
from ray.klein.runtime.coordinator.checkpoint_strategy import _BarrierAligner
from ray.klein.runtime.execution_graph.execution_vertex_id import ExecutionVertexId
from ray.klein.runtime.message import Barrier, DeliveryChannel, Record, Watermark
from ray.klein.runtime.worker.pump import InboxPump
from ray.klein.runtime.worker.stream_task import StreamTask
from ray.klein.runtime.worker.weighted_queue import WeightedQueue


class _BarrierTarget:
    def __init__(self) -> None:
        self.calls = []

    def emit_barrier(self, barrier, **kwargs):
        self.calls.append((barrier, kwargs))
        return len(self.calls)


def _stream_task_with_pump(input_vertex_ids: tuple[ExecutionVertexId, ...]):
    inbox = WeightedQueue(20, lambda _envelope: 1)
    executor = ThreadPoolExecutor(max_workers=1)
    aligner = _BarrierAligner({}, input_vertex_ids)
    strategy = MagicMock()
    strategy.last_alignment_is_terminal = False

    def align(barrier, callback=None, sender_vertex_id=None, delivery_channel=None):
        aligned = aligner.receive(barrier, sender_vertex_id, delivery_channel)
        strategy.last_alignment_is_terminal = aligner.last_alignment_is_terminal
        if aligned and callback is not None:
            callback()
        return aligned

    strategy.on_barrier_received.side_effect = align
    strategy.barrier_to_forward.side_effect = aligner.barrier_to_forward
    strategy.on_eof_received.side_effect = aligner.receive_eof
    strategy.abort_checkpoint.side_effect = aligner.abort
    strategy.reset_inflight_before.side_effect = aligner.reset_inflight_before

    input_batches = MagicMock()
    input_batches.accept.side_effect = lambda record: (record,)
    input_batches.flush.return_value = ()
    processed: list[str] = []
    runner = MagicMock()
    runner.process.side_effect = lambda record: processed.append(record.block["id"])
    metrics = SimpleNamespace(
        barriers_in=SimpleNamespace(inc=MagicMock()),
        barriers_out=SimpleNamespace(inc=MagicMock()),
        observe_barrier=MagicMock(),
        update_input_buffer=MagicMock(),
        update_watermarks=MagicMock(),
    )
    state = SimpleNamespace(
        inbox=inbox,
        executor=executor,
        is_async_operator=False,
        input_batches=input_batches,
        operator=MagicMock(),
        runner=runner,
        async_runner=None,
        event_time_tracker=None,
        metrics=metrics,
        checkpoint_strategy=strategy,
        output=None,
    )
    task = object.__new__(StreamTask)
    task._state = state
    task._descriptor = SimpleNamespace(
        input_buffer_size=20,
        config=Configuration(include_environment=False),
        operator=SimpleNamespace(source=False),
    )
    task._checkpoint_input_gates = {}
    task._checkpoint_gate_loop = None
    task._checkpoint_gate_resolved_through = -1
    task._eof_reached = False
    task._check_end_of_stream = MagicMock(return_value=False)
    task.stop = AsyncMock()
    task.prepare_sink_commit = MagicMock()
    snapshots: list[tuple[str, ...]] = []
    task.snapshot_operator_state = MagicMock(side_effect=lambda _barrier_id: snapshots.append(tuple(processed)) or 0)
    task.register_checkpoint_metrics = MagicMock()
    task.report_eof_finished = MagicMock()
    watermark = SimpleNamespace(
        forwarded_sequence_for=MagicMock(return_value=-1),
        note_processed=MagicMock(return_value=False),
        advance=AsyncMock(),
    )
    task._watermark = watermark
    emit = SimpleNamespace(drain_pending=AsyncMock())
    pump = InboxPump(
        task,
        state,
        watermark,
        emit,
        inbox_timeout=0.1,
        input_vertex_ids=input_vertex_ids,
    )
    task._pump = pump
    return task, pump, processed, snapshots, executor


def test_downstream_barrier_carries_the_same_physical_channel_as_data() -> None:
    target = _BarrierTarget()
    sender_id = ExecutionVertexId(4, 2)
    journal = DeliveryJournal(1)
    journal.configure(
        False,
        sender_id,
        sender_task_name="upstream (3/4)",
        edge_index=1,
        topology_epoch="resize-7",
    )
    sender = DownstreamSender(
        [target],
        ["downstream (1/1)"],
        (0,),
        journal,
        put_timeout=1.0,
        namespace="test",
    )
    barrier = Barrier(9, source_id=ExecutionVertexId(1, 0))

    assert sender._barrier_requests(barrier) == [1]
    assert target.calls == [
        (
            barrier,
            {
                "sender_vertex_id": sender_id,
                "delivery_channel": DeliveryChannel(
                    sender_id,
                    "upstream (3/4)",
                    1,
                    0,
                    "resize-7",
                ),
            },
        )
    ]


@pytest.mark.asyncio
async def test_stream_task_ignores_retired_epoch_before_closing_checkpoint_gate() -> None:
    sender = ExecutionVertexId(4, 2)
    retired = DeliveryChannel(sender, "upstream", 0, 0, "epoch-1")
    current = DeliveryChannel(sender, "upstream", 0, 0, "epoch-2")
    inbox = WeightedQueue(10, lambda _envelope: 1)
    task = object.__new__(StreamTask)
    task._descriptor = SimpleNamespace(input_channels=(current,))
    task._state = SimpleNamespace(
        inbox=inbox,
        metrics=SimpleNamespace(barriers_in=MagicMock()),
    )
    task._pump = None
    task._checkpoint_input_gates = {}
    task._checkpoint_gate_loop = None
    task._checkpoint_gate_resolved_through = -1
    task._update_buffer_size_metrics = MagicMock(return_value=0)
    barrier = Barrier(12, source_id=ExecutionVertexId(1, 0))

    await task.emit_barrier(barrier, sender, retired)
    assert inbox.qsize() == 0
    assert task._checkpoint_input_gates == {}

    await task.emit_barrier(barrier, sender, current)
    assert inbox.qsize() == 1
    assert current in task._checkpoint_input_gates


def test_runtime_context_passes_exact_input_channels_to_checkpoint_strategy() -> None:
    sender = ExecutionVertexId(1, 0)
    channel = DeliveryChannel(sender, "upstream", 1, 0, "epoch-2")
    strategy = object()
    descriptor = SimpleNamespace(
        namespace="test",
        barrier_split={},
        vertex_id=ExecutionVertexId(2, 0),
        operator=SimpleNamespace(
            operator_type=object(),
            transactional_sink=False,
            runtime_info=object(),
        ),
        config=Configuration(include_environment=False),
        is_committer=False,
        metric_group=MagicMock(),
        input_vertex_ids=(sender,),
        input_channels=(channel,),
        task_name="target",
        task_index=0,
        parallelism=1,
    )
    task = object.__new__(StreamTask)

    with (
        patch(
            "ray.klein.runtime.coordinator.checkpoint_strategy.AlignedCheckpointStrategy",
            return_value=strategy,
        ) as strategy_class,
        patch("ray.klein.runtime.worker.stream_task.klein.get_actor_by_name", return_value=object()),
    ):
        context = task._build_runtime_context(descriptor)

    assert context.checkpoint_strategy is strategy
    assert strategy_class.call_args.kwargs["input_channels"] == (channel,)


@pytest.mark.asyncio
async def test_stream_task_gates_sender_until_alignment_or_abort() -> None:
    sender_id = ExecutionVertexId(4, 2)
    channel = DeliveryChannel(sender_id, "upstream", 0, 0)
    inbox = WeightedQueue(10, lambda _envelope: 1)
    checkpoint_strategy = MagicMock()
    checkpoint_strategy.abort_checkpoint.return_value = True
    checkpoint_strategy.reset_inflight_before.return_value = 1
    executor = ThreadPoolExecutor(max_workers=1)
    task = object.__new__(StreamTask)
    task._state = SimpleNamespace(
        inbox=inbox,
        metrics=SimpleNamespace(
            barriers_in=MagicMock(),
            update_input_buffer=MagicMock(),
        ),
        checkpoint_strategy=checkpoint_strategy,
        executor=executor,
    )
    task._descriptor = SimpleNamespace(
        input_buffer_size=10,
        config=Configuration(include_environment=False),
        operator=SimpleNamespace(source=False),
    )
    task._watermark = None
    task._checkpoint_input_gates = {}
    task._checkpoint_gate_loop = None
    task._checkpoint_gate_resolved_through = -1

    try:
        barrier = Barrier(12, source_id=ExecutionVertexId(1, 0))
        await task.emit_barrier(barrier, sender_vertex_id=sender_id, delivery_channel=channel)
        envelope = await inbox.get()
        assert envelope.payload is barrier
        assert envelope.sender_vertex_id == sender_id
        assert envelope.delivery_channel == channel

        blocked = await task.try_put(Record({"id": 1}), sender_vertex_id=sender_id, delivery_channel=channel)
        assert not blocked.accepted
        blocked_control = asyncio.create_task(
            task.emit_stream_control(Watermark(1), sender_vertex_id=sender_id, delivery_channel=channel)
        )
        await asyncio.sleep(0)
        assert not blocked_control.done()

        assert await task.abort_checkpoint(12)
        await blocked_control
        assert isinstance((await inbox.get()).payload, Watermark)
        accepted = await task.try_put(Record({"id": 2}), sender_vertex_id=sender_id, delivery_channel=channel)
        assert accepted.accepted
        await inbox.get()

        later = Barrier(20, source_id=ExecutionVertexId(1, 0))
        await task.emit_barrier(later, sender_vertex_id=sender_id, delivery_channel=channel)
        await inbox.get()
        assert await task.reset_inflight_before(20) == 2
        await task.emit_barrier(later, sender_vertex_id=sender_id, delivery_channel=channel)
        assert inbox.envelope_count == 0
    finally:
        executor.shutdown(wait=True)


@pytest.mark.asyncio
async def test_real_pump_excludes_post_barrier_lane_data_from_aligned_snapshot() -> None:
    first = ExecutionVertexId(4, 0)
    second = ExecutionVertexId(4, 1)
    first_channel = DeliveryChannel(first, "first", 0, 0)
    second_channel = DeliveryChannel(second, "second", 0, 0)
    task, pump, processed, snapshots, executor = _stream_task_with_pump((first, second))
    post_barrier = None

    try:
        await task.emit_barrier(Barrier(12, source_id=ExecutionVertexId(1, 0)), first, first_channel)
        post_barrier = asyncio.create_task(
            task.put(Record({"id": "post-first"}), sender_vertex_id=first, delivery_channel=first_channel)
        )
        await asyncio.sleep(0)
        assert not post_barrier.done()

        await task.put(Record({"id": "pre-second"}), sender_vertex_id=second, delivery_channel=second_channel)
        await task.emit_barrier(Barrier(12, source_id=ExecutionVertexId(2, 0)), second, second_channel)

        await pump.run_once()
        assert snapshots == []
        await pump.run_once()
        assert processed == ["pre-second"]
        await pump.run_once()

        acknowledgement = await asyncio.wait_for(post_barrier, timeout=1)
        assert acknowledgement.accepted
        assert snapshots == [("pre-second",)]
        await pump.run_once()
        assert processed == ["pre-second", "post-first"]
    finally:
        task._release_all_checkpoint_input_gates()
        if post_barrier is not None and not post_barrier.done():
            post_barrier.cancel()
            await asyncio.gather(post_barrier, return_exceptions=True)
        executor.shutdown(wait=True)


@pytest.mark.asyncio
async def test_real_pump_abort_releases_post_barrier_lane_data() -> None:
    first = ExecutionVertexId(4, 0)
    second = ExecutionVertexId(4, 1)
    first_channel = DeliveryChannel(first, "first", 0, 0)
    task, _pump, _processed, snapshots, executor = _stream_task_with_pump((first, second))
    post_barrier = None

    try:
        barrier = Barrier(12, source_id=ExecutionVertexId(1, 0))
        await task.emit_barrier(barrier, first, first_channel)
        post_barrier = asyncio.create_task(
            task.put(Record({"id": "post-first"}), sender_vertex_id=first, delivery_channel=first_channel)
        )
        await asyncio.sleep(0)
        assert not post_barrier.done()

        assert await task.abort_checkpoint(12)
        acknowledgement = await asyncio.wait_for(post_barrier, timeout=1)
        assert acknowledgement.accepted
        assert snapshots == []
    finally:
        task._release_all_checkpoint_input_gates()
        if post_barrier is not None and not post_barrier.done():
            post_barrier.cancel()
            await asyncio.gather(post_barrier, return_exceptions=True)
        executor.shutdown(wait=True)
