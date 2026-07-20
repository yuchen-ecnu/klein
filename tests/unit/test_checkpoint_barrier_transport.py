# SPDX-License-Identifier: Apache-2.0

import asyncio
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from ray.klein.config.configuration import Configuration
from ray.klein.runtime.collector.delivery_journal import DeliveryJournal
from ray.klein.runtime.collector.downstream_sender import DownstreamSender
from ray.klein.runtime.execution_graph.execution_vertex_id import ExecutionVertexId
from ray.klein.runtime.message import Barrier, DeliveryChannel, Record, Watermark
from ray.klein.runtime.worker.stream_task import StreamTask
from ray.klein.runtime.worker.weighted_queue import WeightedQueue


class _BarrierTarget:
    def __init__(self) -> None:
        self.calls = []

    def emit_barrier(self, barrier, **kwargs):
        self.calls.append((barrier, kwargs))
        return len(self.calls)


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
        blocked_control = asyncio.create_task(task.emit_stream_control(Watermark(1), sender_vertex_id=sender_id))
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
