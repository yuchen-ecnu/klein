# SPDX-License-Identifier: Apache-2.0
import asyncio
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from ray.klein.runtime.execution_graph.execution_vertex_id import ExecutionVertexId
from ray.klein.runtime.message import Barrier, DeliveryChannel, Record
from ray.klein.runtime.worker.pump import InboxEnvelope, InboxPump
from ray.klein.runtime.worker.stream_task import StreamTask
from ray.klein.runtime.worker.weighted_queue import WeightedQueue


@pytest.mark.asyncio
async def test_post_barrier_input_is_held_until_all_direct_inputs_align() -> None:
    first = ExecutionVertexId(1, 0)
    second = ExecutionVertexId(1, 1)
    inbox = WeightedQueue[InboxEnvelope](10, lambda _envelope: 1)
    executor = ThreadPoolExecutor(max_workers=1)
    state = SimpleNamespace(inbox=inbox, executor=executor, is_async_operator=False)
    pump = InboxPump(
        Mock(eof_reached=False),
        state,
        AsyncMock(),
        AsyncMock(),
        input_vertex_ids=(first, second),
    )
    pump._handle_sync = Mock()
    pump._emit_drain_and_watermark = AsyncMock()
    pump._check_eof_and_stop = AsyncMock()
    barrier = Barrier(7, first, coordinated=True)
    second_barrier = Barrier(7, second, coordinated=True)
    post_barrier = Record({"value": "after"})

    try:
        await inbox.put(InboxEnvelope(barrier, first))
        await pump.run_once()
        assert pump.checkpoint_input_blocked(first) is True

        gate_wait = asyncio.create_task(pump.wait_for_checkpoint_input(first, post_barrier))
        await asyncio.sleep(0)
        assert gate_wait.done() is False

        # The blocked item remains charged to the weighted queue, while the
        # other physical input can be selected past it to finish alignment.
        await inbox.put(InboxEnvelope(post_barrier, first))
        await inbox.put(InboxEnvelope(second_barrier, second))
        assert inbox.qsize() == 2
        await pump.run_once()
        await asyncio.wait_for(gate_wait, timeout=1)
        assert pump.checkpoint_input_blocked(first) is False
        assert inbox.qsize() == 1

        await pump.run_once()
        assert [call.args[0] for call in pump._handle_sync.call_args_list] == [
            barrier,
            second_barrier,
            post_barrier,
        ]
    finally:
        executor.shutdown(wait=False)


@pytest.mark.asyncio
async def test_capacity_one_inbox_drains_pre_barrier_data_without_starving_peer_barrier() -> None:
    first = ExecutionVertexId(1, 0)
    second = ExecutionVertexId(1, 1)
    inbox = WeightedQueue[InboxEnvelope](1, lambda _envelope: 1)
    executor = ThreadPoolExecutor(max_workers=1)
    state = SimpleNamespace(
        inbox=inbox,
        executor=executor,
        is_async_operator=False,
        metrics=SimpleNamespace(barriers_in=Mock()),
    )
    stream_task = object.__new__(StreamTask)
    stream_task._state = state
    stream_task._update_buffer_size_metrics = Mock(return_value=0)
    pump = InboxPump(
        stream_task,
        state,
        AsyncMock(),
        AsyncMock(),
        input_vertex_ids=(first, second),
    )
    stream_task._pump = pump
    pump._handle_sync = Mock()
    pump._emit_drain_and_watermark = AsyncMock()
    pump._check_eof_and_stop = AsyncMock()
    before = Record({"value": "before"})
    second_before = Record({"value": "second-before"})
    after = Record({"value": "after"})
    first_barrier = Barrier(7, first, coordinated=True)
    second_barrier = Barrier(7, second, coordinated=True)

    async def admit_after_first_barrier() -> None:
        await pump.wait_for_checkpoint_input(first, after)
        await inbox.put(InboxEnvelope(after, first))

    try:
        # The inbox is full before the first barrier RPC arrives. The bounded
        # control allowance admits the barrier immediately without hiding the
        # older queued record.
        await inbox.put(InboxEnvelope(before, first))
        await asyncio.wait_for(stream_task.emit_barrier(first_barrier, first), timeout=1)
        assert inbox.qsize() == 2
        await asyncio.wait_for(pump.run_once(), timeout=1)
        await asyncio.wait_for(pump.run_once(), timeout=1)

        post_barrier_put = asyncio.create_task(admit_after_first_barrier())
        await asyncio.sleep(0)
        assert post_barrier_put.done() is False

        # Another lane may fill ordinary capacity, but it cannot starve the peer
        # barrier needed to finish the alignment.
        await inbox.put(InboxEnvelope(second_before, second))
        await asyncio.wait_for(stream_task.emit_barrier(second_barrier, second), timeout=1)
        await asyncio.wait_for(pump.run_once(), timeout=1)
        await asyncio.wait_for(pump.run_once(), timeout=1)
        await asyncio.wait_for(post_barrier_put, timeout=1)
        await asyncio.wait_for(pump.run_once(), timeout=1)

        assert [call.args[0] for call in pump._handle_sync.call_args_list] == [
            before,
            first_barrier,
            second_before,
            second_barrier,
            after,
        ]
        assert inbox.qsize() == 0
    finally:
        executor.shutdown(wait=False)


@pytest.mark.asyncio
async def test_aborted_checkpoint_releases_channel_backpressure_and_held_records() -> None:
    first = ExecutionVertexId(1, 0)
    second = ExecutionVertexId(1, 1)
    inbox = WeightedQueue[InboxEnvelope](10, lambda _envelope: 1)
    executor = ThreadPoolExecutor(max_workers=1)
    state = SimpleNamespace(inbox=inbox, executor=executor, is_async_operator=False)
    pump = InboxPump(
        Mock(eof_reached=False),
        state,
        AsyncMock(),
        AsyncMock(),
        input_vertex_ids=(first, second),
    )
    pump._handle_sync = Mock()
    pump._emit_drain_and_watermark = AsyncMock()
    pump._check_eof_and_stop = AsyncMock()
    post_barrier = Record({"value": "after"})

    try:
        await inbox.put(InboxEnvelope(Barrier(7, first, coordinated=True), first))
        await pump.run_once()
        await inbox.put(InboxEnvelope(post_barrier, first))
        gate_wait = asyncio.create_task(pump.wait_for_checkpoint_input(first, post_barrier))
        await asyncio.sleep(0)

        assert pump.discard_checkpoint(7) == 0
        await asyncio.wait_for(gate_wait, timeout=1)
        await pump.run_once()
        assert pump._handle_sync.call_args_list[-1].args[0] is post_barrier

        late = Barrier(7, second, coordinated=True)
        await inbox.put(InboxEnvelope(late, second))
        await pump.run_once()
        assert pump.checkpoint_input_blocked(second) is False
    finally:
        executor.shutdown(wait=False)


@pytest.mark.asyncio
async def test_coordinator_recovery_releases_checkpoint_input_gate() -> None:
    first = ExecutionVertexId(1, 0)
    second = ExecutionVertexId(1, 1)
    inbox = WeightedQueue[InboxEnvelope](10, lambda _envelope: 1)
    executor = ThreadPoolExecutor(max_workers=1)
    state = SimpleNamespace(inbox=inbox, executor=executor, is_async_operator=False)
    pump = InboxPump(
        Mock(eof_reached=False),
        state,
        AsyncMock(),
        AsyncMock(),
        input_vertex_ids=(first, second),
    )
    pump._handle_sync = Mock()
    pump._emit_drain_and_watermark = AsyncMock()
    pump._check_eof_and_stop = AsyncMock()

    try:
        await inbox.put(InboxEnvelope(Barrier(7, first, coordinated=True), first))
        await pump.run_once()
        waiter = asyncio.create_task(pump.wait_for_checkpoint_input(first, Record({"value": 1})))
        await asyncio.sleep(0)
        assert waiter.done() is False

        assert pump.reset_inflight_before(7) == 0
        await asyncio.wait_for(waiter, timeout=1)
        assert pump.checkpoint_input_blocked(first) is False
    finally:
        executor.shutdown(wait=False)


@pytest.mark.asyncio
async def test_duplicate_sender_edges_align_by_delivery_channel() -> None:
    sender = ExecutionVertexId(1, 0)
    first_channel = DeliveryChannel(sender, "source", 0, 0)
    second_channel = DeliveryChannel(sender, "source", 1, 0)
    inbox = WeightedQueue[InboxEnvelope](10, lambda _envelope: 1)
    executor = ThreadPoolExecutor(max_workers=1)
    state = SimpleNamespace(inbox=inbox, executor=executor, is_async_operator=False)
    pump = InboxPump(
        Mock(eof_reached=False),
        state,
        AsyncMock(),
        AsyncMock(),
        input_vertex_ids=(sender, sender),
    )
    pump._handle_sync = Mock()
    pump._emit_drain_and_watermark = AsyncMock()
    pump._check_eof_and_stop = AsyncMock()

    try:
        await inbox.put(
            InboxEnvelope(
                Barrier(7, sender, coordinated=True),
                sender,
                delivery_channel=first_channel,
            )
        )
        await pump.run_once()
        assert pump.checkpoint_input_blocked(sender, first_channel) is True
        assert pump.checkpoint_input_blocked(sender, second_channel) is False

        # A retried barrier on edge 0 cannot impersonate edge 1.
        await inbox.put(
            InboxEnvelope(
                Barrier(7, sender, coordinated=True),
                sender,
                delivery_channel=first_channel,
            )
        )
        await pump.run_once()
        assert pump.checkpoint_input_blocked(sender, first_channel) is True

        await inbox.put(
            InboxEnvelope(
                Barrier(7, sender, coordinated=True),
                sender,
                delivery_channel=second_channel,
            )
        )
        await pump.run_once()
        assert pump.checkpoint_input_blocked(sender, first_channel) is False
    finally:
        executor.shutdown(wait=False)


def test_checkpoint_gate_reconfigures_physical_inputs_after_scale_out() -> None:
    first = ExecutionVertexId(1, 0)
    second = ExecutionVertexId(1, 1)
    added = ExecutionVertexId(1, 2)
    pump = InboxPump(
        Mock(),
        SimpleNamespace(),
        AsyncMock(),
        AsyncMock(),
        input_vertex_ids=(first, second),
    )
    pump.reconfigure_checkpoint_inputs((first, second, added))

    barrier = Barrier(7, first, coordinated=True)
    assert pump._observe_coordinated_barrier(InboxEnvelope(barrier, first)) is False
    assert pump._observe_coordinated_barrier(InboxEnvelope(barrier, second)) is False
    assert pump._observe_coordinated_barrier(InboxEnvelope(barrier, added)) is True
    assert pump._checkpoint_expected_inputs == Counter((first, second, added))
