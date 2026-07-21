# SPDX-License-Identifier: Apache-2.0

import asyncio
import threading
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from ray.klein.runtime.collector.delivery_command import (
    BarrierCommand,
    DataCommand,
    DeliveryCommand,
    EdgeCommand,
)
from ray.klein.runtime.collector.downstream_batcher import DownstreamBatcher
from ray.klein.runtime.collector.edge_output import DeliveryMode, EdgeOutput
from ray.klein.runtime.collector.task_output import TaskOutput
from ray.klein.runtime.message import Barrier, PutAck, Record, Watermark
from ray.klein.runtime.partitioning import BroadcastPartitioner
from tests.unit.task_output_utils import open_task_output


class _BlockingEdge:
    def __init__(self, blocked: bool = False) -> None:
        self.blocked = blocked
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.received = []

    async def send_commands(self, commands) -> None:
        self.received.extend(commands)
        self.started.set()
        if self.blocked:
            await self.release.wait()


@pytest.mark.asyncio
async def test_fan_out_edges_progress_concurrently_without_losing_per_edge_order() -> None:
    slow = _BlockingEdge(blocked=True)
    fast = _BlockingEdge()
    output = TaskOutput([slow, fast])
    slow_commands = [EdgeCommand(), EdgeCommand()]
    fast_commands = [EdgeCommand(), EdgeCommand()]
    commands = [
        DeliveryCommand(0, slow_commands[0]),
        DeliveryCommand(1, fast_commands[0]),
        DeliveryCommand(0, slow_commands[1]),
        DeliveryCommand(1, fast_commands[1]),
    ]

    sending = asyncio.create_task(output.send_commands(commands))
    await slow.started.wait()
    await fast.started.wait()

    assert fast.received == fast_commands
    assert slow.received == slow_commands
    assert not sending.done()

    slow.release.set()
    await sending


class _LaneEdge(EdgeOutput):
    def __init__(self) -> None:
        self.started = {0: asyncio.Event(), 1: asyncio.Event()}
        self.release = {0: asyncio.Event(), 1: asyncio.Event()}
        self.events: list[tuple[str, int | None]] = []

    async def _send_async(self, command) -> None:
        if isinstance(command, DataCommand):
            self.events.append(("data", command.target))
            self.started[command.target].set()
            await self.release[command.target].wait()
        elif isinstance(command, BarrierCommand):
            self.events.append(("barrier", None))


@pytest.mark.asyncio
async def test_edge_targets_progress_concurrently_and_barrier_is_a_fence() -> None:
    edge = _LaneEdge()
    first = Record({"id": 1})
    sending = asyncio.create_task(
        edge.send_commands(
            [
                DataCommand(0, (0,), (first,)),
                DataCommand(1, (1,), (first,)),
                BarrierCommand(Barrier(1)),
                DataCommand(0, (0,), (Record({"id": 2}),)),
            ]
        )
    )

    await edge.started[0].wait()
    await edge.started[1].wait()
    assert ("barrier", None) not in edge.events

    edge.release[0].set()
    await asyncio.sleep(0)
    assert ("barrier", None) not in edge.events
    edge.release[1].set()
    await sending

    barrier_index = edge.events.index(("barrier", None))
    assert all(event[0] == "data" for event in edge.events[:barrier_index])
    assert edge.events[barrier_index + 1 :] == [("data", 0)]


def test_downstream_batcher_uses_independent_target_idle_clocks() -> None:
    batcher = DownstreamBatcher(target_count=2, batch_size=10, idle_timeout=5)
    with patch(
        "ray.klein.runtime.collector.downstream_batcher.time.monotonic",
        side_effect=[0.0, 3.0, 6.0],
    ):
        batcher.append(0, Record({"id": 0}))
        batcher.append(1, Record({"id": 1}))
        ready = list(batcher.drain(force=False))

    assert ready == [(0, (Record({"id": 0}),))]


def test_downstream_batcher_flushes_a_large_columnar_record_by_rows() -> None:
    batcher = DownstreamBatcher(target_count=1, batch_size=10, max_rows=3)
    record = Record({"id": [1, 2, 3]}, num_rows=3)

    batcher.append(0, record)

    assert batcher.take_full(0) == (record,)


def test_downstream_batcher_flushes_wide_records_by_bytes() -> None:
    batcher = DownstreamBatcher(target_count=1, batch_size=10, max_bytes=1)
    record = Record({"blob": b"wide"})

    batcher.append(0, record)

    assert batcher.take_full(0) == (record,)


def test_delivery_abort_releases_synchronous_backpressure() -> None:
    attempted = threading.Event()
    errors = []

    class FullTarget:
        def try_put(self, _records, **_kwargs):
            attempted.set()
            return PutAck(False, 1)

    output = open_task_output(
        [FullTarget()],
        BroadcastPartitioner(),
        (0,),
        ["full"],
        max_rows=1,
    )

    def send() -> None:
        try:
            output.collect(Record({"id": 1}))
        except BaseException as error:
            errors.append(error)

    sender = threading.Thread(target=send)
    sender.start()
    assert attempted.wait(1)

    output.abort_delivery()
    sender.join(1)

    assert not sender.is_alive()
    assert errors == []
    output.close()


@pytest.mark.asyncio
async def test_delivery_abort_releases_asynchronous_backpressure() -> None:
    attempted = asyncio.Event()

    class FullTarget:
        def try_put(self, _records, **_kwargs):
            attempted.set()
            return PutAck(False, 1)

    output = open_task_output(
        [FullTarget()],
        BroadcastPartitioner(),
        (0,),
        ["full"],
        max_rows=1,
        delivery_mode=DeliveryMode.PIPELINED,
    )
    output.collect(Record({"id": 1}))
    sending = asyncio.create_task(output.send_commands(output.take_pending_commands()))
    await asyncio.wait_for(attempted.wait(), timeout=1)

    output.abort_delivery()
    await asyncio.wait_for(sending, timeout=1)

    output.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [Record({"id": 1}), Barrier(1), Watermark(1)])
async def test_debug_delivery_abort_cancels_an_inflight_async_rpc(payload) -> None:
    started = asyncio.Event()
    cancelled = asyncio.Event()

    class Target:
        def try_put(self, _records, **_kwargs):
            return object()

        def emit_barrier(self, _barrier, **_kwargs):
            return object()

        def emit_stream_control(self, _control, **_kwargs):
            return object()

    async def blocked_aget(_request):
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    output = open_task_output(
        [Target()],
        BroadcastPartitioner(),
        (0,),
        ["target"],
        max_rows=1,
        delivery_mode=DeliveryMode.PIPELINED,
    )
    output.collect(payload)
    with (
        patch("ray.klein.runtime.collector.downstream_sender.klein.is_debug_mode", return_value=True),
        patch("ray.klein.runtime.collector.downstream_sender.klein.aget", side_effect=blocked_aget),
    ):
        sending = asyncio.create_task(output.send_commands(output.take_pending_commands()))
        await asyncio.wait_for(started.wait(), timeout=1)

        aborter = threading.Thread(target=output.abort_delivery)
        aborter.start()
        aborter.join(1)
        assert not aborter.is_alive()
        await asyncio.wait_for(sending, timeout=1)

    assert cancelled.is_set()
    output.close()


@pytest.mark.asyncio
async def test_real_ray_delivery_keeps_the_direct_inflight_wait_path() -> None:
    started = asyncio.Event()
    release = asyncio.Event()

    class Target:
        def emit_barrier(self, _barrier, **_kwargs):
            return object()

    async def blocked_aget(_request):
        started.set()
        await release.wait()

    output = open_task_output(
        [Target()],
        BroadcastPartitioner(),
        (0,),
        ["target"],
        delivery_mode=DeliveryMode.PIPELINED,
    )
    output.collect(Barrier(1))
    with (
        patch("ray.klein.runtime.collector.downstream_sender.klein.is_debug_mode", return_value=False),
        patch("ray.klein.runtime.collector.downstream_sender.klein.aget", side_effect=blocked_aget),
    ):
        sending = asyncio.create_task(output.send_commands(output.take_pending_commands()))
        await asyncio.wait_for(started.wait(), timeout=1)
        output.abort_delivery()
        await asyncio.sleep(0)
        assert not sending.done()

        release.set()
        await asyncio.wait_for(sending, timeout=1)

    output.close()


def test_edge_swap_rejects_invalid_transactions_without_losing_the_live_route() -> None:
    old = MagicMock(spec=EdgeOutput)
    replacement = MagicMock(spec=EdgeOutput)
    output = TaskOutput([old])
    output.open(SimpleNamespace(metric_group=None))

    with pytest.raises(ValueError, match="logical output edge count"):
        output.prepare_edge_swap("resize-1", [])

    output.prepare_edge_swap("resize-1", [replacement])
    output.prepare_edge_swap("resize-1", [replacement])
    with pytest.raises(RuntimeError, match="already prepared"):
        output.prepare_edge_swap("resize-2", [replacement])
    with pytest.raises(RuntimeError, match="does not belong"):
        output.activate_edge_swap("resize-2")
    with pytest.raises(RuntimeError, match="has not been activated"):
        output.commit_edge_swap("resize-1")

    assert output.rollback_edge_swap("resize-1") is True
    assert output.rollback_edge_swap("resize-1") is False
    assert output._edges == (old,)
    replacement.close.assert_called_once_with()


def test_edge_swap_closes_prepared_edges_when_later_open_fails() -> None:
    old_first = MagicMock(spec=EdgeOutput)
    old_second = MagicMock(spec=EdgeOutput)
    first = MagicMock(spec=EdgeOutput)
    second = MagicMock(spec=EdgeOutput)
    second.open.side_effect = RuntimeError("open failed")
    output = TaskOutput([old_first, old_second])
    output.open(SimpleNamespace(metric_group=None))

    with pytest.raises(RuntimeError, match="open failed"):
        output.prepare_edge_swap("resize-1", [first, second])

    first.close.assert_called_once_with()
    assert output._edge_swap is None
    assert output._edges == (old_first, old_second)


@pytest.mark.asyncio
async def test_large_broadcast_batch_is_put_in_object_store_once(monkeypatch) -> None:
    payload_ref = object()
    payloads = []

    class Target:
        def try_put(self, records, **_kwargs):
            payloads.append(records)
            return PutAck(True, 0)

    monkeypatch.setattr("ray.klein.runtime.collector.edge_output.ray.is_initialized", lambda: True)
    monkeypatch.setattr("ray.klein.runtime.collector.edge_output.klein.is_debug_mode", lambda: False)
    put = patch("ray.klein.runtime.collector.edge_output.ray.put", return_value=payload_ref)
    with put as ray_put:
        output = open_task_output(
            [Target(), Target()],
            BroadcastPartitioner(),
            (0, 1),
            ["d0", "d1"],
            delivery_mode=DeliveryMode.PIPELINED,
            config_values={"pipeline.transport.object-store-threshold-bytes": 1},
        )
        output.collect(Record({"blob": b"x" * 1024}))
        await output.send_commands(output.take_pending_commands())

    ray_put.assert_called_once()
    assert payloads == [payload_ref, payload_ref]
