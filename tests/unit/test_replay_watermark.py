# SPDX-License-Identifier: Apache-2.0
"""Tests for replay-watermark durability boundaries."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from ray.klein.runtime.message import DeliveryChannel
from ray.klein.runtime.worker.stream_task import StreamTask
from ray.klein.runtime.worker.watermark import WatermarkController, WatermarkMode


@pytest.mark.asyncio
async def test_inline_source_replay_requires_checkpoint_restore() -> None:
    task = object.__new__(StreamTask)
    output = MagicMock()
    output.replay_commands_for.return_value = [MagicMock()]
    task._state = SimpleNamespace(output=output, pipelined=False)
    task._emit = MagicMock()

    with pytest.raises(RuntimeError, match="complete checkpoint restore"):
        await task.replay_buffered_to("downstream")
    task._emit.enqueue_commands.assert_not_called()
    output.refresh_downstream.assert_not_called()


@pytest.mark.asyncio
async def test_pipelined_task_propagates_replay_failure() -> None:
    task = object.__new__(StreamTask)
    output = MagicMock()
    commands = [MagicMock()]
    output.replay_commands_for.return_value = commands
    task._state = SimpleNamespace(output=output, pipelined=True)
    task._emit = SimpleNamespace(replay=AsyncMock(side_effect=RuntimeError("delivery failed")))

    with pytest.raises(RuntimeError, match="delivery failed"):
        await task.replay_buffered_to("downstream")
    output.refresh_downstream.assert_called_once_with("downstream")
    task._emit.replay.assert_awaited_once_with(commands)


@pytest.mark.asyncio
async def test_sink_watermark_flushes_before_advancing() -> None:
    operator = MagicMock()
    flush_input = MagicMock()
    controller = WatermarkController(WatermarkMode.SINK, flush_interval_batches=1)
    controller.bind(None, operator, None, None, flush_input)

    assert controller.note_processed("source-1", 7) is True
    await controller.advance()

    flush_input.assert_called_once_with()
    operator.flush.assert_called_once_with()
    assert controller.forwarded_sequence_for("source-1") == 7


@pytest.mark.asyncio
async def test_watermark_advances_every_pending_sender() -> None:
    controller = WatermarkController(WatermarkMode.SINK, flush_interval_batches=2)
    controller.bind(None, MagicMock(), None, None, MagicMock())

    assert controller.note_processed("left", 3) is False
    assert controller.note_processed("right", 9) is True
    await controller.advance()

    assert controller.forwarded_sequence_for("left") == 3
    assert controller.forwarded_sequence_for("right") == 9


@pytest.mark.asyncio
async def test_async_watermark_uses_ordered_input_boundary() -> None:
    operator = MagicMock()
    flush_input = MagicMock()
    flush_input_async = AsyncMock()
    controller = WatermarkController(WatermarkMode.SINK, flush_interval_batches=1)
    controller.bind(None, operator, None, None, flush_input, flush_input_async)

    assert controller.note_processed("source-1", 7) is True
    await controller.advance()

    flush_input_async.assert_awaited_once_with()
    flush_input.assert_not_called()
    operator.flush.assert_called_once_with()
    assert controller.forwarded_sequence_for("source-1") == 7


@pytest.mark.asyncio
async def test_watermark_pushes_channel_ack_to_upstream(monkeypatch) -> None:
    acknowledgements = []

    class Upstream:
        def acknowledge_delivery(self, edge_index, target_index, sequence):
            acknowledgements.append((edge_index, target_index, sequence))

    lookups = []
    monkeypatch.setattr(
        "ray.klein.runtime.worker.watermark.klein.get_actor_by_name",
        lambda name, namespace: lookups.append((name, namespace)) or Upstream(),
    )
    channel = DeliveryChannel("vertex", "source-task", 2, 3)
    controller = WatermarkController(WatermarkMode.SINK, 1, namespace="job-ns")
    controller.bind(None, MagicMock(), None, None, MagicMock())

    controller.note_processed(channel, 7)
    await controller.advance()

    assert lookups == [("source-task", "job-ns")]
    assert acknowledgements == [(2, 3, 7)]
    assert controller.forwarded_sequence_for(channel) == 7
