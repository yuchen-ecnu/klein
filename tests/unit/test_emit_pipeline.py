# SPDX-License-Identifier: Apache-2.0

import asyncio
from contextlib import suppress
from unittest.mock import MagicMock

import pytest

from ray.klein.runtime.worker.emit_pipeline import EmitPipeline


class _BlockingTaskOutput:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    def take_pending_commands(self) -> list:
        return []

    async def send_commands(self, _pending) -> None:
        self.started.set()
        await self.release.wait()


@pytest.mark.asyncio
async def test_replay_waits_for_delivery() -> None:
    output = _BlockingTaskOutput()
    pipeline = EmitPipeline(output, MagicMock(), MagicMock(), "task")
    pipeline.start()
    replay = asyncio.create_task(pipeline.replay([MagicMock()]))
    await asyncio.wait_for(output.started.wait(), timeout=1)
    assert not replay.done()
    output.release.set()
    await asyncio.wait_for(replay, timeout=1)
    await pipeline.shutdown(timeout=1)


@pytest.mark.asyncio
async def test_replay_propagates_delivery_failure() -> None:
    class FailingOutput:
        async def send_commands(self, _pending):
            raise RuntimeError("downstream unavailable")

    fatal = MagicMock()
    pipeline = EmitPipeline(FailingOutput(), MagicMock(), fatal, "task")
    pipeline.start()
    with pytest.raises(RuntimeError, match="downstream unavailable"):
        await asyncio.wait_for(pipeline.replay([MagicMock()]), timeout=1)
    fatal.assert_called_once()
    await pipeline.shutdown(timeout=1)


@pytest.mark.asyncio
@pytest.mark.parametrize("queued_batches", [0, 1])
async def test_replay_does_not_hang_when_live_send_fails(queued_batches) -> None:
    class FailingOutput(_BlockingTaskOutput):
        async def send_commands(self, pending):
            await super().send_commands(pending)
            raise RuntimeError("live delivery failed")

    output = FailingOutput()
    pipeline = EmitPipeline(output, MagicMock(), MagicMock(), "task", queue_maxsize=1)
    pipeline.start()
    await pipeline.enqueue_commands([MagicMock()])
    await asyncio.wait_for(output.started.wait(), timeout=1)
    for _ in range(queued_batches):
        await pipeline.enqueue_commands([MagicMock()])
    replay = asyncio.create_task(pipeline.replay([MagicMock()]))
    await asyncio.sleep(0)
    output.release.set()
    with pytest.raises(RuntimeError, match="stopped before replay") as caught:
        await asyncio.wait_for(replay, timeout=1)
    assert str(caught.value.__cause__) == "live delivery failed"
    await pipeline.shutdown(timeout=1)


@pytest.mark.asyncio
async def test_replay_requires_a_running_emit_worker() -> None:
    pipeline = EmitPipeline(_BlockingTaskOutput(), MagicMock(), MagicMock(), "task")
    with pytest.raises(RuntimeError, match="not running"):
        await pipeline.replay([MagicMock()])


@pytest.mark.asyncio
async def test_shutdown_timeout_cancels_emit_worker() -> None:
    output = _BlockingTaskOutput()
    pipeline = EmitPipeline(output, MagicMock(), MagicMock(), "task")
    pipeline.start()
    await pipeline.enqueue_commands([MagicMock()])
    await output.started.wait()

    with pytest.raises(TimeoutError, match="did not drain"):
        await pipeline.shutdown(timeout=0.01)

    assert pipeline._worker is None


@pytest.mark.asyncio
async def test_cancelled_shutdown_does_not_leave_emit_worker_pending() -> None:
    output = _BlockingTaskOutput()
    pipeline = EmitPipeline(output, MagicMock(), MagicMock(), "task")
    pipeline.start()
    await pipeline.enqueue_commands([MagicMock()])
    await output.started.wait()

    shutdown = asyncio.create_task(pipeline.shutdown(timeout=30))
    await asyncio.sleep(0.01)
    shutdown.cancel()
    with suppress(asyncio.CancelledError):
        await shutdown

    assert pipeline._worker is None
