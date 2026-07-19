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
