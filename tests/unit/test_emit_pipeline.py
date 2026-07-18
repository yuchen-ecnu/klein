# SPDX-License-Identifier: Apache-2.0

import asyncio
from contextlib import suppress
from unittest.mock import MagicMock

import pytest

from ray.klein.runtime.worker.emit_pipeline import EmitPipeline


class _BlockingCollector:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    def detach_pending(self) -> list:
        return []

    async def aemit(self, _pending) -> None:
        self.started.set()
        await self.release.wait()


@pytest.mark.asyncio
async def test_shutdown_timeout_cancels_emit_worker() -> None:
    collector = _BlockingCollector()
    pipeline = EmitPipeline(collector, MagicMock(), MagicMock(), "task")
    pipeline.start()
    await pipeline.enqueue_ops([MagicMock()])
    await collector.started.wait()

    await pipeline.shutdown(timeout=0.01)

    assert pipeline._worker is None


@pytest.mark.asyncio
async def test_cancelled_shutdown_does_not_leave_emit_worker_pending() -> None:
    collector = _BlockingCollector()
    pipeline = EmitPipeline(collector, MagicMock(), MagicMock(), "task")
    pipeline.start()
    await pipeline.enqueue_ops([MagicMock()])
    await collector.started.wait()

    shutdown = asyncio.create_task(pipeline.shutdown(timeout=30))
    await asyncio.sleep(0.01)
    shutdown.cancel()
    with suppress(asyncio.CancelledError):
        await shutdown

    assert pipeline._worker is None
