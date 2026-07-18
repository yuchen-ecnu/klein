# SPDX-License-Identifier: Apache-2.0
"""Tests for the asyncio worker lifecycle without Ray."""

import asyncio

import pytest

from ray.klein.runtime.worker.async_worker import AsyncWorker


class _TickWorker(AsyncWorker):
    """Worker that increments a counter each iteration and sleeps briefly."""

    def __init__(self, sleep: float = 0.01):
        super().__init__()
        self.ticks = 0
        self.sleep = sleep

    async def _run(self) -> None:
        self.ticks += 1
        await asyncio.sleep(self.sleep)

    def _get_name(self) -> str:
        return "tick"


class _RaiseOnceWorker(AsyncWorker):
    """Worker that raises the first time _run is called."""

    def __init__(self):
        super().__init__()
        self.calls = 0
        self.captured_exc = None

    async def _run(self) -> None:
        self.calls += 1
        raise RuntimeError("boom")

    def _get_name(self) -> str:
        return "raise-once"

    def handle_exception(self, exc: Exception) -> None:
        self.captured_exc = exc


class _SlowStopWorker(AsyncWorker):
    """Worker whose _run is a long sleep — exercises cancellation path."""

    def __init__(self):
        super().__init__()
        self.entered = asyncio.Event()

    async def _run(self) -> None:
        self.entered.set()
        await asyncio.sleep(10)  # would block forever without cancel()

    def _get_name(self) -> str:
        return "slow"


@pytest.mark.asyncio
async def test_start_and_stop_idempotent():
    w = _TickWorker(sleep=0.005)
    await w.start()
    await w.start()  # idempotent — must not raise or replace the task
    await asyncio.sleep(0.05)
    assert w.healthy
    assert w.ticks >= 1
    await w.stop()
    assert not w.healthy
    await w.stop()  # second stop is a no-op


@pytest.mark.asyncio
async def test_stop_cancels_blocking_run():
    w = _SlowStopWorker()
    await w.start()
    await w.entered.wait()  # confirm _run is parked in sleep(10)
    await w.stop(timeout=1.0)  # must not wait 10s
    assert not w.healthy


@pytest.mark.asyncio
async def test_run_exception_routes_to_handle_exception():
    w = _RaiseOnceWorker()
    await w.start()
    # Wait for the task to finish (it dies on first _run).
    await asyncio.wait_for(asyncio.shield(w._task), timeout=1.0)
    assert w.calls == 1
    assert isinstance(w.captured_exc, RuntimeError)
    assert str(w.captured_exc) == "boom"
    assert not w.healthy


class _SelfStopWorker(AsyncWorker):
    """Worker that stops itself from inside _run."""

    def __init__(self):
        super().__init__()
        self.ticks = 0

    async def _run(self) -> None:
        self.ticks += 1
        # Trigger self-stop on the third tick.
        if self.ticks == 3:
            await self.stop()
            return
        await asyncio.sleep(0.001)

    def _get_name(self) -> str:
        return "self-stop"


@pytest.mark.asyncio
async def test_self_stop_does_not_deadlock():
    w = _SelfStopWorker()
    await w.start()
    # If self-stop tries to cancel + join itself we'd hang here forever.
    await asyncio.wait_for(asyncio.shield(w._task), timeout=1.0)
    assert w.ticks == 3
    assert not w.healthy


@pytest.mark.asyncio
async def test_loop_iterates_many_times():
    w = _TickWorker(sleep=0.001)
    await w.start()
    await asyncio.sleep(0.05)
    await w.stop()
    assert w.ticks >= 5
