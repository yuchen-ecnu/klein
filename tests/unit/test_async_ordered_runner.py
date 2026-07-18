# SPDX-License-Identifier: Apache-2.0
"""Tests for AsyncOrderedRunner — pure asyncio, no Ray required.

Covers the properties that matter for the serve proxy's async path:
concurrency (multiple requests in flight), the in-flight cap, ordered emission,
control-action ordering relative to data, and fatal-error propagation.
"""

import asyncio

import pytest

from ray.klein.runtime.message import Record
from ray.klein.runtime.worker.async_ordered_runner import AsyncOrderedRunner


def _rec(v):
    return Record({"v": v})


@pytest.mark.asyncio
async def test_requests_run_concurrently():
    """N slow computes should overlap: wall time ≈ one delay, not N delays."""
    emitted = []
    runner = AsyncOrderedRunner(
        capacity=8,
        on_result=lambda recs: _append(emitted, recs),
        on_fatal=lambda e: None,
        task_name="t",
    )
    runner.start()

    delay = 0.05
    n = 6

    async def compute(i):
        await asyncio.sleep(delay)
        return [_rec(i)]

    loop = asyncio.get_running_loop()
    t0 = loop.time()
    for i in range(n):
        await runner.submit_compute(compute(i))
    await runner.barrier()
    elapsed = loop.time() - t0

    # Serial would be n*delay; concurrent should be ~delay (allow scheduling slack).
    assert elapsed < delay * (n / 2), f"not concurrent: {elapsed:.3f}s for {n} x {delay}s"
    assert [r.block["v"] for r in emitted] == list(range(n))


@pytest.mark.asyncio
async def test_capacity_caps_in_flight():
    """No more than `capacity` computes are running at once."""
    capacity = 3
    in_flight = 0
    peak = 0
    gate = asyncio.Event()

    async def compute(i):
        nonlocal in_flight, peak
        in_flight += 1
        peak = max(peak, in_flight)
        await gate.wait()  # hold every request open until released
        in_flight -= 1
        return [_rec(i)]

    runner = AsyncOrderedRunner(
        capacity=capacity,
        on_result=lambda recs: _noop(),
        on_fatal=lambda e: None,
        task_name="t",
    )
    runner.start()

    async def feed():
        for i in range(10):
            await runner.submit_compute(compute(i))

    feeder = asyncio.ensure_future(feed())
    await asyncio.sleep(0.05)  # let the window fill
    assert peak <= capacity, f"peak in-flight {peak} exceeded capacity {capacity}"
    # The feeder should be blocked on a full queue, not done.
    assert not feeder.done()
    gate.set()
    await feeder
    await runner.barrier()


@pytest.mark.asyncio
async def test_emission_is_ordered_despite_out_of_order_completion():
    """Later-submitted faster computes must still emit after earlier ones."""
    emitted = []
    runner = AsyncOrderedRunner(
        capacity=8,
        on_result=lambda recs: _append(emitted, recs),
        on_fatal=lambda e: None,
        task_name="t",
    )
    runner.start()

    # First request is slow, rest are instant — completion order is reversed.
    async def compute(i):
        await asyncio.sleep(0.05 if i == 0 else 0.0)
        return [_rec(i)]

    for i in range(5):
        await runner.submit_compute(compute(i))
    await runner.barrier()
    assert [r.block["v"] for r in emitted] == [0, 1, 2, 3, 4]


@pytest.mark.asyncio
async def test_control_runs_after_preceding_data():
    """A control action sees all preceding data already emitted (barrier order)."""
    events = []
    runner = AsyncOrderedRunner(
        capacity=8,
        on_result=lambda recs: _append_tagged(events, recs),
        on_fatal=lambda e: None,
        task_name="t",
    )
    runner.start()

    async def compute(i):
        await asyncio.sleep(0.02 if i < 2 else 0.0)
        return [_rec(i)]

    async def control():
        events.append("control")

    await runner.submit_compute(compute(0))
    await runner.submit_compute(compute(1))
    await runner.submit_control(control)
    await runner.submit_compute(compute(2))
    await runner.barrier()

    # control must appear after data 0 and 1, before data 2.
    assert events == ["data:0", "data:1", "control", "data:2"]


@pytest.mark.asyncio
async def test_fatal_error_propagates():
    """A compute raising past the consumer fails via on_fatal."""
    captured = []
    runner = AsyncOrderedRunner(
        capacity=4,
        on_result=lambda recs: _noop(),
        on_fatal=lambda e: captured.append(e),
        task_name="t",
    )
    runner.start()

    async def boom():
        raise RuntimeError("compute failed")

    await runner.submit_compute(boom())
    await asyncio.sleep(0.02)
    assert len(captured) == 1
    assert isinstance(captured[0], RuntimeError)
    assert str(captured[0]) == "compute failed"


@pytest.mark.asyncio
async def test_shutdown_drains_pending():
    """shutdown() emits everything already queued before returning."""
    emitted = []
    runner = AsyncOrderedRunner(
        capacity=8,
        on_result=lambda recs: _append(emitted, recs),
        on_fatal=lambda e: None,
        task_name="t",
    )
    runner.start()

    async def compute(i):
        await asyncio.sleep(0.01)
        return [_rec(i)]

    for i in range(4):
        await runner.submit_compute(compute(i))
    await runner.shutdown(timeout=2.0)
    assert [r.block["v"] for r in emitted] == [0, 1, 2, 3]


# --- helpers (on_result must be async) ---


async def _append(sink, recs):
    sink.extend(recs)


async def _append_tagged(sink, recs):
    for r in recs:
        sink.append(f"data:{r.block['v']}")


async def _noop():
    return None
