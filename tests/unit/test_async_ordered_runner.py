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
async def test_fatal_error_cancels_queued_computes_and_unblocks_barrier():
    fatal = asyncio.Event()
    release_failure = asyncio.Event()
    never = asyncio.Event()
    pending_started = {index: asyncio.Event() for index in (1, 2)}
    cancelled = []

    async def boom():
        await release_failure.wait()
        raise RuntimeError("compute failed")

    async def pending(index):
        try:
            pending_started[index].set()
            await never.wait()
            return [_rec(index)]
        finally:
            cancelled.append(index)

    runner = AsyncOrderedRunner(
        capacity=3,
        on_result=lambda recs: _noop(),
        on_fatal=lambda error: fatal.set(),
        task_name="t",
    )
    runner.start()
    await runner.submit_compute(boom())
    await runner.submit_compute(pending(1))
    await runner.submit_compute(pending(2))

    # A task cancelled before its first event-loop step never enters the
    # coroutine body, so its ``finally`` block cannot observe cancellation.
    # Synchronize explicitly to test cancellation of genuinely in-flight work
    # instead of relying on version-dependent asyncio scheduling order.
    await asyncio.wait_for(
        asyncio.gather(*(started.wait() for started in pending_started.values())),
        timeout=1,
    )

    release_failure.set()
    await asyncio.wait_for(fatal.wait(), timeout=1)
    with pytest.raises(RuntimeError, match=r"runner.*failed"):
        await asyncio.wait_for(runner.barrier(), timeout=1)
    await runner.shutdown(timeout=1)

    assert sorted(cancelled) == [1, 2]


@pytest.mark.asyncio
async def test_fatal_error_releases_blocked_submitter():
    fatal = asyncio.Event()
    release_failure = asyncio.Event()

    async def boom():
        await release_failure.wait()
        raise RuntimeError("compute failed")

    async def never_started():
        await asyncio.Event().wait()
        return []

    runner = AsyncOrderedRunner(
        capacity=1,
        on_result=lambda recs: _noop(),
        on_fatal=lambda error: fatal.set(),
        task_name="t",
    )
    runner.start()
    await runner.submit_compute(boom())
    blocked_submit = asyncio.create_task(runner.submit_compute(never_started()))
    await asyncio.sleep(0)
    assert not blocked_submit.done()

    release_failure.set()
    await asyncio.wait_for(fatal.wait(), timeout=1)
    with pytest.raises(RuntimeError, match=r"runner.*failed"):
        await asyncio.wait_for(blocked_submit, timeout=1)
    assert runner._slots._value == 1
    await runner.shutdown(timeout=1)


@pytest.mark.asyncio
async def test_cancel_during_slot_waiter_cleanup_releases_slot_and_coroutine():
    cleanup_entered = asyncio.Event()
    runner = AsyncOrderedRunner(
        capacity=1,
        on_result=lambda recs: _noop(),
        on_fatal=lambda error: None,
        task_name="t",
    )
    runner.start()
    settle_waiters = runner._settle_waiters
    call_count = 0

    async def pause_first_cleanup(*waiters):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            cleanup_entered.set()
            await asyncio.Event().wait()
        await settle_waiters(*waiters)

    runner._settle_waiters = pause_first_cleanup
    coroutine = _wait_forever()
    submitter = asyncio.create_task(runner.submit_compute(coroutine))
    await asyncio.wait_for(cleanup_entered.wait(), timeout=1)
    submitter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await submitter

    assert coroutine.cr_frame is None
    await asyncio.wait_for(runner.submit_compute(_return_empty()), timeout=1)
    await asyncio.wait_for(runner.barrier(), timeout=1)
    await runner.shutdown(timeout=1)


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


@pytest.mark.asyncio
async def test_shutdown_timeout_remains_bounded_when_compute_swallows_cancellation():
    started = asyncio.Event()
    cancelled = asyncio.Event()
    release = asyncio.Event()
    emitted = []

    async def stubborn_compute():
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            await release.wait()
        return []

    runner = AsyncOrderedRunner(
        capacity=1,
        on_result=lambda recs: _append(emitted, recs),
        on_fatal=lambda error: None,
        task_name="t",
    )
    runner.start()
    await runner.submit_compute(stubborn_compute())
    await asyncio.wait_for(started.wait(), timeout=1)
    blocked_submit = asyncio.create_task(runner.submit_compute(_wait_forever()))
    await asyncio.sleep(0)
    assert not blocked_submit.done()

    try:
        await asyncio.wait_for(runner.shutdown(timeout=0.01), timeout=0.2)
        await asyncio.wait_for(cancelled.wait(), timeout=1)
        with pytest.raises(RuntimeError, match="shutting down"):
            await asyncio.wait_for(blocked_submit, timeout=0.2)
    finally:
        release.set()
        if runner._consumer is not None:
            await asyncio.wait_for(asyncio.gather(runner._consumer, return_exceptions=True), timeout=1)
    assert emitted == []


@pytest.mark.asyncio
async def test_shutdown_called_by_consumer_still_stops_consumer():
    shutdown_returned = asyncio.Event()
    runner = AsyncOrderedRunner(
        capacity=1,
        on_result=lambda recs: _noop(),
        on_fatal=lambda error: None,
        task_name="t",
    )
    runner.start()

    async def shutdown_from_control():
        await runner.shutdown(timeout=1)
        shutdown_returned.set()

    await runner.submit_control(shutdown_from_control)
    await asyncio.wait_for(shutdown_returned.wait(), timeout=1)
    assert runner._consumer is not None
    await asyncio.wait_for(runner._consumer, timeout=1)
    assert runner._consumer.done()


# --- helpers (on_result must be async) ---


async def _append(sink, recs):
    sink.extend(recs)


async def _append_tagged(sink, recs):
    for r in recs:
        sink.append(f"data:{r.block['v']}")


async def _noop():
    return None


async def _wait_forever():
    await asyncio.Event().wait()
    return []


async def _return_empty():
    return []
