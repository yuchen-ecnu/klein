# SPDX-License-Identifier: Apache-2.0
"""Ordered async execution window for an async StreamTask operator.

Mirrors Flink's ``AsyncWaitOperator`` (ORDERED mode) on Ray's async-actor event
loop: a bounded number of requests run concurrently, but results are emitted in
input order.

Why this exists
---------------
``OperatorRunner.process_async`` only *computes* a record's output and returns
it — it never touches the shared emit buffer. That split (Flink's
``AsyncFunction`` returning via ``ResultFuture`` rather than emitting inline) is
what lets us have many requests in flight at once without racing the collector:

* **producer** — pulls records the batcher flushed, starts one ``process_async``
  coroutine per record *without awaiting it* (so the event loop runs them
  concurrently), and enqueues the resulting future onto a bounded FIFO queue.
  The queue's ``maxsize`` is ``async_buffer_size``; a full queue suspends the
  producer, which is the in-flight cap + backpressure (Flink's ``capacity``).
* **consumer** — pops futures in FIFO order, awaits each, and emits its records
  via the (single-threaded, race-free) emit path. FIFO await == input order, so
  emission is ordered even though compute finished out of order.

The FIFO queue carries two kinds of work:

* **compute** — a coroutine already scheduled with ``ensure_future`` (so it runs
  concurrently with other in-flight computes); when the consumer reaches it,
  it awaits the result and hands it to ``on_result``.
* **control** — an in-order async thunk (barrier alignment, per-envelope
  watermark/eof bookkeeping). Because it sits in the same FIFO queue, it runs
  only after every preceding compute has been awaited and emitted — so a barrier
  snapshots at the correct stream position and at-least-once holds without
  stalling the in-flight window.

The component owns only the producer→queue handoff and the consumer task; all
actual work (compute, collect, barrier handling, watermark) is delegated back to
the StreamTask/pump via the submitted coroutines/thunks, so this stays a pure
orchestration layer.
"""

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from ray.klein._internal.logging import get_logger
from ray.klein.runtime.message import Record

_SHUTDOWN = object()


logger = get_logger(__name__)


@dataclass(frozen=True, slots=True)
class _Compute:
    future: asyncio.Future[list[Record]]


@dataclass(frozen=True, slots=True)
class _Control:
    action: Callable[[], Awaitable[None]]


class AsyncOrderedRunner:
    """Bounded, order-preserving concurrency window for an async operator."""

    def __init__(
        self,
        capacity: int,
        on_result: Callable[[list[Record]], Awaitable[None]],
        on_fatal: Callable[[Exception], None],
        task_name: str,
    ) -> None:
        # capacity == async_buffer_size: at most this many compute requests are
        # outstanding at once. A semaphore (not the queue's maxsize) is the real
        # gate: the compute is started with ensure_future *before* it reaches the
        # queue, so bounding the queue alone would let extra requests start. A
        # slot is held from just-before-start until the consumer has emitted that
        # request's result — so buffered-but-not-yet-emitted results count toward
        # the cap too, matching Flink's `capacity` semantics.
        self._capacity = max(1, capacity)
        self._slots = asyncio.Semaphore(self._capacity)
        # on_result(records): emit a finished compute's records, in FIFO order.
        self._on_result = on_result
        self._on_fatal = on_fatal
        self._task_name = task_name
        # Unbounded: the semaphore bounds outstanding computes; control items are
        # rare (one per barrier/envelope) and must never block on a full queue.
        self._queue: asyncio.Queue[object] = asyncio.Queue()
        self._consumer: asyncio.Task | None = None

    def start(self) -> None:
        """Launch the FIFO consumer task (idempotent)."""
        if self._consumer is None:
            self._consumer = asyncio.get_running_loop().create_task(
                self._consume(), name=f"{self._task_name}-async-consumer"
            )

    async def submit_compute(self, coro: Awaitable[list[Record]]) -> None:
        """Acquire a slot, schedule ``coro`` (concurrent), and enqueue it (FIFO).

        ``acquire`` blocks once ``capacity`` requests are outstanding — the
        in-flight cap and backpressure (Flink's ``capacity``). Once a slot is
        held the compute starts running immediately (``ensure_future``) so
        multiple run at once; the consumer awaits it in submit order, emits the
        result, then releases the slot.
        """
        await self._slots.acquire()
        try:
            future = asyncio.ensure_future(coro)
        except BaseException:
            self._slots.release()
            raise
        await self._queue.put(_Compute(future))

    async def submit_control(self, thunk: Callable[[], Awaitable[None]]) -> None:
        """Enqueue an in-order async action (barrier / per-envelope bookkeeping).

        Runs only after every preceding compute has been awaited and emitted,
        so control actions observe the correct stream position.
        """
        await self._queue.put(_Control(thunk))

    async def _consume(self) -> None:
        while True:
            item = await self._queue.get()
            is_compute = isinstance(item, _Compute)
            try:
                if item is _SHUTDOWN:
                    return
                if isinstance(item, _Compute):
                    records = await item.future
                    await self._on_result(records)
                elif isinstance(item, _Control):
                    await item.action()
                else:
                    raise TypeError(f"Unsupported async runner item: {type(item).__name__}")
            except Exception as error:
                # A compute that raised past the UDF-exception policy, or an emit
                # failure, is fatal: fail the task rather than die silently in a
                # detached consumer.
                logger.exception("Async ordered consumer of %s failed.", self._task_name)
                self._on_fatal(error)
                return
            finally:
                # Release the in-flight slot once a compute's result has been
                # emitted (or it failed) — buffered results hold their slot until
                # here, so the cap covers outstanding-but-unemitted requests too.
                if is_compute:
                    self._slots.release()
                self._queue.task_done()

    async def barrier(self) -> None:
        """Block until everything currently queued has been consumed + emitted.

        The producer calls this after submitting an envelope's work so the pump
        loop can run the eof check + stop (which must not run inside the
        consumer) only once this envelope's output has fully flowed through.
        """
        await self._queue.join()

    async def shutdown(self, timeout: float) -> None:
        """Drain everything queued, then stop the consumer cleanly.

        Enqueues a sentinel behind all pending work so the consumer finishes
        emitting in-flight results before returning. Cancels stragglers if they
        exceed ``timeout``.
        """
        if self._consumer is None or self._consumer.done():
            return
        try:
            await self._queue.put(_SHUTDOWN)
            await asyncio.wait_for(asyncio.shield(self._consumer), timeout=timeout)
        except asyncio.TimeoutError:
            self._consumer.cancel()
            logger.warning(
                "Async ordered consumer of %s did not drain in %.1fs; cancelling "
                "(at-least-once: in-flight requests may be replayed on recovery).",
                self._task_name,
                timeout,
            )
        except Exception:
            logger.exception("Async ordered consumer of %s failed during shutdown", self._task_name)
