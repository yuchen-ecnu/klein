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
what lets us have many requests in flight at once without racing TaskOutput:

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
import inspect
from collections.abc import Awaitable, Callable
from contextlib import suppress
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
        self._accepting = True
        self._terminal_event = asyncio.Event()
        self._shutdown_requested = False
        self._abort_requested = False
        self._fatal_error: Exception | None = None
        # Queued compute tasks are cancelled synchronously when the consumer
        # fails, but asyncio may run their cancellation handlers on a later
        # loop turn. Keep them reachable so shutdown can settle that cleanup
        # within its existing time budget instead of returning with owned
        # tasks still pending.
        self._cancelling_futures: set[asyncio.Future] = set()

    def start(self) -> None:
        """Launch the FIFO consumer task (idempotent)."""
        if not self._accepting:
            raise RuntimeError(f"Async ordered runner for {self._task_name} is closed")
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
        if not self._accepting:
            self._discard_awaitable(coro)
            self._raise_terminal()
        acquired = False
        slot_state_known = False
        coro_owned = True
        future: asyncio.Future[list[Record]] | None = None
        slot_waiter = asyncio.create_task(self._slots.acquire())
        terminal_waiter = asyncio.create_task(self._terminal_event.wait())
        try:
            done, pending = await asyncio.wait(
                (slot_waiter, terminal_waiter),
                return_when=asyncio.FIRST_COMPLETED,
            )
            acquired = slot_waiter in done and not slot_waiter.cancelled() and slot_waiter.exception() is None
            slot_state_known = True
            for waiter in pending:
                waiter.cancel()
            await self._settle_waiters(*pending)
            if not acquired or not self._accepting:
                if acquired:
                    self._slots.release()
                    acquired = False
                self._discard_awaitable(coro)
                coro_owned = False
                self._raise_terminal()
            future = asyncio.ensure_future(coro)
            coro_owned = False
            self._queue.put_nowait(_Compute(future))
            future = None
            acquired = False
        except BaseException:
            slot_waiter.cancel()
            terminal_waiter.cancel()
            await self._settle_waiters(slot_waiter, terminal_waiter)
            if not slot_state_known:
                acquired = slot_waiter.done() and not slot_waiter.cancelled() and slot_waiter.exception() is None
            if future is not None:
                self._cancel_future(future)
            if acquired:
                self._slots.release()
            if coro_owned:
                self._discard_awaitable(coro)
            raise

    @staticmethod
    async def _settle_waiters(*waiters: asyncio.Future) -> None:
        if waiters:
            await asyncio.gather(*waiters, return_exceptions=True)

    async def submit_control(self, thunk: Callable[[], Awaitable[None]]) -> None:
        """Enqueue an in-order async action (barrier / per-envelope bookkeeping).

        Runs only after every preceding compute has been awaited and emitted,
        so control actions observe the correct stream position.
        """
        if not self._accepting:
            self._raise_terminal()
        self._queue.put_nowait(_Control(thunk))

    async def _consume(self) -> None:
        try:
            while True:
                if self._abort_requested:
                    return
                item = await self._queue.get()
                is_compute = isinstance(item, _Compute)
                try:
                    if not await self._consume_item(item):
                        return
                except asyncio.CancelledError:
                    if not self._accepting:
                        raise
                    self._fail(RuntimeError(f"Async compute of {self._task_name} was cancelled unexpectedly"))
                    return
                except Exception as error:
                    # A compute that raised past the UDF-exception policy, or an emit
                    # failure, is fatal: fail the task rather than die silently in a
                    # detached consumer.
                    self._fail(error)
                    return
                finally:
                    # Release the in-flight slot once a compute's result has been
                    # emitted (or it failed) — buffered results hold their slot until
                    # here, so the cap covers outstanding-but-unemitted requests too.
                    if is_compute:
                        self._slots.release()
                    self._queue.task_done()
        finally:
            self._seal()
            self._cancel_queued()

    async def _consume_item(self, item: object) -> bool:
        if item is _SHUTDOWN:
            return False
        if isinstance(item, _Compute):
            records = await item.future
            if self._abort_requested:
                return False
            await self._on_result(records)
            return True
        if isinstance(item, _Control):
            await item.action()
            return not self._abort_requested
        raise TypeError(f"Unsupported async runner item: {type(item).__name__}")

    def _fail(self, error: Exception) -> None:
        if self._fatal_error is not None:
            return
        self._fatal_error = error
        self._seal()
        logger.error(
            "Async ordered consumer of %s failed.",
            self._task_name,
            exc_info=(type(error), error, error.__traceback__),
        )
        try:
            self._on_fatal(error)
        except Exception:
            logger.exception("Fatal callback of async ordered consumer %s failed.", self._task_name)

    def _cancel_queued(self, *, keep_shutdown: bool = False) -> None:
        restore_shutdown = False
        while True:
            try:
                item = self._queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            try:
                if isinstance(item, _Compute):
                    self._cancel_future(item.future)
                    self._slots.release()
                elif item is _SHUTDOWN and keep_shutdown:
                    restore_shutdown = True
            finally:
                self._queue.task_done()
        if restore_shutdown:
            self._queue.put_nowait(_SHUTDOWN)

    @staticmethod
    def _silence_future(future: asyncio.Future) -> None:
        with suppress(BaseException):
            future.exception()

    def _cancel_future(self, future: asyncio.Future) -> None:
        self._cancelling_futures.add(future)
        future.cancel()
        future.add_done_callback(self._finish_cancelled_future)

    def _finish_cancelled_future(self, future: asyncio.Future) -> None:
        self._silence_future(future)
        self._cancelling_futures.discard(future)

    async def _settle_cancelled_futures(self, timeout: float) -> None:
        pending = tuple(self._cancelling_futures)
        if not pending or timeout <= 0:
            return
        # ``asyncio.wait`` observes completion without issuing another cancel
        # when the budget expires; a cancellation-resistant user coroutine
        # therefore cannot make shutdown exceed its documented bound.
        await asyncio.wait(pending, timeout=timeout)

    @staticmethod
    def _discard_awaitable(awaitable: Awaitable[list[Record]]) -> None:
        if isinstance(awaitable, asyncio.Future):
            awaitable.cancel()
        elif inspect.iscoroutine(awaitable):
            awaitable.close()
        else:
            future = asyncio.ensure_future(awaitable)
            future.cancel()

    def _raise_terminal(self) -> None:
        if self._fatal_error is not None:
            raise RuntimeError(f"Async ordered runner for {self._task_name} failed") from self._fatal_error
        raise RuntimeError(f"Async ordered runner for {self._task_name} is shutting down")

    def _seal(self) -> None:
        self._accepting = False
        self._terminal_event.set()

    async def barrier(self) -> None:
        """Block until everything currently queued has been consumed + emitted.

        The producer calls this after submitting an envelope's work so the pump
        loop can run the eof check + stop (which must not run inside the
        consumer) only once this envelope's output has fully flowed through.
        """
        await self._queue.join()
        if self._fatal_error is not None:
            raise RuntimeError(f"Async ordered runner for {self._task_name} failed") from self._fatal_error

    async def shutdown(self, timeout: float) -> None:
        """Drain everything queued, then stop the consumer cleanly.

        Enqueues a sentinel behind all pending work so the consumer finishes
        emitting in-flight results before returning. Cancels stragglers if they
        exceed ``timeout``.
        """
        self._seal()
        loop = asyncio.get_running_loop()
        deadline = loop.time() + max(0.0, timeout)
        consumer = self._consumer
        if consumer is None:
            self._cancel_queued()
            await self._settle_cancelled_futures(max(0.0, deadline - loop.time()))
            return
        if consumer.done():
            self._cancel_queued()
            await self._settle_cancelled_futures(max(0.0, deadline - loop.time()))
            return
        if not self._shutdown_requested:
            self._shutdown_requested = True
            self._queue.put_nowait(_SHUTDOWN)
        if asyncio.current_task() is consumer:
            return
        timed_out = False
        try:
            await asyncio.wait_for(asyncio.shield(consumer), timeout=timeout)
        except asyncio.TimeoutError:
            timed_out = True
            self._abort_requested = True
            consumer.cancel()
            consumer.add_done_callback(self._silence_future)
            logger.warning(
                "Async ordered consumer of %s did not drain in %.1fs; cancelling "
                "(at-least-once: in-flight requests may be replayed on recovery).",
                self._task_name,
                timeout,
            )
        except Exception:
            logger.exception("Async ordered consumer of %s failed during shutdown", self._task_name)
        finally:
            self._cancel_queued(keep_shutdown=timed_out and not consumer.done())
            await self._settle_cancelled_futures(max(0.0, deadline - loop.time()))
