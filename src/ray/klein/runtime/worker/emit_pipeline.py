# SPDX-License-Identifier: Apache-2.0
"""The loop-side emit pipeline for a pipelined StreamTask.

A transform task processes records on its single executor thread and *buffers*
the resulting delivery commands in ``TaskOutput``. This pipeline drains them on the
actor event loop, one FIFO consumer task, so process(N+1) overlaps emit(N) while
ordering is preserved (data before the barrier that followed it).

Queue item kinds:
  * a private shutdown sentinel;
  * :class:`_WatermarkMarker`, after all output covered by that sequence;
    * immutable delivery commands drained via ``TaskOutput.send_commands``.

Sources don't use this (they emit inline from inside the blocking source loop);
only the pipelined path constructs an EmitPipeline.
"""

import asyncio
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from typing import TYPE_CHECKING

from ray.klein._internal.logging import get_logger

if TYPE_CHECKING:
    from ray.klein.runtime.collector.delivery_command import DeliveryCommand
    from ray.klein.runtime.collector.task_output import TaskOutput
    from ray.klein.runtime.worker.watermark import WatermarkController


logger = get_logger(__name__)
_SHUTDOWN = object()


@dataclass(frozen=True, slots=True)
class _WatermarkMarker:
    sender_vertex_id: object
    sequence: int


class EmitPipeline:
    """Owns the emit queue and the single FIFO emit-worker task."""

    def __init__(
        self,
        task_output: "TaskOutput | None",
        watermark: "WatermarkController",
        on_fatal: Callable[[Exception], None],
        task_name: str,
        queue_maxsize: int = 2,
        queue_size_observer: Callable[[int], None] | None = None,
    ) -> None:
        if queue_maxsize <= 0:
            raise ValueError("emit queue max batches must be greater than zero")
        self._task_output = task_output
        self._watermark = watermark
        self._on_fatal = on_fatal
        self._task_name = task_name
        self._queue: asyncio.Queue[object] = asyncio.Queue(maxsize=queue_maxsize)
        self._queue_size_observer = queue_size_observer
        self._worker: asyncio.Task | None = None

    def start(self) -> None:
        """Launch the emit-worker task (idempotent)."""
        if self._worker is None:
            self._worker = asyncio.get_running_loop().create_task(self._loop(), name=f"{self._task_name}-emit")

    async def drain_pending(self) -> None:
        """Move TaskOutput's buffered commands onto the emit queue.

        Runs on the loop right after the executor returns (executor idle ->
        detach is race-free). Enqueue blocks when the bounded queue is full,
        which is the pipeline's backpressure.
        """
        if self._task_output is None:
            return
        commands = self._task_output.take_pending_commands()
        if commands:
            await self._queue.put(commands)
            self._publish_queue_size()

    async def enqueue_watermark(self, sender_vertex_id: object, sequence: int) -> None:
        """Enqueue a watermark marker BEHIND the ops already queued."""
        await self._queue.put(_WatermarkMarker(sender_vertex_id, sequence))
        self._publish_queue_size()

    async def enqueue_watermarks(self, pending: dict[object, int]) -> None:
        """Enqueue one ordered durability marker for every pending sender."""
        for sender_vertex_id, sequence in pending.items():
            await self.enqueue_watermark(sender_vertex_id, sequence)

    async def enqueue_commands(self, commands: list["DeliveryCommand"]) -> None:
        """Enqueue ready-made commands, including replay re-delivery."""
        await self._queue.put(commands)
        self._publish_queue_size()

    async def wait_idle(self, timeout: float) -> None:
        """Wait until every command accepted before a topology cut is sent."""

        await self.drain_pending()
        try:
            await asyncio.wait_for(self._queue.join(), timeout=timeout)
        except asyncio.TimeoutError:
            raise TimeoutError(f"emit worker of {self._task_name} did not become idle within {timeout:.1f}s") from None
        if self._worker is not None and self._worker.done() and not self._worker.cancelled():
            error = self._worker.exception()
            if error is not None:
                raise error

    async def _loop(self) -> None:
        while True:
            pending = await self._queue.get()
            try:
                if pending is _SHUTDOWN:
                    return
                if isinstance(pending, _WatermarkMarker):
                    await self._watermark.apply_forwarded(pending.sender_vertex_id, pending.sequence)
                    continue
                if self._task_output is not None:
                    await self._task_output.send_commands(pending)
            except Exception as error:
                # A fatal emit failure (e.g. downstream permanently gone) must
                # fail the task, not die silently in a detached worker.
                logger.exception("Emit worker of %s failed.", self._task_name)
                self._on_fatal(error)
                return
            finally:
                self._queue.task_done()
                self._publish_queue_size()

    def _publish_queue_size(self) -> None:
        if self._queue_size_observer is not None:
            self._queue_size_observer(self._queue.qsize())

    async def shutdown(self, timeout: float) -> None:
        """Drain every queued operation or fail the task; never report silent loss."""
        worker = self._worker
        if worker is None or worker.done():
            self._worker = None
            return
        try:
            await asyncio.wait_for(self._queue.put(_SHUTDOWN), timeout=timeout)
            self._publish_queue_size()
            await asyncio.wait_for(asyncio.shield(worker), timeout=timeout)
        except asyncio.CancelledError:
            await self._cancel_worker(worker)
            raise
        except asyncio.TimeoutError:
            stranded = self._queue.qsize()
            if self._task_output is not None:
                stranded += len(self._task_output.take_pending_commands())
            await self._cancel_worker(worker)
            raise TimeoutError(
                f"emit worker of {self._task_name} did not drain ~{stranded} buffered "
                f"operation(s) within {timeout:.1f}s"
            ) from None
        except Exception:
            logger.exception("Emit worker of %s failed during shutdown", self._task_name)
            await self._cancel_worker(worker)
            raise
        finally:
            if worker.done():
                self._worker = None

    @staticmethod
    async def _cancel_worker(worker: asyncio.Task) -> None:
        worker.cancel()
        with suppress(asyncio.CancelledError):
            await worker
