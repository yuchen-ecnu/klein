# SPDX-License-Identifier: Apache-2.0
"""The loop-side emit pipeline for a pipelined StreamTask.

A transform task processes records on its single executor thread and *buffers*
the resulting emit-ops in the collector. This pipeline drains those ops on the
actor event loop, one FIFO consumer task, so process(N+1) overlaps emit(N) while
ordering is preserved (data before the barrier that followed it).

Queue item kinds:
  * a private shutdown sentinel;
  * :class:`_WatermarkMarker`, after all output covered by that sequence;
  * buffered emit operations to drain via ``collector.aemit``.

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
    from ray.klein.api.collector import Collector
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
        collector: "Collector",
        watermark: "WatermarkController",
        on_fatal: Callable[[Exception], None],
        task_name: str,
        queue_maxsize: int = 2,
    ) -> None:
        self._collector = collector
        self._watermark = watermark
        self._on_fatal = on_fatal
        self._task_name = task_name
        self._queue: asyncio.Queue[object] = asyncio.Queue(maxsize=queue_maxsize)
        self._worker: asyncio.Task | None = None

    def start(self) -> None:
        """Launch the emit-worker task (idempotent)."""
        if self._worker is None:
            self._worker = asyncio.get_running_loop().create_task(self._loop(), name=f"{self._task_name}-emit")

    async def drain_pending(self) -> None:
        """Move the collector's buffered emit-ops onto the emit queue.

        Runs on the loop right after the executor returns (executor idle ->
        detach is race-free). Enqueue blocks when the bounded queue is full,
        which is the pipeline's backpressure.
        """
        if self._collector is None:
            return
        pending = self._collector.detach_pending()
        if pending:
            await self._queue.put(pending)

    async def enqueue_watermark(self, sender_vertex_id: object, sequence: int) -> None:
        """Enqueue a watermark marker BEHIND the ops already queued."""
        await self._queue.put(_WatermarkMarker(sender_vertex_id, sequence))

    async def enqueue_ops(self, ops: list) -> None:
        """Enqueue ready-made emit-ops (used for replay re-delivery)."""
        await self._queue.put(ops)

    async def _loop(self) -> None:
        while True:
            pending = await self._queue.get()
            try:
                if pending is _SHUTDOWN:
                    return
                if isinstance(pending, _WatermarkMarker):
                    self._watermark.apply_forwarded(pending.sender_vertex_id, pending.sequence)
                    continue
                if self._collector is not None:
                    await self._collector.aemit(pending)
            except Exception as error:
                # A fatal emit failure (e.g. downstream permanently gone) must
                # fail the task, not die silently in a detached worker.
                logger.exception("Emit worker of %s failed.", self._task_name)
                self._on_fatal(error)
                return
            finally:
                self._queue.task_done()

    async def shutdown(self, timeout: float) -> None:
        """Signal drain-complete and await the worker, dropping stragglers on timeout."""
        worker = self._worker
        if worker is None or worker.done():
            self._worker = None
            return
        try:
            await asyncio.wait_for(self._queue.put(_SHUTDOWN), timeout=timeout)
            await asyncio.wait_for(asyncio.shield(worker), timeout=timeout)
        except asyncio.CancelledError:
            await self._cancel_worker(worker)
            raise
        except asyncio.TimeoutError:
            stranded = self._queue.qsize()
            if self._collector is not None:
                stranded += len(self._collector.detach_pending())
            logger.warning(
                "Emit worker of %s did not drain in %.1fs; dropping ~%d buffered "
                "emit-op(s) on shutdown (at-least-once: downstream may miss these "
                "records).",
                self._task_name,
                timeout,
                stranded,
            )
            await self._cancel_worker(worker)
        except Exception:
            logger.exception("Emit worker of %s failed during shutdown", self._task_name)
        finally:
            if worker.done():
                self._worker = None

    @staticmethod
    async def _cancel_worker(worker: asyncio.Task) -> None:
        worker.cancel()
        with suppress(asyncio.CancelledError):
            await worker
