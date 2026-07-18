# SPDX-License-Identifier: Apache-2.0
"""Replay-watermark control for a StreamTask (single-fault at-least-once).

The watermark protocol:

* Each upstream sends records tagged with its ``(sender_vertex_id, sequence)``. After this
  task forwards a batch onward, it must tell that upstream "I've durably forwarded
  your output through ``sequence``" so the upstream can drop those records from its
  replay buffer. That per-sender high-water mark is :attr:`_forwarded_to`, read
  back on every ``put`` acknowledgement as ``PutAck.forwarded_sequence``.

* The mark may only advance once this task's *own* output has physically left it:

  - PIPELINED (transform): flush the collector, drain buffered emit-ops, then
    enqueue a watermark marker on the same emit queue. The emit
    loop applies it (via :meth:`apply_forwarded`) only after every op ahead of it
    has been ``put`` downstream — so the mark can't run ahead of the data.
  - SINK (terminal): no collector; flush the sink operator (output is durable on
    write-out) and advance the mark directly on the loop.
  - DISABLED: replay off — advancing is a no-op.

All state here is touched only on the actor event loop (the pump task, the emit
loop task, and ``put``), never the executor thread, so no lock is needed.
"""

import asyncio
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ray.klein.runtime.worker.emit_pipeline import EmitPipeline


class WatermarkMode(Enum):
    """Specifies the watermark generation mode."""

    DISABLED = "disabled"  # replay off
    PIPELINED = "pipelined"  # transform: flush collector + emit-queue marker
    SINK = "sink"  # terminal: flush operator + advance directly


class WatermarkController:
    """Owns the replay-watermark state and the per-mode advance policy."""

    def __init__(self, mode: WatermarkMode, flush_interval_batches: int) -> None:
        self._mode = mode
        self._flush_interval_batches = max(1, flush_interval_batches)
        # Per-sender high-water mark, reported upstream on each put acknowledgement
        # so the upstream can truncate its replay buffer.
        self._forwarded_to: dict[object, int] = {}
        # Last data batch processed: (sender, sequence). None until the first record.
        self._last_processed: tuple[object, int] | None = None
        self._batches_since_flush: int = 0
        # Set after construction (the emit pipeline is built alongside this).
        self._emit: EmitPipeline | None = None
        # Runtime handles for the flush, set via bind().
        self._collector = None
        self._operator = None
        self._executor = None

    @property
    def active(self) -> bool:
        return self._mode is not WatermarkMode.DISABLED

    def bind(self, collector, operator, executor, emit_pipeline) -> None:
        """Wire the runtime collaborators (built together in setup_and_run)."""
        self._collector = collector
        self._operator = operator
        self._executor = executor
        self._emit = emit_pipeline

    # --- read side (called by StreamTask.put) ---

    def forwarded_sequence_for(self, sender_vertex_id: object) -> int:
        """The sequence durably forwarded for ``sender_vertex_id``, or -1."""
        if sender_vertex_id is None:
            return -1
        return self._forwarded_to.get(sender_vertex_id, -1)

    # --- write side (called by the emit loop on a ("wm", ...) marker) ---

    def apply_forwarded(self, sender_vertex_id: object, sequence: int) -> None:
        """Advance the forwarded mark for one sender (monotonic)."""
        self._forwarded_to[sender_vertex_id] = max(
            self._forwarded_to.get(sender_vertex_id, -1),
            sequence,
        )

    # --- pump-driven progress ---

    def note_processed(self, sender_vertex_id: object | None, sequence: int | None) -> bool:
        """Record a processed data batch; return True if a flush is now due.

        Called by the pump after each non-barrier batch. A barrier (sender None)
        never sets last_processed.
        """
        if sender_vertex_id is None or sequence is None:
            return False
        self._last_processed = (sender_vertex_id, sequence)
        self._batches_since_flush += 1
        return self._batches_since_flush >= self._flush_interval_batches

    async def advance(self) -> None:
        """Flush output and publish the watermark for the last processed batch.

        No-op when replay is off or nothing has been processed yet.
        """
        if not self.active or self._last_processed is None:
            self._batches_since_flush = 0
            return
        sender_vertex_id, sequence = self._last_processed
        await self._do_advance(sender_vertex_id, sequence)
        self._batches_since_flush = 0

    async def _do_advance(self, sender_vertex_id: object, sequence: int) -> None:
        loop = asyncio.get_running_loop()
        if self._mode is WatermarkMode.PIPELINED:
            # Flush the collector's buffered micro-batches on the executor, drain
            # them onto the emit queue, then enqueue the watermark marker BEHIND
            # them so it's applied only after they've been put downstream.
            await loop.run_in_executor(self._executor, self._flush_collector)
            await self._emit.drain_pending()
            await self._emit.enqueue_watermark(sender_vertex_id, sequence)
        elif self._mode is WatermarkMode.SINK:
            # Sink output is durable after the operator's write-out; advance the
            # mark directly (no collector, no emit queue).
            await loop.run_in_executor(self._executor, self._flush_sink)
            self.apply_forwarded(sender_vertex_id, sequence)

    def _flush_collector(self) -> None:
        if self._collector is not None:
            self._collector.flush(force=True)

    def _flush_sink(self) -> None:
        if self._operator is not None:
            self._operator.flush()
