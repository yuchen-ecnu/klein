# SPDX-License-Identifier: Apache-2.0
"""Per-sender replay durability watermarks for a StreamTask."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from enum import Enum
from typing import TYPE_CHECKING

import ray.klein as klein
from ray.klein._internal.logging import get_logger
from ray.klein.runtime.message import DeliveryChannel

if TYPE_CHECKING:
    from ray.klein.runtime.worker.emit_pipeline import EmitPipeline


logger = get_logger(__name__)


class WatermarkMode(Enum):
    DISABLED = "disabled"
    PIPELINED = "pipelined"
    SINK = "sink"


class WatermarkController:
    """Publish per-sender progress only after the full processing boundary."""

    def __init__(
        self,
        mode: WatermarkMode,
        flush_interval_batches: int,
        *,
        namespace: str | None = None,
    ) -> None:
        if flush_interval_batches <= 0:
            raise ValueError("replay watermark flush batches must be greater than zero")
        self._mode = mode
        self._namespace = namespace
        self._flush_interval_batches = flush_interval_batches
        self._forwarded_to: dict[object, int] = {}
        self._pending_by_sender: dict[object, int] = {}
        self._batches_since_flush = 0
        self._emit: EmitPipeline | None = None
        self._task_output = None
        self._operator = None
        self._executor = None
        self._flush_input: Callable[[], None] | None = None
        self._flush_input_async: Callable[[], Awaitable[None]] | None = None

    @property
    def active(self) -> bool:
        return self._mode is not WatermarkMode.DISABLED

    def bind(
        self,
        task_output,
        operator,
        executor,
        emit_pipeline,
        flush_input: Callable[[], None],
        flush_input_async: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        self._task_output = task_output
        self._operator = operator
        self._executor = executor
        self._emit = emit_pipeline
        self._flush_input = flush_input
        self._flush_input_async = flush_input_async

    def forwarded_sequence_for(self, sender_vertex_id: object) -> int:
        if sender_vertex_id is None:
            return -1
        return self._forwarded_to.get(sender_vertex_id, -1)

    async def apply_forwarded(self, sender_vertex_id: object, sequence: int) -> None:
        self._forwarded_to[sender_vertex_id] = max(
            self._forwarded_to.get(sender_vertex_id, -1),
            sequence,
        )
        await self._push_ack(sender_vertex_id, sequence)

    async def _push_ack(self, sender: object, sequence: int) -> None:
        if not isinstance(sender, DeliveryChannel) or not self._namespace:
            return
        try:
            upstream = klein.get_actor_by_name(sender.sender_task_name, namespace=self._namespace)
            if upstream is not None:
                await klein.aget(
                    upstream.acknowledge_delivery(
                        sender.edge_index,
                        sender.target_index,
                        sequence,
                    ),
                    timeout=1.0,
                )
        except Exception:
            # The next put still piggybacks the same watermark. An unavailable
            # upstream must not turn replay-buffer cleanup into downstream loss.
            logger.debug("Unable to push replay acknowledgement to %s", sender.sender_task_name, exc_info=True)

    def note_processed(self, sender_vertex_id: object | None, sequence: int | None) -> bool:
        """Record one landed input batch and report whether a flush is due."""
        if sender_vertex_id is None or sequence is None:
            return False
        self._pending_by_sender[sender_vertex_id] = max(
            self._pending_by_sender.get(sender_vertex_id, -1),
            sequence,
        )
        self._batches_since_flush += 1
        return self._batches_since_flush >= self._flush_interval_batches

    async def advance(self) -> None:
        """Flush input, operator and output before publishing all sender marks."""
        if not self.active or not self._pending_by_sender:
            self._batches_since_flush = 0
            return
        pending = dict(self._pending_by_sender)
        await self._do_advance(pending)
        for sender_vertex_id, sequence in pending.items():
            if self._pending_by_sender.get(sender_vertex_id, -1) <= sequence:
                self._pending_by_sender.pop(sender_vertex_id, None)
        self._batches_since_flush = 0

    async def _do_advance(self, pending: dict[object, int]) -> None:
        loop = asyncio.get_running_loop()
        if self._flush_input_async is None:
            await loop.run_in_executor(self._executor, self._flush_processing_boundary)
        else:
            await self._flush_input_async()
            await loop.run_in_executor(self._executor, self._flush_output_boundary)
        if self._mode is WatermarkMode.PIPELINED:
            if self._emit is None:
                raise RuntimeError("pipelined watermark controller is not bound to an emit pipeline")
            await self._emit.drain_pending()
            await self._emit.enqueue_watermarks(pending)
        elif self._mode is WatermarkMode.SINK:
            await asyncio.gather(
                *(self.apply_forwarded(sender_vertex_id, sequence) for sender_vertex_id, sequence in pending.items())
            )

    def _flush_processing_boundary(self) -> None:
        # Input batching is part of the durability boundary: acknowledging an
        # envelope that still lives here would lose it on a single-task restart.
        if self._flush_input is not None:
            self._flush_input()
        self._flush_output_boundary()

    def _flush_output_boundary(self) -> None:
        """Flush output after all input at the durability boundary was processed."""
        if self._task_output is not None:
            self._task_output.flush(force=True)
        elif self._operator is not None:
            self._operator.flush()
