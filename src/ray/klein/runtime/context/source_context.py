# SPDX-License-Identifier: Apache-2.0
import collections
from collections.abc import Callable, Iterable, Mapping
from typing import Any

from ray.klein._internal.values import truncated_repr
from ray.klein.api.collector import Collector
from ray.klein.api.source_context import SourceContext
from ray.klein.runtime.message import InputActive, InputIdle, Record, Watermark


class RuntimeSourceContext(SourceContext):
    """Runtime source context for ordered records and control messages."""

    def __init__(self, collector: Collector) -> None:
        self.collector = collector
        self._on_record_emitted: Callable | None = None
        self._idle = False
        self._last_watermark = -1

    def collect(self, data: dict[str, Any]) -> None:
        self._validate_record(data)
        if self._idle:
            self.mark_active()
        self.collector.collect(Record(data))
        self._maybe_emit_barrier(record_emitted=True)

    def collect_many(self, records: Iterable[Mapping[str, Any]]) -> None:
        """Ship homogeneous source rows as one columnar transport record."""
        materialized = list(records)
        if not materialized:
            return
        if not self.batch_collect_supported:
            for record in materialized:
                self.collect(dict(record))
            return
        for record in materialized:
            self._validate_record(record)
        if self._idle:
            self.mark_active()
        keys = tuple(dict.fromkeys(key for record in materialized for key in record))
        block = {key: [record.get(key) for record in materialized] for key in keys}
        self.collector.collect(Record(block, num_rows=len(materialized)))
        # Preserve record-count checkpoint triggers. The data block is already
        # ordered before any resulting barrier, so snapshots may safely capture
        # the source position at the end of this atomic poll batch.
        self._maybe_emit_barrier(record_emitted=True, record_count=len(materialized))

    @property
    def batch_collect_supported(self) -> bool:
        # A chained source already avoids transport RPCs and its next operator
        # may be row-oriented. Only batch at the actual task-output boundary.
        from ray.klein.runtime.collector.task_output import TaskOutput

        return isinstance(self.collector, TaskOutput)

    def flush(self) -> None:
        self.collector.flush(force=True)

    def collect_durable(self, data: dict[str, Any]) -> None:
        self.collect(data)
        self.flush()

    def on_idle(self) -> None:
        self.mark_idle()
        self._maybe_emit_barrier(record_emitted=False)

    def emit_watermark(self, timestamp: int) -> None:
        watermark = Watermark(timestamp)
        if timestamp <= self._last_watermark:
            return
        if self._idle:
            self.mark_active(self._last_watermark if self._last_watermark >= 0 else None)
        self.collector.collect(watermark)
        self._last_watermark = timestamp

    def mark_idle(self) -> None:
        if self._idle:
            return
        self.collector.collect(InputIdle())
        self._idle = True

    def mark_active(self, resume_watermark: int | None = None) -> None:
        if not self._idle:
            return
        resume = self._last_watermark if resume_watermark is None else max(self._last_watermark, resume_watermark)
        self.collector.collect(InputActive(None if resume < 0 else resume))
        self._idle = False

    def _maybe_emit_barrier(self, record_emitted: bool, record_count: int = 1) -> None:
        if self._on_record_emitted is not None:
            barrier = self._on_record_emitted(record_emitted, record_count)
            if barrier is not None:
                self.collector.collect(barrier)

    def bind_record_emitter(self, on_record_emitted: Callable) -> None:
        self._on_record_emitted = on_record_emitted

    @staticmethod
    def _validate_record(data: Mapping[str, Any]) -> None:
        if not isinstance(data, collections.abc.Mapping):
            raise TypeError(
                f"Error validating {truncated_repr(data)}: "
                "Standalone Python objects are not allowed. "
                "To return Python objects from Source, wrap them in a dict, e.g., "
                "return `{'item': item}` instead of just `item`."
            )
