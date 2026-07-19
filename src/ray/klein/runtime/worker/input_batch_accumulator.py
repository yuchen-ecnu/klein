# SPDX-License-Identifier: Apache-2.0
"""Ordered input batching before operator invocation."""

from __future__ import annotations

import time
from collections import deque
from collections.abc import Sequence
from typing import Any

import numpy as np
import pyarrow as pa

from ray.klein._internal.block import (
    block_row_dict,
    concat_blocks,
    slice_block_rows,
    wrapper_batch_data,
)
from ray.klein.api.runtime_info import RuntimeInfo
from ray.klein.runtime.message import Barrier, Record, StreamControl


class InputBatchAccumulator:
    """A pure, arrival-ordered batching state machine.

    ``accept`` and ``flush`` return records instead of invoking a mutable
    callback. Row-shaped and columnar inputs share one FIFO, so mixed inputs can
    never be reordered merely because their physical representations differ.
    Fast paths retain columnar blocks; conversion to row dictionaries is needed
    only when one output batch genuinely mixes representations or schemas.
    """

    def __init__(self, runtime_info: RuntimeInfo) -> None:
        self._runtime_info = runtime_info
        self._batch_size = runtime_info.batch_size or 1
        self._batch_format = runtime_info.batch_format or "default"
        self._records: deque[Record] = deque()
        self._buffered_rows = 0
        self._batch_started_at: float | None = None

    def accept(self, record: Record) -> tuple[Record, ...]:
        """Accept one ordered input and return every batch made ready by it."""
        if not self._runtime_info.batch_enabled:
            return self._unbatched(record)
        if isinstance(record, Barrier | StreamControl):
            return (*self.flush(force=True), record)

        row_count = self._record_rows(record)
        if row_count == 0:
            return ()
        emitted: tuple[Record, ...] = ()
        if self._records and record.input_tag != self._records[-1].input_tag:
            # A two-input side is semantic metadata for joins, not merely a
            # physical record attribute. Treat a side transition as an ordered
            # batch boundary so no emitted batch can erase that distinction.
            emitted = self._drain(only_full=False)
        self._records.append(record)
        self._buffered_rows += row_count
        if self._batch_started_at is None:
            self._batch_started_at = time.monotonic()
        if self._buffered_rows >= self._batch_size:
            return (*emitted, *self._drain(only_full=True))
        if self._timed_out():
            return (*emitted, *self.flush(force=True))
        return emitted

    def flush(self, force: bool = False) -> tuple[Record, ...]:
        """Return ready batches; ``force`` also drains a partial trailing batch."""
        if not self._runtime_info.batch_enabled or self._buffered_rows == 0:
            return ()
        if not force and self._buffered_rows < self._batch_size and not self._timed_out():
            return ()
        return self._drain(only_full=not force)

    def _unbatched(self, record: Record) -> tuple[Record, ...]:
        if isinstance(record, Barrier | StreamControl) or not record.is_columnar:
            return (record,)
        return tuple(Record(block_row_dict(record.block, index)) for index in range(self._record_rows(record)))

    def _drain(self, only_full: bool) -> tuple[Record, ...]:
        emitted: list[Record] = []
        while self._buffered_rows > 0:
            if only_full and self._buffered_rows < self._batch_size:
                break
            # input_tag selects a two-input side and therefore cannot be erased
            # by batching. If the tag changes before a full batch, emit the
            # ordered prefix as a partial batch instead of mixing both sides.
            compatible_rows = self._compatible_prefix_rows()
            emitted.append(self._merge(self._take(min(self._batch_size, compatible_rows))))
        self._batch_started_at = time.monotonic() if self._buffered_rows else None
        return tuple(emitted)

    def _compatible_prefix_rows(self) -> int:
        first_tag = self._records[0].input_tag
        rows = 0
        for record in self._records:
            if record.input_tag != first_tag:
                break
            rows += self._record_rows(record)
        return rows

    def _take(self, row_count: int) -> list[Record]:
        taken: list[Record] = []
        remaining = row_count
        while remaining > 0:
            record = self._records[0]
            available = self._record_rows(record)
            if available <= remaining:
                taken.append(self._records.popleft())
                consumed = available
            else:
                if not record.is_columnar:
                    raise AssertionError("a row-shaped record cannot be split")
                taken.append(Record(slice_block_rows(record.block, slice(0, remaining)), num_rows=remaining))
                self._records[0] = Record(
                    slice_block_rows(record.block, slice(remaining, available)),
                    num_rows=available - remaining,
                )
                consumed = remaining
            remaining -= consumed
            self._buffered_rows -= consumed
        return taken

    def _merge(self, records: Sequence[Record]) -> Record:
        if len(records) == 1 and records[0].is_columnar:
            return records[0]
        if records and all(record.is_columnar for record in records) and self._same_columnar_schema(records):
            blocks = [record.block for record in records]
            merged = Record(concat_blocks(blocks), num_rows=sum(self._record_rows(record) for record in records))
            self._inherit_metadata(merged, records)
            return merged

        rows: list[dict[str, Any]] = []
        contains_columnar = False
        for record in records:
            if record.is_columnar:
                contains_columnar = True
                rows.extend(block_row_dict(record.block, index) for index in range(self._record_rows(record)))
            else:
                if record.block is None:
                    raise ValueError("control records cannot be accumulated as data")
                rows.append(record.block)
        columns = list(dict.fromkeys(column for row in rows for column in row))
        block = {
            column: wrapper_batch_data([row.get(column) for row in rows], self._batch_format) for column in columns
        }
        merged = Record(block, num_rows=len(rows) if contains_columnar else None)
        self._inherit_metadata(merged, records)
        return merged

    @staticmethod
    def _inherit_metadata(merged: Record, records: Sequence[Record]) -> None:
        first = records[0]
        if all(record.input_tag == first.input_tag for record in records):
            merged.input_tag = first.input_tag
        if all(record.sender == first.sender for record in records):
            merged.sender = first.sender
        if all(record.timestamp == first.timestamp for record in records):
            merged.timestamp = first.timestamp

    @staticmethod
    def _same_columnar_schema(records: Sequence[Record]) -> bool:
        first_block = records[0].block
        if first_block is None:
            return False
        columns = tuple(first_block)
        value_types = tuple(type(first_block[column]) for column in columns)
        supported_types = (list, tuple, np.ndarray, pa.Array)
        return all(
            record.block is not None
            and tuple(record.block) == columns
            and tuple(type(record.block[column]) for column in columns) == value_types
            and all(isinstance(record.block[column], supported_types) for column in columns)
            for record in records
        )

    @staticmethod
    def _record_rows(record: Record) -> int:
        rows = 1 if record.num_rows is None else record.num_rows
        if isinstance(rows, bool) or not isinstance(rows, int) or rows < 0:
            raise ValueError(f"record has invalid row count: {rows!r}")
        return rows

    def _timed_out(self) -> bool:
        timeout = self._runtime_info.batch_timeout
        return (
            timeout is not None
            and self._batch_started_at is not None
            and time.monotonic() - self._batch_started_at >= timeout
        )
