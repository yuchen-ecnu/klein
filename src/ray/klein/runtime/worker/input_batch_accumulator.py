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
    ColumnarBlock,
    arrow_block_to_mapping,
    block_row_dict,
    concat_blocks,
    slice_block_rows,
    to_arrow_record_batch,
    wrapper_batch_data,
)
from ray.klein._internal.memory import estimate_retained_size
from ray.klein.api.changelog_row import ChangelogRow
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

    def __init__(self, runtime_info: RuntimeInfo, *, max_bytes: int | None = None) -> None:
        if max_bytes is not None and (isinstance(max_bytes, bool) or not isinstance(max_bytes, int)):
            raise TypeError("input batch byte budget must be an integer or None")
        if max_bytes is not None and max_bytes <= 0:
            raise ValueError("input batch byte budget must be positive")
        self._runtime_info = runtime_info
        self._batch_size = runtime_info.batch_size or 1
        self._batch_format = runtime_info.batch_format or "default"
        self._max_bytes = max_bytes
        self._records: deque[Record] = deque()
        self._record_sizes: deque[int] = deque()
        self._buffered_rows = 0
        self._buffered_bytes = 0
        self._batch_started_at: float | None = None

    @property
    def buffered_rows(self) -> int:
        return self._buffered_rows

    @property
    def buffered_bytes(self) -> int:
        return self._buffered_bytes

    def accept(self, record: Record) -> tuple[Record, ...]:
        """Accept one ordered input and return every batch made ready by it."""
        if not self._runtime_info.batch_enabled:
            return self._unbatched(record)
        if isinstance(record, Barrier | StreamControl):
            return (*self.flush(force=True), record)

        row_count = self._record_rows(record)
        if row_count == 0:
            return ()
        record_size = estimate_retained_size(record)
        emitted: tuple[Record, ...] = ()
        if self._records and record.input_tag != self._records[-1].input_tag:
            # A two-input side is semantic metadata for joins, not merely a
            # physical record attribute. Treat a side transition as an ordered
            # batch boundary so no emitted batch can erase that distinction.
            emitted = self._drain(only_full=False)
        if self._records and self._max_bytes is not None and self._buffered_bytes + record_size > self._max_bytes:
            emitted = (*emitted, *self._drain(only_full=False))
        self._records.append(record)
        self._record_sizes.append(record_size)
        self._buffered_rows += row_count
        self._buffered_bytes += record_size
        if self._batch_started_at is None:
            self._batch_started_at = time.monotonic()
        if self._max_bytes is not None and self._buffered_bytes >= self._max_bytes:
            # Reaching the byte budget is an eager boundary. One oversized
            # record is still admitted and flushed alone so the stream keeps
            # making progress without allowing more records to stack on it.
            return (*emitted, *self._drain(only_full=False))
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
        rows = tuple(Record(self._row_block(record, index)) for index in range(self._record_rows(record)))
        self._validate_row_timestamps(record)
        for index, row in enumerate(rows):
            self._inherit_metadata(row, (record,))
            if record.row_timestamps is not None:
                row.timestamp = record.row_timestamps[index]
        return rows

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
            record_size = self._record_sizes[0]
            available = self._record_rows(record)
            if available <= remaining:
                taken.append(self._records.popleft())
                self._record_sizes.popleft()
                consumed = available
                self._buffered_bytes -= record_size
            else:
                if not record.is_columnar:
                    raise AssertionError("a row-shaped record cannot be split")
                prefix = Record(slice_block_rows(record.block, slice(0, remaining)), num_rows=remaining)
                suffix = Record(
                    slice_block_rows(record.block, slice(remaining, available)),
                    num_rows=available - remaining,
                )
                self._inherit_metadata(prefix, (record,))
                self._inherit_metadata(suffix, (record,))
                if record.row_kinds is not None:
                    prefix.row_kinds = record.row_kinds[:remaining]
                    suffix.row_kinds = record.row_kinds[remaining:]
                if record.row_timestamps is not None:
                    self._validate_row_timestamps(record)
                    prefix.row_timestamps = record.row_timestamps[:remaining]
                    suffix.row_timestamps = record.row_timestamps[remaining:]
                taken.append(prefix)
                self._records[0] = suffix
                suffix_size = estimate_retained_size(suffix)
                self._record_sizes[0] = suffix_size
                self._buffered_bytes += suffix_size - record_size
                consumed = remaining
            remaining -= consumed
            self._buffered_rows -= consumed
        return taken

    def _merge(self, records: Sequence[Record]) -> Record:
        if len(records) == 1 and records[0].is_columnar:
            source = records[0]
            block = self._format_columnar_block(source.block)
            merged = Record(block, num_rows=self._record_rows(source))
            self._inherit_metadata(merged, records)
            self._inherit_row_kinds(merged, records)
            self._inherit_row_timestamps(merged, records)
            return merged
        if records and all(record.is_columnar for record in records) and self._same_columnar_schema(records):
            blocks = [record.block for record in records]
            block = self._format_columnar_block(concat_blocks(blocks))
            merged = Record(block, num_rows=sum(self._record_rows(record) for record in records))
            self._inherit_metadata(merged, records)
            self._inherit_row_kinds(merged, records)
            self._inherit_row_timestamps(merged, records)
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
        self._inherit_row_kinds(merged, records)
        self._inherit_row_timestamps(merged, records)
        return merged

    def _format_columnar_block(self, block: ColumnarBlock | None) -> ColumnarBlock:
        if block is None:
            raise ValueError("columnar record cannot have an empty block")
        if isinstance(block, pa.RecordBatch | pa.Table):
            return arrow_block_to_mapping(block, self._batch_format)
        return block

    @staticmethod
    def _inherit_metadata(merged: Record, records: Sequence[Record]) -> None:
        first = records[0]
        if all(record.input_tag == first.input_tag for record in records):
            merged.input_tag = first.input_tag
        if all(record.sender == first.sender for record in records):
            merged.sender = first.sender
        if all(record.timestamp == first.timestamp for record in records):
            merged.timestamp = first.timestamp

    @classmethod
    def _inherit_row_kinds(cls, merged: Record, records: Sequence[Record]) -> None:
        kinds: list[object | None] = []
        for record in records:
            rows = cls._record_rows(record)
            if record.row_kinds is not None:
                if len(record.row_kinds) != rows:
                    raise ValueError(f"record has {len(record.row_kinds)} row kinds for {rows} logical rows")
                kinds.extend(record.row_kinds)
            elif not record.is_columnar and isinstance(record.block, ChangelogRow):
                kinds.append(record.block.row_kind)
            else:
                kinds.extend([None] * rows)
        if any(kind is not None for kind in kinds):
            merged.row_kinds = tuple(kinds)

    @classmethod
    def _inherit_row_timestamps(cls, merged: Record, records: Sequence[Record]) -> None:
        timestamps: list[int | None] = []
        for record in records:
            rows = cls._record_rows(record)
            if record.row_timestamps is not None:
                cls._validate_row_timestamps(record)
                timestamps.extend(record.row_timestamps)
            else:
                timestamps.extend([record.timestamp] * rows)
        if timestamps and all(timestamp == timestamps[0] for timestamp in timestamps[1:]):
            merged.timestamp = timestamps[0]
        elif timestamps:
            merged.timestamp = None
            merged.row_timestamps = tuple(timestamps)

    @classmethod
    def _validate_row_timestamps(cls, record: Record) -> None:
        if record.row_timestamps is not None and len(record.row_timestamps) != cls._record_rows(record):
            raise ValueError(
                f"record has {len(record.row_timestamps)} row timestamps for {cls._record_rows(record)} logical rows"
            )

    @staticmethod
    def _row_block(record: Record, index: int) -> dict[str, Any]:
        row = block_row_dict(record.block, index)
        if record.row_kinds is None:
            return dict(row)
        if len(record.row_kinds) != InputBatchAccumulator._record_rows(record):
            raise ValueError(
                f"record has {len(record.row_kinds)} row kinds for "
                f"{InputBatchAccumulator._record_rows(record)} logical rows"
            )
        row_kind = record.row_kinds[index]
        return ChangelogRow(row, row_kind=row_kind) if row_kind is not None else dict(row)

    @staticmethod
    def _same_columnar_schema(records: Sequence[Record]) -> bool:
        first_block = records[0].block
        if first_block is None:
            return False
        if isinstance(first_block, pa.RecordBatch | pa.Table):
            schema = to_arrow_record_batch(first_block).schema
            return all(
                isinstance(record.block, pa.RecordBatch | pa.Table)
                and to_arrow_record_batch(record.block).schema.equals(schema)
                for record in records
            )
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
