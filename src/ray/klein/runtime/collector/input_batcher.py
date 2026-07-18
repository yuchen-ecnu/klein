# SPDX-License-Identifier: Apache-2.0
import time
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any

from ray.klein._internal.block import (
    block_row_dict,
    concat_blocks,
    slice_block_rows,
    wrapper_batch_data,
)
from ray.klein.api.runtime_info import RuntimeInfo
from ray.klein.runtime.message import Barrier, Record, StreamControl


class _Accumulator(ABC):
    """Buffers input of one shape and drains it into batch_size-sized blocks.

    Two shapes reach a batched operator: per-row records (the source /
    non-passthrough path) and columnar batches (the columnar-passthrough path).
    They batch differently — rows need column-union null-fill, columns need a
    zero-copy concat/slice — so each shape gets its own accumulator. Both expose
    the same ``drain(only_full)`` contract so the batcher treats them uniformly:

    * ``only_full=True``  — size-triggered: emit only whole batch_size blocks and
      KEEP a sub-size remainder for the next round (downstream batches don't
      fragment).
    * ``only_full=False`` — barrier / timeout / idle / teardown: emit everything
      INCLUDING the final partial block (nothing is stranded past a barrier).
    """

    @abstractmethod
    def add(self, record: Record) -> None: ...

    @property
    @abstractmethod
    def row_count(self) -> int: ...

    @abstractmethod
    def drain(self, batch_size: int, only_full: bool) -> Iterator[Record]: ...


class _RowAccumulator(_Accumulator):
    """Per-row records -> column-oriented blocks with first-seen column union.

    Heterogeneous rows (a column absent from some rows) are null-filled so every
    column spans every row in the emitted block (Klein's block is a
    dict-of-columns).
    """

    def __init__(self, batch_format: str) -> None:
        self._batch_format = batch_format
        self._rows: list[dict[str, Any]] = []
        self._columns: list[str] = []  # insertion-ordered union of seen columns
        self._column_set: set = set()

    def add(self, record: Record) -> None:
        row = record.block
        for col in row:
            if col not in self._column_set:
                self._column_set.add(col)
                self._columns.append(col)
        self._rows.append(row)

    @property
    def row_count(self) -> int:
        return len(self._rows)

    def drain(self, batch_size: int, only_full: bool) -> Iterator[Record]:
        n = len(self._rows)
        limit = (n // batch_size) * batch_size if only_full else n
        emitted = 0
        while emitted < limit:
            chunk = self._rows[emitted : emitted + batch_size]
            block = {
                col: wrapper_batch_data([row.get(col) for row in chunk], self._batch_format) for col in self._columns
            }
            # No num_rows tag: this feeds the operator (which reads .block), not
            # the wire — keeping the row path's record shape unchanged.
            yield Record(block)
            emitted += batch_size
        # Retain any sub-size remainder; reset column bookkeeping only when empty.
        self._rows = self._rows[emitted:]
        if not self._rows:
            self._columns = []
            self._column_set = set()


class _ColumnarAccumulator(_Accumulator):
    """Already-columnar blocks -> concat + re-slice to batch_size (no row explode)."""

    def __init__(self) -> None:
        self._blocks: list[dict[str, Any]] = []
        self._rows: int = 0

    def add(self, record: Record) -> None:
        self._blocks.append(record.block)
        self._rows += record.num_rows or 0

    @property
    def row_count(self) -> int:
        return self._rows

    def drain(self, batch_size: int, only_full: bool) -> Iterator[Record]:
        if not self._blocks:
            return
        merged = concat_blocks(self._blocks)
        total = self._rows
        limit = (total // batch_size) * batch_size if only_full else total
        emitted = 0
        while emitted < limit:
            end = min(emitted + batch_size, total)
            yield Record(
                slice_block_rows(merged, list(range(emitted, end))),
                num_rows=end - emitted,
            )
            emitted = end
        if emitted < total:
            # Carry the sub-size tail as a single block for the next round.
            self._blocks = [slice_block_rows(merged, list(range(emitted, total)))]
            self._rows = total - emitted
        else:
            self._blocks = []
            self._rows = 0


class InputBatcher:
    """Accumulates input into column-oriented batches before the operator.

    Control-flow contract:
    * A **barrier is a flush trigger**, not a queued item. On any barrier we
      first flush whatever data has accumulated, then pass the barrier straight
      through — so a periodic checkpoint / EndOfData barrier is never delayed by
      a partially-filled batch (which would stall checkpoint alignment).
    * Data flushes when the accumulated row count reaches ``batch_size`` (keeping
      any remainder) or ``batch_timeout`` seconds elapse since the first row of
      the current batch (flushing everything).
    * Timing uses ``time.monotonic()`` (immune to wall-clock jumps).

    Two input shapes — per-row records and columnar-passthrough batches — are
    handled by two accumulators behind one ``drain`` contract; the batcher owns
    only the shared timer + readiness + barrier dispatch. The row count for
    readiness/timeout spans both, so a stream that mixes shapes (e.g. a union of
    a columnar and a row edge) still batches by the same rules.
    """

    def __init__(
        self,
        runtime_info: RuntimeInfo,
        data_handler: Callable[[Record], None],
    ) -> None:
        self._runtime_info = runtime_info
        self._batch_size = runtime_info.batch_size or 1
        # Public so async drivers can temporarily redirect emissions into a
        # buffer (the async pump must still drive batching on the input side but
        # needs to await the operator one-by-one on the loop, not via the default
        # sync handler that calls into the executor-bound operator).
        self.data_handler = data_handler
        fmt = runtime_info.batch_format or "default"
        self._rows = _RowAccumulator(fmt)
        self._blocks = _ColumnarAccumulator()
        self._batch_start: float | None = None

    def accumulate_then_flush(self, record: Record) -> None:
        if not self._runtime_info.batch_enabled:
            self._emit_unbatched(record)
            return
        if isinstance(record, (Barrier, StreamControl)):
            self.force_flush()
            self.data_handler(record)
            return

        self._accumulate_data(record)

    def _emit_unbatched(self, record: Record) -> None:
        if isinstance(record, (Barrier, StreamControl)) or not record.is_columnar:
            self.data_handler(record)
            return
        for index in range(record.num_rows or 0):
            self.data_handler(Record(block_row_dict(record.block, index)))

    def _accumulate_data(self, record: Record) -> None:
        target = self._blocks if record.is_columnar else self._rows
        target.add(record)
        if self._batch_start is None:
            self._batch_start = time.monotonic()
        if self._row_count() >= self._batch_size:
            self._drain(only_full=True)
        elif self._timed_out():
            self.force_flush()

    @contextmanager
    def _redirect_to(self, sink: Callable[[Record], None]) -> Iterator[None]:
        """Temporarily route emissions to ``sink`` (restored on exit).

        Backs the collecting query methods below: the async pump needs the
        batcher's output as a list it can await one-by-one on the loop, not
        pushed through the executor-bound default handler. Encapsulated here so
        callers never have to save/restore ``data_handler`` by hand.
        """
        prev = self.data_handler
        self.data_handler = sink
        try:
            yield
        finally:
            self.data_handler = prev

    def collect_accumulate_then_flush(self, record: Record) -> list[Record]:
        """Query variant of :meth:`accumulate_then_flush` — returns what it emits.

        Same batching/barrier semantics; the emitted records are captured into a
        list instead of pushed to ``data_handler`` so an async caller can await
        them in order.
        """
        collected: list[Record] = []
        with self._redirect_to(collected.append):
            self.accumulate_then_flush(record)
        return collected

    def collect_force_flush(self) -> list[Record]:
        """Query variant of :meth:`force_flush` — returns the drained records."""
        collected: list[Record] = []
        with self._redirect_to(collected.append):
            self.force_flush()
        return collected

    def flush(self) -> None:
        """Idle/time-based flush — only fires once a batch is actually ready.

        No-op when batching is disabled: nothing is ever accumulated (records
        pass straight through in accumulate_then_flush), and batch_size /
        batch_timeout are None, so there's neither anything to flush nor a
        threshold to compare against."""
        if not self._runtime_info.batch_enabled:
            return
        if self._row_count() >= self._batch_size or self._timed_out():
            self.force_flush()

    def force_flush(self) -> None:
        """Drain everything (barrier / idle / teardown), including a partial batch."""
        self._drain(only_full=False)

    def _drain(self, only_full: bool) -> None:
        for accumulator in (self._rows, self._blocks):
            for batch in accumulator.drain(self._batch_size, only_full):
                self.data_handler(batch)
        # Restart the timeout clock unless a remainder is still buffered (its
        # clock keeps running so it can't be stranded indefinitely).
        self._batch_start = time.monotonic() if self._row_count() > 0 else None

    def _row_count(self) -> int:
        return self._rows.row_count + self._blocks.row_count

    def _timed_out(self) -> bool:
        timeout = self._runtime_info.batch_timeout
        if timeout is None or self._batch_start is None or self._row_count() == 0:
            return False
        return time.monotonic() - self._batch_start >= timeout
