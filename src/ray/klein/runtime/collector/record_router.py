# SPDX-License-Identifier: Apache-2.0
"""Validated, executor-thread routing over a task-local Partitioner."""

from __future__ import annotations

from collections.abc import Iterable, Iterator

from ray.klein._internal.block import slice_block_rows
from ray.klein.runtime.context.runtime_context import OperatorRuntimeContext
from ray.klein.runtime.message import Record
from ray.klein.runtime.partitioning.partitioner import Partitioner


class RecordRouter:
    """Turn records into validated target slices and immutable retry rings."""

    def __init__(
        self,
        partitioner: Partitioner,
        target_count: int,
        control_target_indices: tuple[int, ...],
    ) -> None:
        if target_count <= 0:
            raise ValueError("router target count must be greater than zero")
        self._partitioner = partitioner
        self._target_count = target_count
        self._control_target_indices = self._validate_targets(
            control_target_indices,
            "control topology",
        )
        if not self._control_target_indices:
            raise ValueError("control topology must contain at least one target")
        self._allowed_targets = frozenset(self._control_target_indices)
        self._retry_rings: dict[int, tuple[int, ...]] = {}

    def open(self, op_runtime_context: OperatorRuntimeContext) -> None:
        self._partitioner.open(op_runtime_context, self._target_count)

    def route(self, record: Record) -> Iterator[tuple[int, Record]]:
        """Yield validated ``(target_index, record_or_slice)`` pairs."""
        if not record.is_columnar:
            yield from self._route_row(record)
            return
        yield from self._route_columnar(record)

    def _route_row(self, record: Record) -> Iterator[tuple[int, Record]]:
        targets = self._validate_targets(self._partitioner.partition(record), "row route")
        if not targets:
            raise ValueError(f"{self._partitioner} produced an empty row route")
        for target_index in targets:
            self.retry_ring(target_index)
            yield target_index, record

    def _route_columnar(self, record: Record) -> Iterator[tuple[int, Record]]:
        num_rows = record.num_rows
        if isinstance(num_rows, bool) or not isinstance(num_rows, int) or num_rows < 0:
            raise ValueError(f"columnar record has invalid row count: {num_rows!r}")
        raw_routes = self._partitioner.partition_columnar(record, num_rows)
        try:
            routes = list(raw_routes)
        except TypeError as error:
            raise TypeError(f"{self._partitioner} columnar route must be iterable") from error
        if num_rows and not routes:
            raise ValueError(f"{self._partitioner} produced an empty columnar route")

        seen_targets: set[int] = set()
        covered_rows: set[int] = set()
        covers_all_rows = False
        for route in routes:
            if not isinstance(route, tuple) or len(route) != 2:
                raise TypeError(f"{self._partitioner} columnar route entries must be (target, row_indices) tuples")
            target_index = self._validate_target(route[0], "columnar route")
            if target_index in seen_targets:
                raise ValueError(f"{self._partitioner} returned target {target_index} more than once")
            seen_targets.add(target_index)
            self.retry_ring(target_index)
            row_indices = route[1]
            if row_indices is None:
                # ``None`` is the whole-block fast path used by all
                # content-independent partitioners. It proves coverage without
                # allocating sets containing every row number.
                covers_all_rows = True
                yield target_index, record
                continue
            indices = self._validate_row_indices(row_indices, num_rows, target_index)
            covered_rows.update(indices)
            sub_block = slice_block_rows(record.block, indices)
            yield target_index, Record(sub_block, num_rows=len(indices))

        if not covers_all_rows and len(covered_rows) != num_rows:
            expected_rows = set(range(num_rows))
            missing = sorted(expected_rows - covered_rows)
            raise ValueError(f"{self._partitioner} did not route columnar rows {missing[:10]}")

    def retry_ring(self, initial_target: int) -> tuple[int, ...]:
        """Freeze and validate a partitioner's retry decision on this thread."""
        cached = self._retry_rings.get(initial_target)
        if cached is not None:
            return cached
        ring = self._validate_targets(
            self._partitioner.retry_targets(initial_target),
            f"retry ring for target {initial_target}",
        )
        if not ring or ring[0] != initial_target:
            raise ValueError(f"{self._partitioner} retry ring must start with initial target {initial_target}: {ring}")
        self._retry_rings[initial_target] = ring
        return ring

    @property
    def control_targets(self) -> tuple[int, ...]:
        return self._control_target_indices

    def _validate_targets(self, targets: Iterable[int], context: str) -> tuple[int, ...]:
        try:
            values = tuple(targets)
        except TypeError as error:
            raise TypeError(f"{self._partitioner} {context} must be an iterable of target indices") from error
        validated: list[int] = []
        seen: set[int] = set()
        for target in values:
            target = self._validate_target(target, context)
            if target in seen:
                raise ValueError(f"{self._partitioner} {context} contains duplicate target {target}")
            seen.add(target)
            validated.append(target)
        return tuple(validated)

    def _validate_target(self, target: int, context: str) -> int:
        if isinstance(target, bool) or not isinstance(target, int):
            raise TypeError(f"{self._partitioner} {context} returned non-integer target {target!r}")
        if target < 0 or target >= self._target_count:
            raise ValueError(f"{self._partitioner} {context} target {target} is outside [0, {self._target_count})")
        if hasattr(self, "_allowed_targets") and target not in self._allowed_targets:
            raise ValueError(
                f"{self._partitioner} routed data to target {target}, which is outside its control topology "
                f"{self._control_target_indices}"
            )
        return target

    def _validate_row_indices(
        self,
        row_indices: Iterable[int],
        num_rows: int,
        target_index: int,
    ) -> list[int]:
        try:
            indices = list(row_indices)
        except TypeError as error:
            raise TypeError(f"{self._partitioner} row indices for target {target_index} must be iterable") from error
        if not indices:
            raise ValueError(f"{self._partitioner} returned an empty row slice for target {target_index}")
        seen: set[int] = set()
        for index in indices:
            if isinstance(index, bool) or not isinstance(index, int):
                raise TypeError(f"{self._partitioner} returned non-integer row index {index!r}")
            if index < 0 or index >= num_rows:
                raise ValueError(f"{self._partitioner} row index {index} is outside [0, {num_rows})")
            if index in seen:
                raise ValueError(f"{self._partitioner} returned row index {index} twice for target {target_index}")
            seen.add(index)
        return indices
