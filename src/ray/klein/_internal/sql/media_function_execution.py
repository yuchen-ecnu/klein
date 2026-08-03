# SPDX-License-Identifier: Apache-2.0
"""Fused batch execution for built-in SQL media functions."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import timedelta
from typing import TYPE_CHECKING, Any

import numpy as np
import pyarrow as pa

from ray.klein._internal.frozen_mapping import FrozenMapping
from ray.klein._internal.sql.expression import evaluate_expression
from ray.klein._internal.sql.media_function import MediaComputation
from ray.klein._internal.sql.scalar_function_registry import ScalarFunction
from ray.klein._internal.values import create_ragged_ndarray
from ray.klein.api.row_kind import RowKind
from ray.klein.api.sql_query_error import SQLQueryError

if TYPE_CHECKING:
    from ray.data import Dataset

    from ray.klein._internal.sql.media_runtime import MediaLimits
    from ray.klein.api.data_stream import DataStream

# Ray materializes the input batch before this UDF can enforce its cumulative
# binary budget. Media inputs can each be hundreds of MiB, so one row per batch
# also bounds the pre-validation peak by the per-input limit.
MEDIA_BATCH_SIZE = 1
_ARRAY_MEDIA_FUNCTIONS = frozenset({"pdf_split", "pdf_to_images"})


class _MediaBatchBudget:
    """Track distinct binary values retained by one fused media batch."""

    def __init__(self, limit: int) -> None:
        self._limit = limit
        self._retained = 0
        self._seen: set[int] = set()

    def retain(self, value: Any) -> None:
        pending = [value]
        while pending:
            item = pending.pop()
            identity = id(item)
            if identity in self._seen:
                continue
            if isinstance(item, (bytes, bytearray, memoryview)):
                self._seen.add(identity)
                try:
                    size = item.nbytes if isinstance(item, memoryview) else len(item)
                except ValueError as error:
                    raise SQLQueryError("SQL media batch contains an invalid binary value") from error
                self._retained += size
                if self._retained > self._limit:
                    raise SQLQueryError(
                        f"SQL media batch exceeds the {self._limit}-byte cumulative binary safety limit"
                    )
            elif isinstance(item, (list, tuple)):
                self._seen.add(identity)
                pending.extend(item)


class _ApplyMediaFunctionsBatch:
    """Execute every media expression in one batch and decode each asset once."""

    def __init__(
        self,
        computations: Sequence[MediaComputation],
        functions: Mapping[str, ScalarFunction],
        native_threads: int | None = None,
        arrow_arrays: bool = True,
        limits: MediaLimits | None = None,
    ) -> None:
        from ray.klein._internal.sql.media_runtime import DEFAULT_MEDIA_LIMITS, MediaRuntime

        self._computations = tuple(computations)
        self._functions = FrozenMapping(functions)
        effective_limits = DEFAULT_MEDIA_LIMITS if limits is None else limits
        self._runtime = MediaRuntime(limits=effective_limits, native_threads=native_threads)
        self._max_batch_bytes = effective_limits.max_batch_bytes
        self._arrow_arrays = arrow_arrays

    def __call__(self, batch: Mapping[str, Any] | pa.Table) -> dict[str, Any]:
        input_columns = (
            {name: batch.column(name) for name in batch.column_names} if isinstance(batch, pa.Table) else dict(batch)
        )
        output = dict(input_columns)
        row_count = len(next(iter(input_columns.values()))) if input_columns else 0
        columns = {computation.field_name: [None] * row_count for computation in self._computations}
        budget = _MediaBatchBudget(self._max_batch_bytes)
        for index in range(row_count):
            row = {name: _python_scalar(values[index]) for name, values in input_columns.items()}
            cache: dict[Any, Any] = {}
            try:
                for computation in self._computations:
                    arguments = tuple(
                        evaluate_expression(argument, row, self._functions) for argument in computation.arguments
                    )
                    budget.retain(arguments)
                    value = (
                        None
                        if any(argument is None for argument in arguments)
                        else self._execute(computation.function_name, arguments, cache)
                    )
                    budget.retain(value)
                    row[computation.field_name] = value
                    columns[computation.field_name][index] = value
            finally:
                self._runtime.clear_cache(cache)
        for computation in self._computations:
            values = columns[computation.field_name]
            output[computation.field_name] = (
                pa.array(values, type=pa.list_(pa.binary()))
                if self._arrow_arrays and computation.function_name in _ARRAY_MEDIA_FUNCTIONS
                else create_ragged_ndarray(values)
            )
        return output

    def _execute(self, name: str, arguments: tuple[Any, ...], cache: dict[Any, Any]) -> Any:
        try:
            return self._runtime.execute(name, arguments, cache)
        except SQLQueryError:
            raise
        except Exception as error:
            raise SQLQueryError(f"{name.upper()} failed with {type(error).__name__}") from error


def apply_batch_media_computations(
    dataset: Dataset,
    computations: Sequence[MediaComputation],
    *,
    functions: Mapping[str, ScalarFunction],
    num_cpus: float,
) -> Dataset:
    if not computations:
        return dataset
    return dataset.map_batches(
        _ApplyMediaFunctionsBatch,
        fn_constructor_args=(tuple(computations), functions, _native_threads(num_cpus)),
        batch_size=MEDIA_BATCH_SIZE,
        batch_format="pyarrow",
        num_cpus=num_cpus,
    )


def apply_streaming_media_computations(
    stream: DataStream,
    computations: Sequence[MediaComputation],
    *,
    functions: Mapping[str, ScalarFunction],
    num_cpus: float,
) -> DataStream:
    if not computations:
        return stream
    if stream.changelog_mode != frozenset({RowKind.INSERT}):
        raise SQLQueryError("SQL media functions currently require an insert-only streaming input")
    return stream.map_batches(
        _ApplyMediaFunctionsBatch,
        fn_constructor_args=(tuple(computations), functions, _native_threads(num_cpus), False),
        num_cpus=num_cpus,
        batch_size=MEDIA_BATCH_SIZE,
        batch_timeout=timedelta(seconds=3),
        batch_format="numpy",
        name="SQLMediaFunctions",
    )


def _python_scalar(value: Any) -> Any:
    if isinstance(value, pa.Scalar):
        return value.as_py()
    return value.item() if isinstance(value, np.generic) else value


def _native_threads(num_cpus: float) -> int:
    """Match native decoder threads to the CPU reservation without oversubscription."""

    return max(1, int(num_cpus))
