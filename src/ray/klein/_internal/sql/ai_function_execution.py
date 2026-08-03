# SPDX-License-Identifier: Apache-2.0
"""Batched execution and projection rewriting for SQL AI functions."""

from __future__ import annotations

import inspect
from collections.abc import Mapping, Sequence
from datetime import timedelta
from typing import TYPE_CHECKING, Any

import numpy as np
import pyarrow as pa
from sqlglot import exp

from ray.klein._internal.frozen_mapping import FrozenMapping
from ray.klein._internal.sql.ai_function_registry import (
    AIFunctionSpec,
    ai_function_arguments,
    ai_function_call_name,
    is_ai_function_call,
)
from ray.klein._internal.sql.expression import evaluate_expression
from ray.klein._internal.sql.ray_data_expression import is_ray_data_only_expression
from ray.klein._internal.sql.scalar_function_registry import ScalarFunction
from ray.klein._internal.values import create_ragged_ndarray
from ray.klein.api.row_kind import RowKind
from ray.klein.api.sql_query_error import SQLQueryError

if TYPE_CHECKING:
    from ray.data import Dataset

    from ray.klein.api.data_stream import DataStream


class _AIFunctionBatchBase:
    def __init__(
        self,
        name: str,
        spec: AIFunctionSpec,
        arguments: Sequence[exp.Expression],
        functions: Mapping[str, ScalarFunction],
        output_field: str,
    ) -> None:
        self._name = name
        self._arguments = tuple(arguments)
        self._functions = FrozenMapping(functions)
        self._output_field = output_field
        try:
            if isinstance(spec.function, type):
                self._backend = spec.function(*spec.constructor_args, **spec.constructor_kwargs)
            else:
                self._backend = spec.function
        except Exception as error:
            raise SQLQueryError(f"{name.upper()} backend initialization failed with {type(error).__name__}") from error

    def _prepare(
        self,
        batch: Mapping[str, Any] | pa.Table,
    ) -> tuple[dict[str, Any], list[tuple[Any, ...]], list[int], list[Any]]:
        output = _batch_columns(batch)
        row_count = len(next(iter(output.values()))) if output else 0
        calls: list[tuple[Any, ...]] = []
        call_indices: list[int] = []
        results: list[Any] = [None] * row_count
        for index in range(row_count):
            row = {name: _python_scalar(values[index]) for name, values in output.items()}
            arguments = tuple(evaluate_expression(argument, row, self._functions) for argument in self._arguments)
            if any(argument is None for argument in arguments):
                continue
            calls.append(arguments)
            call_indices.append(index)
        return output, calls, call_indices, results

    def _finish(
        self,
        output: dict[str, Any],
        call_indices: Sequence[int],
        results: list[Any],
        backend_results: Any,
    ) -> dict[str, Any]:
        values = _result_sequence(self._name, backend_results, len(call_indices))
        for index, value in zip(call_indices, values, strict=True):
            results[index] = value
        output[self._output_field] = create_ragged_ndarray(results)
        return output


class _ApplyAIFunctionBatch(_AIFunctionBatchBase):
    def __call__(self, batch: Mapping[str, Any]) -> dict[str, Any]:
        output, calls, call_indices, results = self._prepare(batch)
        if not calls:
            output[self._output_field] = create_ragged_ndarray(results)
            return output
        try:
            backend_results = self._backend(calls)
        except Exception as error:
            raise SQLQueryError(f"{self._name.upper()} backend failed with {type(error).__name__}") from error
        if inspect.isawaitable(backend_results):
            if inspect.iscoroutine(backend_results):
                backend_results.close()
            raise SQLQueryError(f"{self._name.upper()} registered an async backend in batch execution mode")
        return self._finish(output, call_indices, results, backend_results)


class _ApplyAsyncAIFunctionBatch(_AIFunctionBatchBase):
    async def __call__(self, batch: Mapping[str, Any]) -> dict[str, Any]:
        output, calls, call_indices, results = self._prepare(batch)
        if not calls:
            output[self._output_field] = create_ragged_ndarray(results)
            return output
        try:
            backend_results = await self._backend(calls)
        except Exception as error:
            raise SQLQueryError(f"{self._name.upper()} backend failed with {type(error).__name__}") from error
        return self._finish(output, call_indices, results, backend_results)


def apply_batch_ai_projections(
    dataset: Dataset,
    projections: Sequence[exp.Expression],
    *,
    functions: Mapping[str, ScalarFunction],
    ai_functions: Mapping[str, AIFunctionSpec],
) -> tuple[Dataset, tuple[exp.Expression, ...]]:
    computations, rewritten = _ai_projection_plan(projections, ai_functions)
    for field_name, call, spec in computations:
        if spec.is_async:
            raise SQLQueryError(
                f"{ai_function_call_name(call).upper()} async backends require streaming execution mode"
            )
        dataset = dataset.map_batches(
            _ApplyAIFunctionBatch,
            fn_constructor_args=(
                ai_function_call_name(call),
                spec,
                ai_function_arguments(call),
                functions,
                field_name,
            ),
            batch_size=spec.batch_size,
            batch_format="pyarrow",
            num_cpus=spec.resources.num_cpus,
            num_gpus=spec.resources.num_gpus,
            concurrency=spec.resources.concurrency,
        )
    return dataset, rewritten


def apply_streaming_ai_projections(
    stream: DataStream,
    projections: Sequence[exp.Expression],
    *,
    functions: Mapping[str, ScalarFunction],
    ai_functions: Mapping[str, AIFunctionSpec],
) -> tuple[DataStream, tuple[exp.Expression, ...]]:
    computations, rewritten = _ai_projection_plan(projections, ai_functions)
    if computations and stream.changelog_mode != frozenset({RowKind.INSERT}):
        raise SQLQueryError("SQL AI functions currently require an insert-only streaming input")
    for field_name, call, spec in computations:
        stream = stream.map_batches(
            _ApplyAsyncAIFunctionBatch if spec.is_async else _ApplyAIFunctionBatch,
            fn_constructor_args=[
                ai_function_call_name(call),
                spec,
                ai_function_arguments(call),
                functions,
                field_name,
            ],
            num_cpus=spec.resources.num_cpus,
            num_gpus=spec.resources.num_gpus,
            concurrency=spec.resources.concurrency,
            batch_size=spec.batch_size,
            batch_timeout=timedelta(seconds=spec.batch_timeout_seconds),
            batch_format="numpy",
            async_buffer_size=spec.async_buffer_size if spec.is_async else None,
            name=f"SQL{ai_function_call_name(call).upper()}",
        )
    return stream, rewritten


def _ai_projection_plan(
    projections: Sequence[exp.Expression],
    ai_functions: Mapping[str, AIFunctionSpec],
) -> tuple[list[tuple[str, exp.Expression, AIFunctionSpec]], tuple[exp.Expression, ...]]:
    computations: list[tuple[str, exp.Expression, AIFunctionSpec]] = []
    fields_by_call: dict[tuple[str, str], str] = {}
    rewritten: list[exp.Expression] = []
    for projection in projections:
        value = projection.this if isinstance(projection, exp.Alias) else projection
        if not is_ai_function_call(value):
            rewritten.append(projection)
            continue
        identifier = ai_function_call_name(value)
        if any(is_ray_data_only_expression(argument) for argument in ai_function_arguments(value)):
            raise SQLQueryError(f"{identifier.upper()} arguments cannot contain Ray-native-only SQL expressions")
        key = identifier, value.sql()
        field_name = fields_by_call.get(key)
        if field_name is None:
            field_name = f"_klein_ai_result_{len(computations)}"
            fields_by_call[key] = field_name
            try:
                spec = ai_functions[identifier]
            except KeyError as error:
                raise SQLQueryError(
                    f"Unregistered SQL AI function: {identifier.upper()}; call register_ai_function() first"
                ) from error
            computations.append((field_name, value, spec))
        output_name = projection.alias_or_name or projection.sql()
        rewritten.append(exp.alias_(exp.column(field_name), output_name, quoted=True))
    return computations, tuple(rewritten)


def _result_sequence(name: str, value: Any, expected: int) -> list[Any]:
    if isinstance(value, (str, bytes, bytearray, Mapping)) or not hasattr(value, "__iter__"):
        raise SQLQueryError(f"{name.upper()} backend must return one result sequence")
    results = list(value)
    if len(results) != expected:
        raise SQLQueryError(f"{name.upper()} backend returned {len(results)} results for {expected} calls")
    return results


def _python_scalar(value: Any) -> Any:
    if isinstance(value, pa.Scalar):
        return value.as_py()
    return value.item() if isinstance(value, np.generic) else value


def _batch_columns(batch: Mapping[str, Any] | pa.Table) -> dict[str, Any]:
    if isinstance(batch, pa.Table):
        return {name: batch.column(name) for name in batch.column_names}
    return dict(batch)
