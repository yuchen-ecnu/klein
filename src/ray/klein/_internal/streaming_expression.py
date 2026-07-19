# SPDX-License-Identifier: Apache-2.0
"""Evaluate Ray Data 2.56 expressions inside Klein streaming operators."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

import numpy as np
import pandas as pd
import pyarrow as pa
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.planner.plan_expression.expression_evaluator import eval_expr
from ray.data._internal.planner.plan_expression.expression_visitors import (
    _CallableClassUDFCollector,
)
from ray.data._internal.util import RetryingPyFileSystem
from ray.data.context import DataContext
from ray.data.datasource.path_util import (
    _resolve_paths_and_filesystem,
    _validate_and_wrap_filesystem,
)
from ray.data.expressions import DownloadExpr, Expr

from ray.klein._internal.logging import get_logger
from ray.klein.api.changelog_row import ChangelogRow, row_kind_of

if TYPE_CHECKING:
    from ray.klein.api.runtime_context import RuntimeContext


logger = get_logger(__name__)

# A streaming expression has one request per input record. This bounds both the
# number of worker threads occupied by DOWNLOAD and the number of completed
# values retained while ordered emission waits for an earlier request.
DEFAULT_EXPRESSION_ASYNC_BUFFER_SIZE = 32


class StreamingExpressionEvaluator:
    """Task-local evaluator for one Ray Data expression."""

    def __init__(self, expression: Expr, runtime_context: RuntimeContext) -> None:
        if not isinstance(expression, Expr):
            raise TypeError(f"expression must be a Ray Data Expr, got {type(expression).__name__}")
        self._expression = expression
        self._task_context = TaskContext(
            task_idx=runtime_context.task_index,
            op_name=runtime_context.task_name,
        )
        udf_collector = _CallableClassUDFCollector()
        udf_collector.visit(expression)
        for udf in udf_collector.get_callable_class_udfs():
            udf.init()

    @property
    def is_async(self) -> bool:
        return isinstance(self._expression, DownloadExpr)

    def evaluate(self, row: Mapping[str, Any]) -> Any:
        """Evaluate a non-download expression against one logical row."""

        if self.is_async:
            raise TypeError("DownloadExpr requires evaluate_async()")
        block = pa.Table.from_pylist([dict(row)])
        with TaskContext.current(self._task_context):
            result = eval_expr(self._expression, block)
        return _first_value(result)

    async def evaluate_async(self, row: Mapping[str, Any]) -> Any:
        """Evaluate an expression without blocking the streaming actor loop."""

        if not isinstance(self._expression, DownloadExpr):
            return self.evaluate(row)
        try:
            uri = row[self._expression.uri_column_name]
        except KeyError as error:
            raise KeyError(
                f"DownloadExpr references missing URI column {self._expression.uri_column_name!r}"
            ) from error
        if uri is None:
            return None
        return await asyncio.to_thread(
            _download_uri,
            uri,
            self._expression.filesystem,
            self._expression.uri_column_name,
        )


class StreamingWithColumn:
    """Synchronous streaming implementation of ``Dataset.with_column``."""

    def __init__(self, name: str, expression: Expr, runtime_context: RuntimeContext) -> None:
        self._name = name
        self._evaluator = StreamingExpressionEvaluator(expression, runtime_context)
        if self._evaluator.is_async:
            raise TypeError("DownloadExpr requires AsyncStreamingWithColumn")

    def __call__(self, row: Mapping[str, Any]) -> ChangelogRow:
        result = dict(row)
        result[self._name] = self._evaluator.evaluate(row)
        return ChangelogRow(result, row_kind=row_kind_of(row))


class AsyncStreamingWithColumn:
    """Asynchronous streaming implementation of ``Dataset.with_column``."""

    def __init__(self, name: str, expression: Expr, runtime_context: RuntimeContext) -> None:
        self._name = name
        self._evaluator = StreamingExpressionEvaluator(expression, runtime_context)
        if not self._evaluator.is_async:
            raise TypeError("AsyncStreamingWithColumn requires DownloadExpr")

    async def __call__(self, row: Mapping[str, Any]) -> ChangelogRow:
        result = dict(row)
        result[self._name] = await self._evaluator.evaluate_async(row)
        return ChangelogRow(result, row_kind=row_kind_of(row))


class StreamingExpressionFilter:
    """Streaming implementation of ``Dataset.filter(expr=...)``."""

    def __init__(self, expression: Expr, runtime_context: RuntimeContext) -> None:
        self._evaluator = StreamingExpressionEvaluator(expression, runtime_context)
        if self._evaluator.is_async:
            raise TypeError("DownloadExpr cannot be used as a filter predicate")

    def __call__(self, row: Mapping[str, Any]) -> bool:
        return self._evaluator.evaluate(row) is True


def _first_value(value: Any) -> Any:
    if isinstance(value, (pa.Array, pa.ChunkedArray)):
        return value[0].as_py()
    if isinstance(value, pd.Series):
        return value.iloc[0]
    if isinstance(value, np.ndarray):
        result = value[0]
        return result.item() if isinstance(result, np.generic) else result
    if isinstance(value, pa.Scalar):
        return value.as_py()
    return value


def _download_uri(uri: Any, filesystem: Any, column_name: str) -> bytes | None:
    """Read one URI with the same soft-failure contract as Ray's Download op."""

    try:
        resolved_filesystem = _validate_and_wrap_filesystem(filesystem)
        paths, resolved_filesystem = _resolve_paths_and_filesystem(
            str(uri),
            filesystem=resolved_filesystem,
        )
        if not paths or resolved_filesystem is None:
            return None
        retrying_filesystem = RetryingPyFileSystem.wrap(
            resolved_filesystem,
            retryable_errors=DataContext.get_current().retried_io_errors,
        )
        with retrying_filesystem.open_input_stream(paths[0]) as stream:
            return stream.read()
    except OSError:
        logger.debug("OSError reading URI %r from column %r", uri, column_name, exc_info=True)
    except Exception:
        logger.warning("Unexpected error reading URI %r from column %r", uri, column_name, exc_info=True)
    return None
