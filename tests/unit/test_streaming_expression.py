# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, Mock

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pytest
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data.datatype import DataType
from ray.data.expressions import col, download, udf
from sqlglot import parse_one

from ray.klein._internal import streaming_expression
from ray.klein._internal.sql.ray_data_expression import to_ray_data_expression
from ray.klein._internal.streaming_expression import (
    AsyncStreamingWithColumn,
    StreamingExpressionEvaluator,
    StreamingExpressionFilter,
    StreamingWithColumn,
)
from ray.klein.api.changelog_row import ChangelogRow
from ray.klein.api.row_kind import RowKind


def _runtime_context(*, task_index: int = 7, task_name: str = "Map") -> SimpleNamespace:
    return SimpleNamespace(task_index=task_index, task_name=task_name)


def test_evaluator_runs_ray_expression_in_the_stream_task_context(monkeypatch) -> None:
    captured = {}

    def evaluate(expression, block):
        current = TaskContext.get_current()
        captured.update(
            expression=expression,
            rows=block.to_pylist(),
            task_index=current.task_idx,
            task_name=current.op_name,
        )
        return pa.array([9])

    expression = col("amount") * 2 + 1
    monkeypatch.setattr(streaming_expression, "eval_expr", evaluate)
    evaluator = StreamingExpressionEvaluator(expression, _runtime_context())

    assert evaluator.evaluate({"amount": 4}) == 9
    assert captured == {
        "expression": expression,
        "rows": [{"amount": 4}],
        "task_index": 7,
        "task_name": "Map",
    }
    assert TaskContext.get_current() is None


def test_task_context_is_reset_when_expression_evaluation_fails(monkeypatch) -> None:
    def fail(_expression, _block):
        assert TaskContext.get_current() is not None
        raise ValueError("invalid value")

    monkeypatch.setattr(streaming_expression, "eval_expr", fail)
    evaluator = StreamingExpressionEvaluator(col("value"), _runtime_context())

    with pytest.raises(ValueError, match="invalid value"):
        evaluator.evaluate({"value": 1})

    assert TaskContext.get_current() is None


def test_callable_class_udf_is_initialized_once_per_evaluator() -> None:
    initialized = []

    @udf(return_dtype=DataType.int64())
    class AddOffset:
        def __init__(self, offset: int) -> None:
            initialized.append(offset)
            self._offset = offset

        def __call__(self, values: pa.Array) -> pa.Array:
            return pc.add(values, self._offset)

    evaluator = StreamingExpressionEvaluator(AddOffset(3)(col("value")), _runtime_context())

    assert initialized == [3]
    assert evaluator.evaluate({"value": 4}) == 7
    assert initialized == [3]


def test_sql_arithmetic_expression_executes_through_ray_data_evaluator() -> None:
    expression = to_ray_data_expression(parse_one("amount * 2 + 1"), ("orders",))

    assert expression is not None
    evaluator = StreamingExpressionEvaluator(expression, _runtime_context())
    assert evaluator.evaluate({"orders.amount": 4}) == 9


def test_evaluator_rejects_non_ray_expression() -> None:
    with pytest.raises(TypeError, match="Ray Data Expr, got Add"):
        StreamingExpressionEvaluator(parse_one("value + 1"), _runtime_context())


@pytest.mark.asyncio
async def test_non_download_expression_can_use_async_evaluator_api() -> None:
    evaluator = StreamingExpressionEvaluator(col("value") + 1, _runtime_context())

    assert evaluator.is_async is False
    assert await evaluator.evaluate_async({"value": 4}) == 5


@pytest.mark.asyncio
async def test_download_expression_is_offloaded_with_uri_and_filesystem(monkeypatch) -> None:
    filesystem = object()
    expression = download("uri", filesystem=filesystem)
    downloaded = Mock(return_value=b"body")
    offloads = []

    async def to_thread(function, *args):
        offloads.append((function, args))
        return function(*args)

    monkeypatch.setattr(streaming_expression, "_download_uri", downloaded)
    monkeypatch.setattr(streaming_expression.asyncio, "to_thread", to_thread)
    evaluator = StreamingExpressionEvaluator(expression, _runtime_context())

    assert evaluator.is_async is True
    assert await evaluator.evaluate_async({"uri": "memory://payload"}) == b"body"
    assert offloads == [(downloaded, ("memory://payload", filesystem, "uri"))]

    with pytest.raises(TypeError, match=r"requires evaluate_async\(\)"):
        evaluator.evaluate({"uri": "memory://payload"})


@pytest.mark.asyncio
async def test_download_none_short_circuits_and_missing_column_is_actionable(monkeypatch) -> None:
    evaluator = StreamingExpressionEvaluator(download("uri"), _runtime_context())
    offload = Mock()
    monkeypatch.setattr(streaming_expression.asyncio, "to_thread", offload)

    assert await evaluator.evaluate_async({"uri": None}) is None
    offload.assert_not_called()

    with pytest.raises(KeyError, match="missing URI column 'uri'"):
        await evaluator.evaluate_async({"other": "value"})


def test_sync_projection_preserves_changelog_kind() -> None:
    projection = StreamingWithColumn("doubled", col("value") * 2, _runtime_context())

    result = projection(ChangelogRow.delete({"value": 4}))

    assert result == {"value": 4, "doubled": 8}
    assert result.row_kind is RowKind.DELETE


@pytest.mark.asyncio
async def test_async_download_projection_preserves_changelog_kind(monkeypatch) -> None:
    monkeypatch.setattr(streaming_expression, "_download_uri", Mock(return_value=b"body"))
    projection = AsyncStreamingWithColumn("body", download("uri"), _runtime_context())

    result = await projection(ChangelogRow.update_after({"uri": "memory://payload"}))

    assert result == {"uri": "memory://payload", "body": b"body"}
    assert result.row_kind is RowKind.UPDATE_AFTER


def test_projection_classes_reject_the_wrong_execution_mode() -> None:
    with pytest.raises(TypeError, match="requires AsyncStreamingWithColumn"):
        StreamingWithColumn("body", download("uri"), _runtime_context())

    with pytest.raises(TypeError, match="requires DownloadExpr"):
        AsyncStreamingWithColumn("value", col("value") + 1, _runtime_context())


def test_expression_filter_keeps_only_exact_sql_true() -> None:
    predicate = StreamingExpressionFilter(col("value") > 1, _runtime_context())

    assert predicate({"value": 2}) is True
    assert predicate({"value": 1}) is False
    assert predicate({"value": None}) is False


def test_expression_filter_rejects_download_expression() -> None:
    with pytest.raises(TypeError, match="cannot be used as a filter predicate"):
        StreamingExpressionFilter(download("uri"), _runtime_context())


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (pa.array([1]), 1),
        (pa.chunked_array([[2]]), 2),
        (pd.Series([3]), 3),
        (np.array([np.int64(4)]), 4),
        (np.array([{"value": 5}], dtype=object), {"value": 5}),
        (pa.scalar(6), 6),
        ("already scalar", "already scalar"),
    ],
)
def test_first_value_normalizes_supported_ray_block_results(value, expected) -> None:
    assert streaming_expression._first_value(value) == expected


def _configure_download_filesystem(monkeypatch, *, paths, resolved_filesystem):
    filesystem = object()
    validated = object()
    retrying = Mock()
    retrying_errors = [OSError]
    monkeypatch.setattr(streaming_expression, "_validate_and_wrap_filesystem", Mock(return_value=validated))
    resolve = Mock(return_value=(paths, resolved_filesystem))
    monkeypatch.setattr(streaming_expression, "_resolve_paths_and_filesystem", resolve)
    wrap = Mock(return_value=retrying)
    monkeypatch.setattr(streaming_expression.RetryingPyFileSystem, "wrap", wrap)
    monkeypatch.setattr(
        streaming_expression.DataContext,
        "get_current",
        Mock(return_value=SimpleNamespace(retried_io_errors=retrying_errors)),
    )
    return filesystem, validated, retrying, retrying_errors, resolve, wrap


def test_download_uri_reads_first_resolved_path(monkeypatch) -> None:
    resolved = object()
    filesystem, validated, retrying, retrying_errors, resolve, wrap = _configure_download_filesystem(
        monkeypatch,
        paths=["first", "second"],
        resolved_filesystem=resolved,
    )
    stream = MagicMock()
    stream.__enter__.return_value.read.return_value = b"payload"
    retrying.open_input_stream.return_value = stream

    assert streaming_expression._download_uri(42, filesystem, "uri") == b"payload"
    resolve.assert_called_once_with("42", filesystem=validated)
    wrap.assert_called_once_with(resolved, retryable_errors=retrying_errors)
    retrying.open_input_stream.assert_called_once_with("first")


@pytest.mark.parametrize(
    ("paths", "resolved_filesystem"),
    [([], object()), (["payload"], None)],
)
def test_download_uri_returns_none_when_resolution_has_no_readable_target(
    monkeypatch,
    paths,
    resolved_filesystem,
) -> None:
    _filesystem, _validated, _retrying, _errors, _resolve, wrap = _configure_download_filesystem(
        monkeypatch,
        paths=paths,
        resolved_filesystem=resolved_filesystem,
    )

    assert streaming_expression._download_uri("missing", None, "uri") is None
    wrap.assert_not_called()


@pytest.mark.parametrize(
    ("error", "log_method"),
    [(OSError("not found"), "debug"), (RuntimeError("broken plugin"), "warning")],
)
def test_download_uri_soft_fails_and_logs_by_error_type(monkeypatch, error: Exception, log_method: str) -> None:
    logger_method = Mock()
    monkeypatch.setattr(streaming_expression, "_validate_and_wrap_filesystem", Mock(side_effect=error))
    monkeypatch.setattr(streaming_expression.logger, log_method, logger_method)

    assert streaming_expression._download_uri("bad://uri", None, "uri") is None
    logger_method.assert_called_once()
