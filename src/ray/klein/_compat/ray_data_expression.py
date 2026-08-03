# SPDX-License-Identifier: Apache-2.0
"""Ray Data 2.56 expression compatibility boundary.

Ray Data does not currently expose public hooks for evaluating one expression
against an externally supplied block or for applying its retrying filesystem
wrapper.  Keep those imports in one small adapter so a Ray minor upgrade has a
single review point and normal Klein modules never depend on private symbols.
"""

from __future__ import annotations

from typing import Any, cast

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
from ray.data.expressions import Expr


class RayDataExpressionRuntime:
    """Task-local adapter around Ray Data's block expression evaluator."""

    def __init__(self, expression: Expr, *, task_index: int, task_name: str) -> None:
        self._expression = expression
        self._task_context = TaskContext(task_idx=task_index, op_name=task_name)
        collector = _CallableClassUDFCollector()
        collector.visit(expression)
        for udf in collector.get_callable_class_udfs():
            udf.init()

    def evaluate(self, block: pa.Table) -> Any:
        """Evaluate the configured expression against one Arrow block."""

        with TaskContext.current(self._task_context):
            return eval_expr(self._expression, block)


def read_uri(uri: str, filesystem: Any, *, max_bytes: int | None = None) -> bytes | None:
    """Read one URI using Ray Data's filesystem resolution and retry policy.

    ``max_bytes`` bounds both the read request and retained result. Filesystems
    that expose metadata are rejected before opening an already-oversized
    object; unknown-size streams are read only through ``max_bytes + 1``.
    """

    if max_bytes is not None and (type(max_bytes) is not int or max_bytes <= 0):
        raise ValueError("max_bytes must be a positive integer or None")

    resolved_filesystem = _validate_and_wrap_filesystem(filesystem)
    paths, resolved_filesystem = _resolve_paths_and_filesystem(
        uri,
        filesystem=resolved_filesystem,
    )
    if not paths or resolved_filesystem is None:
        return None
    retrying_filesystem = RetryingPyFileSystem.wrap(
        resolved_filesystem,
        retryable_errors=DataContext.get_current().retried_io_errors,
    )
    path = paths[0]
    if max_bytes is not None:
        file_info = retrying_filesystem.get_file_info(path)
        if file_info.size is not None and file_info.size > max_bytes:
            raise OSError("DOWNLOAD object exceeds sql.download.max-bytes")
    with retrying_filesystem.open_input_stream(path) as stream:
        payload = cast(bytes, stream.read() if max_bytes is None else stream.read(max_bytes + 1))
    if max_bytes is not None and len(payload) > max_bytes:
        raise OSError("DOWNLOAD object exceeds sql.download.max-bytes")
    return payload
