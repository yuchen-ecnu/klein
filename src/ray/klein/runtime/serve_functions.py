# SPDX-License-Identifier: Apache-2.0
"""Logical-function instantiation for extracted Ray Serve regions."""

import inspect
from collections.abc import Callable, Sequence
from typing import Any

from ray.klein._internal.logging import get_logger
from ray.klein.api.functions.logical_function import LogicalFunction

logger = get_logger(__name__)


def _instantiate_operator(operator: Any, args, kwargs) -> Callable:
    try:
        if inspect.isclass(operator):
            return operator(*(args or []), **(kwargs or {}))
        if callable(operator):
            return operator
        raise TypeError(f"operator must be callable, got {type(operator).__name__}")
    except Exception as error:
        name = getattr(operator, "__name__", str(operator))
        logger.exception("Failed to instantiate operator %s", name)
        raise RuntimeError(f"Failed to instantiate operator {name}: {error}") from error


def instantiate_logical_functions(
    logical_functions: list[LogicalFunction],
) -> list[Callable]:
    """Instantiate an extracted in-memory operator chain."""
    operators: list[Callable] = []
    try:
        for function in logical_functions:
            operators.append(  # noqa: PERF401 - retain partial results for transactional cleanup
                _instantiate_operator(
                    function.function,
                    function.constructor_args,
                    function.constructor_kwargs,
                )
            )
    except BaseException:
        close_operators(operators)
        raise
    return operators


def close_operators(operators: Sequence[Callable], *, excluding: Sequence[Callable] = ()) -> None:
    """Best-effort close a replaced or partially constructed sync Serve chain."""

    retained_ids = {id(operator) for operator in excluding}
    closed_ids: set[int] = set()
    for operator in reversed(operators):
        operator_id = id(operator)
        if operator_id in retained_ids or operator_id in closed_ids:
            continue
        closed_ids.add(operator_id)
        close = getattr(operator, "close", None)
        if not callable(close):
            continue
        if inspect.iscoroutinefunction(close):
            logger.warning("Skipping async close() for Serve operator %r; only sync lifecycle is supported", operator)
            continue
        try:
            result = close()
            if inspect.isawaitable(result):
                if inspect.iscoroutine(result):
                    result.close()
                logger.warning(
                    "Ignoring awaitable returned by close() for Serve operator %r; only sync lifecycle is supported",
                    operator,
                )
        except BaseException:
            logger.exception("Failed to close Serve operator %r", operator)
