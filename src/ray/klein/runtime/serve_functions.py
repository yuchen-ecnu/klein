# SPDX-License-Identifier: Apache-2.0
"""Logical-function instantiation for extracted Ray Serve regions."""

import inspect
from collections.abc import Callable
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
    return [
        _instantiate_operator(
            function.function,
            function.constructor_args,
            function.constructor_kwargs,
        )
        for function in logical_functions
    ]
