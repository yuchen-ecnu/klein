# SPDX-License-Identifier: Apache-2.0
"""Session-local, engine-neutral SQL scalar-function bindings."""

from __future__ import annotations

import inspect
from collections.abc import Callable, Mapping
from typing import Any, cast

from sqlglot import exp

from ray.klein._internal.frozen_mapping import FrozenMapping
from ray.klein._internal.sql.validation import (
    referenced_scalar_function_calls,
    validate_scalar_function_name,
)
from ray.klein.api.sql_query_error import SQLQueryError

ScalarFunction = Callable[..., Any]


def contains_scalar_function(
    expression: exp.Expression,
    functions: Mapping[str, ScalarFunction],
) -> bool:
    """Whether an expression invokes one of the bound user functions."""

    return any(isinstance(node, exp.Anonymous) and node.name.casefold() in functions for node in expression.walk())


class ScalarFunctionRegistry:
    """Mutable session catalog that produces immutable per-query snapshots."""

    def __init__(self) -> None:
        self._functions: dict[str, ScalarFunction] = {}

    def register(self, name: str, function: ScalarFunction, *, replace: bool = False) -> None:
        validate_scalar_function_name(name)
        function = _validate_scalar_function(function)
        identifier = name.casefold()
        if identifier in self._functions and not replace:
            raise SQLQueryError(f"SQL scalar function {name!r} is already registered")
        self._functions[identifier] = function

    def drop(self, name: str) -> None:
        validate_scalar_function_name(name)
        try:
            del self._functions[name.casefold()]
        except KeyError as error:
            raise SQLQueryError(f"Unknown SQL scalar function {name!r}") from error

    @property
    def identifiers(self) -> tuple[str, ...]:
        return tuple(sorted(self._functions))

    def snapshot(self) -> Mapping[str, ScalarFunction]:
        """Return an immutable copy of every registered binding."""

        return cast(Mapping[str, ScalarFunction], FrozenMapping(self._functions))

    def bind(
        self,
        query: exp.Expression,
        overrides: Mapping[str, ScalarFunction] | None = None,
    ) -> Mapping[str, ScalarFunction]:
        """Resolve and validate only the functions referenced by one query."""

        available = dict(self._functions)
        for name, override_function in (overrides or {}).items():
            validate_scalar_function_name(name)
            available[name.casefold()] = _validate_scalar_function(override_function)

        bound: dict[str, ScalarFunction] = {}
        unknown: set[str] = set()
        for call in referenced_scalar_function_calls(query):
            identifier = call.name.casefold()
            bound_function = available.get(identifier)
            if bound_function is None:
                unknown.add(call.name)
                continue
            _validate_call_arity(call, bound_function)
            bound[identifier] = bound_function
        if unknown:
            names = ", ".join(sorted(unknown, key=str.casefold))
            registered = ", ".join(sorted(available)) or "none"
            raise SQLQueryError(
                f"Unknown SQL scalar function(s): {names}; call register_scalar_function() "
                f"or pass a functions= mapping. Registered functions: {registered}"
            )
        return cast(Mapping[str, ScalarFunction], FrozenMapping(bound))


def _validate_scalar_function(function: Any) -> ScalarFunction:
    if not callable(function):
        raise TypeError("SQL scalar function must be callable")
    call = type(function).__call__
    if (
        inspect.iscoroutinefunction(function)
        or inspect.iscoroutinefunction(call)
        or inspect.isasyncgenfunction(function)
        or inspect.isasyncgenfunction(call)
    ):
        raise TypeError("SQL scalar function must be synchronous")
    return cast(ScalarFunction, function)


def _validate_call_arity(call: exp.Anonymous, function: ScalarFunction) -> None:
    try:
        signature = inspect.signature(function)
    except (TypeError, ValueError):
        return
    try:
        signature.bind(*([None] * len(call.expressions)))
    except TypeError as error:
        raise SQLQueryError(f"Invalid call to SQL scalar function {call.name!r}: {error}") from error
