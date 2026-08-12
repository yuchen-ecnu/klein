# SPDX-License-Identifier: Apache-2.0
"""Provider-neutral SQL AI-function registration and query binding."""

from __future__ import annotations

import inspect
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, cast

from sqlglot import exp

from ray.klein._internal.frozen_mapping import FrozenMapping
from ray.klein._internal.sql.media_function import is_media_function_call
from ray.klein._internal.sql.ray_data_expression import is_ray_data_only_node
from ray.klein.api.sql_query_error import SQLQueryError
from ray.klein.runtime.resources import Resources

AIBackend = Callable[[list[tuple[Any, ...]]], Any]

_SUPPORTED_AI_EXPRESSIONS: dict[type[exp.Expression], str] = {
    exp.AIGenerate: "ai_generate",
    exp.AIEmbed: "ai_embed",
}
_UNSUPPORTED_AI_EXPRESSIONS: dict[type[exp.Expression], str] = {
    exp.AIClassify: "AI_CLASSIFY",
    exp.AISimilarity: "AI_SIMILARITY",
    exp.AIAgg: "AI_AGG",
    exp.AISummarizeAgg: "AI_SUMMARIZE_AGG",
}


@dataclass(frozen=True, slots=True)
class AIFunctionSpec:
    """Immutable worker-side recipe for one batched AI capability."""

    function: Any
    constructor_args: tuple[Any, ...]
    constructor_kwargs: Mapping[str, Any]
    resources: Resources
    batch_size: int
    batch_timeout_seconds: float
    async_buffer_size: int | None
    is_async: bool


class AIFunctionRegistry:
    """Mutable session catalog that produces immutable per-query AI bindings."""

    def __init__(self) -> None:
        self._functions: dict[str, AIFunctionSpec] = {}

    def register(
        self,
        name: str,
        function: Any,
        *,
        fn_constructor_args: Iterable[Any] | None = None,
        fn_constructor_kwargs: Mapping[str, Any] | None = None,
        num_cpus: float | None = None,
        num_gpus: float | None = None,
        concurrency: int | tuple[int, int] | None = None,
        batch_size: int = 32,
        batch_timeout: timedelta = timedelta(seconds=3),
        async_buffer_size: int | None = None,
        replace: bool = False,
    ) -> None:
        identifier = _validate_ai_function_name(name)
        if not callable(function):
            raise TypeError("SQL AI function backend must be callable")
        constructor_args = tuple(fn_constructor_args or ())
        constructor_kwargs = dict(fn_constructor_kwargs or {})
        if not isinstance(function, type) and (constructor_args or constructor_kwargs):
            raise TypeError("fn_constructor_args and fn_constructor_kwargs require a callable class")
        if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size <= 0:
            raise ValueError("batch_size must be a positive integer")
        if not isinstance(batch_timeout, timedelta) or batch_timeout.total_seconds() <= 0:
            raise ValueError("batch_timeout must be a positive timedelta")
        if async_buffer_size is not None and (
            isinstance(async_buffer_size, bool) or not isinstance(async_buffer_size, int) or async_buffer_size <= 0
        ):
            raise ValueError("async_buffer_size must be a positive integer or None")
        is_async = _is_async_callable(function)
        if _is_async_generator_callable(function):
            raise TypeError("SQL AI function backend must return one result sequence, not an async generator")
        if not is_async and async_buffer_size is not None:
            raise ValueError("async_buffer_size is supported only for async SQL AI function backends")
        if is_async and async_buffer_size is None:
            async_buffer_size = 8
        if identifier in self._functions and not replace:
            raise SQLQueryError(f"SQL AI function {name!r} is already registered")
        self._functions[identifier] = AIFunctionSpec(
            function=function,
            constructor_args=constructor_args,
            constructor_kwargs=FrozenMapping(constructor_kwargs),
            resources=Resources(num_cpus, num_gpus, concurrency),
            batch_size=batch_size,
            batch_timeout_seconds=batch_timeout.total_seconds(),
            async_buffer_size=async_buffer_size,
            is_async=is_async,
        )

    def drop(self, name: str) -> None:
        identifier = _validate_ai_function_name(name)
        try:
            del self._functions[identifier]
        except KeyError as error:
            raise SQLQueryError(f"Unknown SQL AI function {name!r}") from error

    @property
    def identifiers(self) -> tuple[str, ...]:
        return tuple(sorted(self._functions))

    def snapshot(self) -> Mapping[str, AIFunctionSpec]:
        return cast(Mapping[str, AIFunctionSpec], FrozenMapping(self._functions))

    def inherit(self, functions: Mapping[str, AIFunctionSpec]) -> None:
        """Copy immutable bindings into a fresh one-query session."""

        self._functions.update(functions)

    def bind(self, query: exp.Expression) -> Mapping[str, AIFunctionSpec]:
        """Validate AI syntax and return only capabilities referenced by a query."""

        for node in query.walk():
            unsupported = _UNSUPPORTED_AI_EXPRESSIONS.get(type(node))
            if unsupported is not None:
                raise SQLQueryError(
                    f"{unsupported} is not supported yet; the first AI SQL capabilities are AI_GENERATE and AI_EMBED"
                )

        bound: dict[str, AIFunctionSpec] = {}
        unknown: set[str] = set()
        for call in referenced_ai_function_calls(query):
            identifier = ai_function_call_name(call)
            _validate_ai_call(call, identifier)
            spec = self._functions.get(identifier)
            if spec is None:
                unknown.add(identifier.upper())
            else:
                bound[identifier] = spec
        if unknown:
            names = ", ".join(sorted(unknown))
            raise SQLQueryError(f"Unregistered SQL AI function(s): {names}; call register_ai_function() first")
        return cast(Mapping[str, AIFunctionSpec], FrozenMapping(bound))


def referenced_ai_function_calls(query: exp.Expression) -> tuple[exp.Expression, ...]:
    return tuple(node for node in query.walk() if type(node) in _SUPPORTED_AI_EXPRESSIONS)


def ai_function_call_name(expression: exp.Expression) -> str:
    try:
        return _SUPPORTED_AI_EXPRESSIONS[type(expression)]
    except KeyError as error:
        raise TypeError(f"Not a supported SQL AI expression: {type(expression).__name__}") from error


def ai_function_arguments(expression: exp.Expression) -> tuple[exp.Expression, ...]:
    return tuple(expression.iter_expressions())


def is_ai_function_call(expression: exp.Expression) -> bool:
    return type(expression) in _SUPPORTED_AI_EXPRESSIONS


def _validate_ai_function_name(name: str) -> str:
    if not isinstance(name, str):
        raise TypeError("SQL AI function name must be a string")
    identifier = name.casefold()
    if identifier not in set(_SUPPORTED_AI_EXPRESSIONS.values()):
        supported = ", ".join(sorted(value.upper() for value in _SUPPORTED_AI_EXPRESSIONS.values()))
        raise SQLQueryError(f"Unsupported SQL AI function name {name!r}; supported names: {supported}")
    return identifier


def _validate_ai_call(call: exp.Expression, identifier: str) -> None:
    arguments = ai_function_arguments(call)
    if len(arguments) not in {1, 2}:
        raise SQLQueryError(f"{identifier.upper()} requires one input and accepts one optional config argument")
    for argument in arguments:
        for node in argument.walk():
            if isinstance(node, (exp.Star, exp.StarMap)):
                raise SQLQueryError(f"{identifier.upper()} arguments cannot contain wildcard expressions")
            if not is_ray_data_only_node(node):
                continue
            parent = node.parent
            if (
                isinstance(node, exp.Anonymous)
                and node.name.casefold() == "download"
                and isinstance(parent, exp.Anonymous)
                and is_media_function_call(parent)
                and parent.expressions
                and parent.expressions[0] is node
            ):
                continue
            raise SQLQueryError(f"{identifier.upper()} arguments cannot contain Ray-native-only SQL expressions")
    parent = call.parent
    if isinstance(parent, exp.Alias):
        parent = parent.parent
    if not isinstance(parent, exp.Select):
        raise SQLQueryError(f"{identifier.upper()} must be a top-level SELECT expression")
    if parent.args.get("group") is not None or parent.find(exp.AggFunc) is not None:
        raise SQLQueryError(f"{identifier.upper()} cannot be used in an aggregate query")


def _call_method(function: Any) -> Any:
    if isinstance(function, type):
        return function.__call__
    return type(function).__call__


def _is_async_callable(function: Any) -> bool:
    return inspect.iscoroutinefunction(function) or inspect.iscoroutinefunction(_call_method(function))


def _is_async_generator_callable(function: Any) -> bool:
    return inspect.isasyncgenfunction(function) or inspect.isasyncgenfunction(_call_method(function))
