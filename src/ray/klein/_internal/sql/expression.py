# SPDX-License-Identifier: Apache-2.0
"""Row-level SQL expression evaluation for Ray Dataset transforms."""

from __future__ import annotations

import inspect
import math
import operator
import re
from collections.abc import AsyncIterator, Callable, Mapping
from functools import singledispatch
from typing import Any

from sqlglot import exp

from ray.klein._internal.sql.scalar_function_registry import ScalarFunction
from ray.klein.api.sql_query_error import SQLQueryError


def evaluate_expression(
    expression: exp.Expression,
    row: Mapping[str, Any],
    functions: Mapping[str, ScalarFunction] | None = None,
) -> Any:
    """Evaluate a supported SQLGlot expression against one logical row."""

    return _evaluate(expression, row, functions or {})


@singledispatch
def _evaluate(
    expression: exp.Expression,
    _row: Mapping[str, Any],
    _functions: Mapping[str, ScalarFunction],
) -> Any:
    raise SQLQueryError(f"Unsupported SQL expression: {expression.sql()}")


@_evaluate.register(exp.Alias)
@_evaluate.register(exp.Paren)
def _evaluate_wrapped(
    expression: exp.Expression,
    row: Mapping[str, Any],
    functions: Mapping[str, ScalarFunction],
) -> Any:
    return _evaluate(expression.this, row, functions)


@_evaluate.register
def _evaluate_column(
    column: exp.Column,
    row: Mapping[str, Any],
    _functions: Mapping[str, ScalarFunction],
) -> Any:
    name = column.name
    if column.table:
        qualified = f"{column.table}.{name}"
        if qualified in row:
            return row[qualified]
    if name in row:
        return row[name]
    matches = [value for key, value in row.items() if key.endswith(f".{name}")]
    if len(matches) == 1:
        return matches[0]
    if not matches:
        raise SQLQueryError(f"Unknown SQL column {column.sql()!r}")
    raise SQLQueryError(f"Ambiguous SQL column {column.sql()!r}; qualify it with a table alias")


@_evaluate.register
def _evaluate_null(
    _expression: exp.Null,
    _row: Mapping[str, Any],
    _functions: Mapping[str, ScalarFunction],
) -> None:
    return None


@_evaluate.register
def _evaluate_boolean_literal(
    expression: exp.Boolean,
    _row: Mapping[str, Any],
    _functions: Mapping[str, ScalarFunction],
) -> bool:
    return bool(expression.this)


@_evaluate.register
def _evaluate_literal(
    expression: exp.Literal,
    _row: Mapping[str, Any],
    _functions: Mapping[str, ScalarFunction],
) -> Any:
    if expression.is_string:
        return expression.this
    try:
        return int(expression.this)
    except ValueError:
        return float(expression.this)


@_evaluate.register
def _evaluate_negation(
    expression: exp.Neg,
    row: Mapping[str, Any],
    functions: Mapping[str, ScalarFunction],
) -> Any:
    value = _evaluate(expression.this, row, functions)
    return None if value is None else -value


@_evaluate.register
def _evaluate_not(
    expression: exp.Not,
    row: Mapping[str, Any],
    functions: Mapping[str, ScalarFunction],
) -> bool | None:
    value = _evaluate(expression.this, row, functions)
    return None if value is None else not value


_ARITHMETIC_OPERATIONS: dict[type[exp.Binary], Callable[[Any, Any], Any]] = {
    exp.Add: operator.add,
    exp.Sub: operator.sub,
    exp.Mul: operator.mul,
    exp.Div: operator.truediv,
    exp.Mod: operator.mod,
}


def _evaluate_arithmetic(
    expression: exp.Binary,
    row: Mapping[str, Any],
    functions: Mapping[str, ScalarFunction],
) -> Any:
    left = _evaluate(expression.this, row, functions)
    right = _evaluate(expression.expression, row, functions)
    if left is None or right is None:
        return None
    return _ARITHMETIC_OPERATIONS[type(expression)](left, right)


for _expression_type in _ARITHMETIC_OPERATIONS:
    _evaluate.register(_expression_type)(_evaluate_arithmetic)


@_evaluate.register
def _evaluate_power(
    expression: exp.Pow,
    row: Mapping[str, Any],
    functions: Mapping[str, ScalarFunction],
) -> Any:
    base = _evaluate(expression.this, row, functions)
    exponent = _evaluate(expression.expression, row, functions)
    if base is None or exponent is None:
        return None
    return base**exponent


_COMPARISON_OPERATIONS: dict[type[exp.Binary], Callable[[Any, Any], bool]] = {
    exp.EQ: operator.eq,
    exp.NEQ: operator.ne,
    exp.GT: operator.gt,
    exp.GTE: operator.ge,
    exp.LT: operator.lt,
    exp.LTE: operator.le,
}


def _evaluate_comparison(
    expression: exp.Binary,
    row: Mapping[str, Any],
    functions: Mapping[str, ScalarFunction],
) -> bool | None:
    left = _evaluate(expression.this, row, functions)
    right = _evaluate(expression.expression, row, functions)
    if left is None or right is None:
        return None
    return _COMPARISON_OPERATIONS[type(expression)](left, right)


for _expression_type in _COMPARISON_OPERATIONS:
    _evaluate.register(_expression_type)(_evaluate_comparison)


@_evaluate.register
def _evaluate_and(
    expression: exp.And,
    row: Mapping[str, Any],
    functions: Mapping[str, ScalarFunction],
) -> bool | None:
    left = _evaluate(expression.this, row, functions)
    right = _evaluate(expression.expression, row, functions)
    if left is False or right is False:
        return False
    if left is None or right is None:
        return None
    return bool(left and right)


@_evaluate.register
def _evaluate_or(
    expression: exp.Or,
    row: Mapping[str, Any],
    functions: Mapping[str, ScalarFunction],
) -> bool | None:
    left = _evaluate(expression.this, row, functions)
    right = _evaluate(expression.expression, row, functions)
    if left is True or right is True:
        return True
    if left is None or right is None:
        return None
    return bool(left or right)


@_evaluate.register
def _evaluate_is(
    expression: exp.Is,
    row: Mapping[str, Any],
    functions: Mapping[str, ScalarFunction],
) -> bool:
    left = _evaluate(expression.this, row, functions)
    right = expression.expression
    if isinstance(right, exp.Null):
        return left is None
    return left is _evaluate(right, row, functions)


@_evaluate.register
def _evaluate_between(
    expression: exp.Between,
    row: Mapping[str, Any],
    functions: Mapping[str, ScalarFunction],
) -> bool | None:
    value = _evaluate(expression.this, row, functions)
    low = _evaluate(expression.args["low"], row, functions)
    high = _evaluate(expression.args["high"], row, functions)
    if value is None or low is None or high is None:
        return None
    return bool(low <= value <= high)


@_evaluate.register
def _evaluate_in(
    expression: exp.In,
    row: Mapping[str, Any],
    functions: Mapping[str, ScalarFunction],
) -> bool | None:
    if expression.args.get("query") is not None:
        raise SQLQueryError("SQL IN subqueries are not supported")
    value = _evaluate(expression.this, row, functions)
    if value is None:
        return None
    choices = [_evaluate(choice, row, functions) for choice in expression.expressions]
    if value in choices:
        return True
    return None if None in choices else False


@_evaluate.register(exp.Like)
@_evaluate.register(exp.ILike)
def _evaluate_like(
    expression: exp.Expression,
    row: Mapping[str, Any],
    functions: Mapping[str, ScalarFunction],
) -> bool | None:
    value = _evaluate(expression.this, row, functions)
    pattern = _evaluate(expression.expression, row, functions)
    if value is None or pattern is None:
        return None
    flags = re.IGNORECASE if isinstance(expression, exp.ILike) else 0
    regex = (
        "^" + "".join({"%": ".*", "_": "."}.get(character, re.escape(character)) for character in str(pattern)) + "$"
    )
    return re.match(regex, str(value), flags) is not None


@_evaluate.register
def _evaluate_coalesce(
    expression: exp.Coalesce,
    row: Mapping[str, Any],
    functions: Mapping[str, ScalarFunction],
) -> Any:
    for candidate in (expression.this, *expression.expressions):
        value = _evaluate(candidate, row, functions)
        if value is not None:
            return value
    return None


@_evaluate.register(exp.Lower)
@_evaluate.register(exp.Upper)
def _evaluate_text_case(
    expression: exp.Expression,
    row: Mapping[str, Any],
    functions: Mapping[str, ScalarFunction],
) -> str | None:
    value = _evaluate(expression.this, row, functions)
    if value is None:
        return None
    text = str(value)
    return text.lower() if isinstance(expression, exp.Lower) else text.upper()


def _sign(value: Any) -> int:
    return int((value > 0) - (value < 0))


_NUMERIC_OPERATIONS: dict[type[exp.Expression], Callable[[Any], Any]] = {
    exp.Abs: abs,
    exp.Acos: math.acos,
    exp.Asin: math.asin,
    exp.Atan: math.atan,
    exp.Ceil: math.ceil,
    exp.Cos: math.cos,
    exp.Exp: math.exp,
    exp.Floor: math.floor,
    exp.Ln: math.log,
    exp.Round: round,
    exp.Sign: _sign,
    exp.Sin: math.sin,
    exp.Tan: math.tan,
    exp.Trunc: math.trunc,
}


def _evaluate_numeric(
    expression: exp.Expression,
    row: Mapping[str, Any],
    functions: Mapping[str, ScalarFunction],
) -> Any:
    value = _evaluate(expression.this, row, functions)
    if value is None:
        return None
    if expression.args.get("decimals") is not None:
        raise SQLQueryError(f"Unsupported SQL expression: {expression.sql()}")
    return _NUMERIC_OPERATIONS[type(expression)](value)


for _expression_type in _NUMERIC_OPERATIONS:
    _evaluate.register(_expression_type)(_evaluate_numeric)


@_evaluate.register
def _evaluate_logarithm(
    expression: exp.Log,
    row: Mapping[str, Any],
    functions: Mapping[str, ScalarFunction],
) -> Any:
    operand_expression = expression.args.get("expression")
    if operand_expression is None:
        value = _evaluate(expression.this, row, functions)
        return None if value is None else math.log(value)
    base = _evaluate(expression.this, row, functions)
    value = _evaluate(operand_expression, row, functions)
    if base is None or value is None:
        return None
    if base not in {2, 10}:
        raise SQLQueryError(f"Unsupported SQL expression: {expression.sql()}")
    return math.log(value, base)


@_evaluate.register
def _evaluate_cast(
    expression: exp.Cast,
    row: Mapping[str, Any],
    functions: Mapping[str, ScalarFunction],
) -> Any:
    value = _evaluate(expression.this, row, functions)
    if value is None:
        return None
    target = expression.to.sql().upper()
    conversions = (
        (("INT", "BIGINT", "SMALLINT", "TINYINT"), int),
        (("FLOAT", "DOUBLE", "DECIMAL"), float),
        (("STRING", "TEXT", "VARCHAR"), str),
        (("BOOL",), _cast_boolean),
    )
    for prefixes, converter in conversions:
        if target.startswith(prefixes):
            return converter(value)
    raise SQLQueryError(f"Unsupported SQL CAST target {target!r}")


def _cast_boolean(value: Any) -> bool:
    if not isinstance(value, str):
        return bool(value)
    normalized = value.strip().lower()
    if normalized in {"true", "1"}:
        return True
    if normalized in {"false", "0"}:
        return False
    raise SQLQueryError(f"Cannot CAST {value!r} to BOOLEAN")


@_evaluate.register
def _evaluate_case(
    expression: exp.Case,
    row: Mapping[str, Any],
    functions: Mapping[str, ScalarFunction],
) -> Any:
    searched_case = expression.this is None
    base = None if searched_case else _evaluate(expression.this, row, functions)
    for condition in expression.args.get("ifs") or ():
        matched = _evaluate(condition.this, row, functions)
        if (searched_case and matched is True) or (not searched_case and base is not None and matched == base):
            return _evaluate(condition.args["true"], row, functions)
    default = expression.args.get("default")
    return None if default is None else _evaluate(default, row, functions)


@_evaluate.register
def _evaluate_scalar_function(
    expression: exp.Anonymous,
    row: Mapping[str, Any],
    functions: Mapping[str, ScalarFunction],
) -> Any:
    try:
        function = functions[expression.name.casefold()]
    except KeyError as error:
        raise SQLQueryError(f"Unsupported SQL expression: {expression.sql()}") from error
    arguments = [_evaluate(argument, row, functions) for argument in expression.expressions]
    try:
        result = function(*arguments)
    except Exception as error:
        raise SQLQueryError(f"SQL scalar function {expression.name!r} failed: {error}") from error
    if inspect.isawaitable(result):
        if inspect.iscoroutine(result):
            result.close()
        raise SQLQueryError(
            f"SQL scalar function {expression.name!r} returned an awaitable; scalar functions must be synchronous"
        )
    if isinstance(result, AsyncIterator):
        raise SQLQueryError(
            f"SQL scalar function {expression.name!r} returned an async iterator; "
            "scalar functions must return synchronous scalar values"
        )
    return result
