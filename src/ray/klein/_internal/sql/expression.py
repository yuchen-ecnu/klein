# SPDX-License-Identifier: Apache-2.0
"""Row-level SQL expression evaluation for Ray Dataset transforms."""

from __future__ import annotations

import operator
import re
from collections.abc import Callable, Mapping
from functools import singledispatch
from typing import Any

from sqlglot import exp

from ray.klein.api.sql_query_error import SQLQueryError


def evaluate_expression(expression: exp.Expression, row: Mapping[str, Any]) -> Any:
    """Evaluate a supported SQLGlot expression against one logical row."""

    return _evaluate(expression, row)


@singledispatch
def _evaluate(expression: exp.Expression, _row: Mapping[str, Any]) -> Any:
    raise SQLQueryError(f"Unsupported SQL expression: {expression.sql()}")


@_evaluate.register(exp.Alias)
@_evaluate.register(exp.Paren)
def _evaluate_wrapped(expression: exp.Expression, row: Mapping[str, Any]) -> Any:
    return _evaluate(expression.this, row)


@_evaluate.register
def _evaluate_column(column: exp.Column, row: Mapping[str, Any]) -> Any:
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
def _evaluate_null(_expression: exp.Null, _row: Mapping[str, Any]) -> None:
    return None


@_evaluate.register
def _evaluate_boolean_literal(expression: exp.Boolean, _row: Mapping[str, Any]) -> bool:
    return expression.this


@_evaluate.register
def _evaluate_literal(expression: exp.Literal, _row: Mapping[str, Any]) -> Any:
    if expression.is_string:
        return expression.this
    try:
        return int(expression.this)
    except ValueError:
        return float(expression.this)


@_evaluate.register
def _evaluate_negation(expression: exp.Neg, row: Mapping[str, Any]) -> Any:
    value = _evaluate(expression.this, row)
    return None if value is None else -value


@_evaluate.register
def _evaluate_not(expression: exp.Not, row: Mapping[str, Any]) -> bool | None:
    value = _evaluate(expression.this, row)
    return None if value is None else not value


_ARITHMETIC_OPERATIONS: dict[type[exp.Binary], Callable[[Any, Any], Any]] = {
    exp.Add: operator.add,
    exp.Sub: operator.sub,
    exp.Mul: operator.mul,
    exp.Div: operator.truediv,
    exp.Mod: operator.mod,
}


def _evaluate_arithmetic(expression: exp.Binary, row: Mapping[str, Any]) -> Any:
    left = _evaluate(expression.this, row)
    right = _evaluate(expression.expression, row)
    if left is None or right is None:
        return None
    return _ARITHMETIC_OPERATIONS[type(expression)](left, right)


for _expression_type in _ARITHMETIC_OPERATIONS:
    _evaluate.register(_expression_type)(_evaluate_arithmetic)


_COMPARISON_OPERATIONS: dict[type[exp.Binary], Callable[[Any, Any], bool]] = {
    exp.EQ: operator.eq,
    exp.NEQ: operator.ne,
    exp.GT: operator.gt,
    exp.GTE: operator.ge,
    exp.LT: operator.lt,
    exp.LTE: operator.le,
}


def _evaluate_comparison(expression: exp.Binary, row: Mapping[str, Any]) -> bool | None:
    left = _evaluate(expression.this, row)
    right = _evaluate(expression.expression, row)
    if left is None or right is None:
        return None
    return _COMPARISON_OPERATIONS[type(expression)](left, right)


for _expression_type in _COMPARISON_OPERATIONS:
    _evaluate.register(_expression_type)(_evaluate_comparison)


@_evaluate.register
def _evaluate_and(expression: exp.And, row: Mapping[str, Any]) -> bool | None:
    left = _evaluate(expression.this, row)
    right = _evaluate(expression.expression, row)
    if left is False or right is False:
        return False
    if left is None or right is None:
        return None
    return bool(left and right)


@_evaluate.register
def _evaluate_or(expression: exp.Or, row: Mapping[str, Any]) -> bool | None:
    left = _evaluate(expression.this, row)
    right = _evaluate(expression.expression, row)
    if left is True or right is True:
        return True
    if left is None or right is None:
        return None
    return bool(left or right)


@_evaluate.register
def _evaluate_is(expression: exp.Is, row: Mapping[str, Any]) -> bool:
    left = _evaluate(expression.this, row)
    right = expression.expression
    if isinstance(right, exp.Null):
        return left is None
    return left is _evaluate(right, row)


@_evaluate.register
def _evaluate_between(expression: exp.Between, row: Mapping[str, Any]) -> bool | None:
    value = _evaluate(expression.this, row)
    low = _evaluate(expression.args["low"], row)
    high = _evaluate(expression.args["high"], row)
    if value is None or low is None or high is None:
        return None
    return low <= value <= high


@_evaluate.register
def _evaluate_in(expression: exp.In, row: Mapping[str, Any]) -> bool | None:
    if expression.args.get("query") is not None:
        raise SQLQueryError("SQL IN subqueries are not supported")
    value = _evaluate(expression.this, row)
    if value is None:
        return None
    choices = [_evaluate(choice, row) for choice in expression.expressions]
    if value in choices:
        return True
    return None if None in choices else False


@_evaluate.register(exp.Like)
@_evaluate.register(exp.ILike)
def _evaluate_like(expression: exp.Expression, row: Mapping[str, Any]) -> bool | None:
    value = _evaluate(expression.this, row)
    pattern = _evaluate(expression.expression, row)
    if value is None or pattern is None:
        return None
    flags = re.IGNORECASE if isinstance(expression, exp.ILike) else 0
    regex = (
        "^" + "".join({"%": ".*", "_": "."}.get(character, re.escape(character)) for character in str(pattern)) + "$"
    )
    return re.match(regex, str(value), flags) is not None


@_evaluate.register
def _evaluate_coalesce(expression: exp.Coalesce, row: Mapping[str, Any]) -> Any:
    for candidate in (expression.this, *expression.expressions):
        value = _evaluate(candidate, row)
        if value is not None:
            return value
    return None


@_evaluate.register(exp.Lower)
@_evaluate.register(exp.Upper)
def _evaluate_text_case(expression: exp.Expression, row: Mapping[str, Any]) -> str | None:
    value = _evaluate(expression.this, row)
    if value is None:
        return None
    text = str(value)
    return text.lower() if isinstance(expression, exp.Lower) else text.upper()


@_evaluate.register
def _evaluate_cast(expression: exp.Cast, row: Mapping[str, Any]) -> Any:
    value = _evaluate(expression.this, row)
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
def _evaluate_case(expression: exp.Case, row: Mapping[str, Any]) -> Any:
    searched_case = expression.this is None
    base = None if searched_case else _evaluate(expression.this, row)
    for condition in expression.args.get("ifs") or ():
        matched = _evaluate(condition.this, row)
        if (searched_case and matched is True) or (not searched_case and base is not None and matched == base):
            return _evaluate(condition.args["true"], row)
    default = expression.args.get("default")
    return None if default is None else _evaluate(default, row)
