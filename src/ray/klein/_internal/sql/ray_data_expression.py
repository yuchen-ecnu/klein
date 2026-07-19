# SPDX-License-Identifier: Apache-2.0
"""Translate SQLGlot scalar expressions to Ray Data 2.56 expressions."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from functools import singledispatch
from typing import Any

from ray.data.datatype import DataType
from ray.data.expressions import Expr, col, download, lit, monotonically_increasing_id, random, uuid
from sqlglot import exp

from ray.klein.api.sql_query_error import SQLQueryError


def to_ray_data_expression(
    expression: exp.Expression,
    aliases: Sequence[str],
    *,
    predicate: bool = False,
) -> Expr | None:
    """Return the equivalent Ray expression, or ``None`` for row fallback.

    Ray Data's ``DownloadExpr`` is deliberately accepted only as a complete
    value expression. Ray 2.56 plans it as a dedicated download operator; it
    cannot be evaluated as a child of a normal projection or predicate.
    """

    value = expression.this if isinstance(expression, exp.Alias) else expression
    download_calls = [node for node in value.walk() if _is_download_call(node)]
    if download_calls and not _is_download_call(value):
        raise SQLQueryError("DOWNLOAD(column) must be a standalone SELECT or aggregate input expression")

    translated = _translate(value, tuple(aliases), predicate=predicate)
    if predicate and translated is not None and _is_download_call(value):
        raise SQLQueryError("DOWNLOAD(column) cannot be used as a SQL predicate")
    return translated


def is_ray_data_only_expression(expression: exp.Expression) -> bool:
    """Whether ``expression`` needs a Ray Data execution operator."""

    return any(_is_ray_data_only_node(node) for node in expression.walk())


@singledispatch
def _translate(_expression: exp.Expression, _aliases: tuple[str, ...], *, predicate: bool) -> Expr | None:
    del predicate
    return None


@_translate.register(exp.Alias)
@_translate.register(exp.Paren)
def _translate_wrapper(expression: exp.Expression, aliases: tuple[str, ...], *, predicate: bool) -> Expr | None:
    return _translate(expression.this, aliases, predicate=predicate)


@_translate.register
def _translate_column(expression: exp.Column, aliases: tuple[str, ...], *, predicate: bool) -> Expr | None:
    del predicate
    name = _column_name(expression, aliases)
    return None if name is None else col(name)


@_translate.register
def _translate_null(_expression: exp.Null, _aliases: tuple[str, ...], *, predicate: bool) -> Expr:
    del predicate
    return lit(None)


@_translate.register
def _translate_boolean(expression: exp.Boolean, _aliases: tuple[str, ...], *, predicate: bool) -> Expr:
    del predicate
    return lit(expression.this)


@_translate.register
def _translate_literal(expression: exp.Literal, _aliases: tuple[str, ...], *, predicate: bool) -> Expr:
    del predicate
    return lit(_literal_value(expression))


@_translate.register
def _translate_negation(expression: exp.Neg, aliases: tuple[str, ...], *, predicate: bool) -> Expr | None:
    operand = _translate(expression.this, aliases, predicate=predicate)
    return None if operand is None else operand.negate()


@_translate.register
def _translate_not(expression: exp.Not, aliases: tuple[str, ...], *, predicate: bool) -> Expr | None:
    if isinstance(expression.this, exp.In):
        return _translate_in(expression.this, aliases, predicate=predicate, negate=True)
    operand = _translate(expression.this, aliases, predicate=predicate)
    return None if operand is None else ~operand


@_translate.register
def _translate_and(expression: exp.And, aliases: tuple[str, ...], *, predicate: bool) -> Expr | None:
    return _translate_pair(expression, aliases, predicate, lambda left, right: left & right)


@_translate.register
def _translate_or(expression: exp.Or, aliases: tuple[str, ...], *, predicate: bool) -> Expr | None:
    return _translate_pair(expression, aliases, predicate, lambda left, right: left | right)


@_translate.register
def _translate_is(expression: exp.Is, aliases: tuple[str, ...], *, predicate: bool) -> Expr | None:
    operand = _translate(expression.this, aliases, predicate=predicate)
    if operand is None or not isinstance(expression.expression, exp.Null):
        return None
    return operand.is_null()


@_translate.register
def _translate_membership(expression: exp.In, aliases: tuple[str, ...], *, predicate: bool) -> Expr | None:
    return _translate_in(expression, aliases, predicate=predicate, negate=False)


@_translate.register
def _translate_cast(expression: exp.Cast, aliases: tuple[str, ...], *, predicate: bool) -> Expr | None:
    operand = _translate(expression.this, aliases, predicate=predicate)
    target = _cast_type(expression)
    return None if operand is None or target is None else operand.cast(target)


@_translate.register(exp.Lower)
@_translate.register(exp.Upper)
def _translate_text_case(expression: exp.Expression, aliases: tuple[str, ...], *, predicate: bool) -> Expr | None:
    operand = _translate(expression.this, aliases, predicate=predicate)
    if operand is None:
        return None
    return operand.str.lower() if isinstance(expression, exp.Lower) else operand.str.upper()


@_translate.register
def _translate_power(expression: exp.Pow, aliases: tuple[str, ...], *, predicate: bool) -> Expr | None:
    base = _translate(expression.this, aliases, predicate=predicate)
    exponent = _translate(expression.expression, aliases, predicate=predicate)
    return None if base is None or exponent is None else base.power(exponent)


@_translate.register
def _translate_rand(expression: exp.Rand, _aliases: tuple[str, ...], *, predicate: bool) -> Expr | None:
    del predicate
    return _translate_random(expression)


@_translate.register
def _translate_uuid(_expression: exp.Uuid, _aliases: tuple[str, ...], *, predicate: bool) -> Expr:
    del predicate
    return uuid()


@_translate.register
def _translate_logarithm(expression: exp.Log, aliases: tuple[str, ...], *, predicate: bool) -> Expr | None:
    return _translate_log(expression, aliases, predicate)


@_translate.register
def _translate_function(expression: exp.Anonymous, aliases: tuple[str, ...], *, predicate: bool) -> Expr | None:
    del predicate
    return _translate_anonymous(expression, aliases)


def _translate_pair(
    expression: exp.Binary,
    aliases: tuple[str, ...],
    predicate: bool,
    operation: Callable[[Expr, Expr], Expr],
) -> Expr | None:
    left = _translate(expression.this, aliases, predicate=predicate)
    right = _translate(expression.expression, aliases, predicate=predicate)
    return None if left is None or right is None else operation(left, right)


def _translate_in(
    expression: exp.In,
    aliases: tuple[str, ...],
    *,
    predicate: bool,
    negate: bool,
) -> Expr | None:
    # Arrow's is_in returns False for a null input, whereas SQL projection
    # semantics require UNKNOWN. In a WHERE predicate both are discarded, so
    # native lowering is safe only there.
    if not predicate or expression.args.get("query") is not None:
        return None
    operand = _translate(expression.this, aliases, predicate=True)
    choices = expression.expressions
    if operand is None or not all(isinstance(choice, (exp.Literal, exp.Boolean)) for choice in choices):
        return None
    values = [_literal_value(choice) if isinstance(choice, exp.Literal) else choice.this for choice in choices]
    return operand.not_in(values) if negate else operand.is_in(values)


def _translate_random(expression: exp.Rand) -> Expr | None:
    seed_expression = expression.this
    if seed_expression is None:
        return random()
    if not isinstance(seed_expression, exp.Literal) or seed_expression.is_string:
        return None
    return random(seed=int(seed_expression.this))


def _translate_log(expression: exp.Log, aliases: tuple[str, ...], predicate: bool) -> Expr | None:
    operand_expression = expression.args.get("expression")
    if operand_expression is None:
        operand = _translate(expression.this, aliases, predicate=predicate)
        return None if operand is None else operand.ln()
    operand = _translate(operand_expression, aliases, predicate=predicate)
    base = expression.this
    if operand is None or not isinstance(base, exp.Literal) or base.is_string:
        return None
    if base.this == "2":
        return operand.log2()
    if base.this == "10":
        return operand.log10()
    return None


def _translate_anonymous(expression: exp.Anonymous, aliases: tuple[str, ...]) -> Expr | None:
    name = expression.name.upper()
    if name == "DOWNLOAD":
        if len(expression.expressions) != 1 or not isinstance(expression.expressions[0], exp.Column):
            raise SQLQueryError("DOWNLOAD requires exactly one URI column argument")
        column_name = _column_name(expression.expressions[0], aliases)
        if column_name is None:
            raise SQLQueryError("DOWNLOAD columns must be table-qualified when a query has multiple relations")
        return download(column_name)
    if name == "MONOTONICALLY_INCREASING_ID":
        if expression.expressions:
            raise SQLQueryError(f"{name} does not accept arguments")
        return monotonically_increasing_id()
    return None


def _column_name(column: exp.Column, aliases: tuple[str, ...]) -> str | None:
    if column.table:
        return f"{column.table}.{column.name}"
    if len(aliases) == 1:
        return f"{aliases[0]}.{column.name}"
    if not aliases:
        return column.name
    return None


def _literal_value(literal: exp.Literal) -> Any:
    if literal.is_string:
        return literal.this
    try:
        return int(literal.this)
    except ValueError:
        return float(literal.this)


def _cast_type(expression: exp.Cast) -> DataType | None:
    target = expression.to.sql().upper()
    conversions = (
        (("TINYINT",), DataType.int8),
        (("SMALLINT",), DataType.int16),
        (("INT", "INTEGER", "BIGINT"), DataType.int64),
        (("FLOAT",), DataType.float32),
        (("DOUBLE", "DECIMAL"), DataType.float64),
        (("STRING", "TEXT", "VARCHAR", "CHAR"), DataType.string),
        (("BOOL", "BOOLEAN"), DataType.bool),
        (("BINARY", "VARBINARY"), DataType.binary),
    )
    for prefixes, factory in conversions:
        if target.startswith(prefixes):
            return factory()
    return None


def _is_download_call(expression: exp.Expression) -> bool:
    return isinstance(expression, exp.Anonymous) and expression.name.upper() == "DOWNLOAD"


def _is_ray_data_only_node(expression: exp.Expression) -> bool:
    return (
        _is_download_call(expression)
        or isinstance(expression, (exp.Rand, exp.Uuid))
        or (isinstance(expression, exp.Anonymous) and expression.name.upper() == "MONOTONICALLY_INCREASING_ID")
    )


_BINARY_OPERATIONS = {
    exp.Add: lambda left, right: left + right,
    exp.Sub: lambda left, right: left - right,
    exp.Mul: lambda left, right: left * right,
    exp.Div: lambda left, right: left / right,
    exp.Mod: lambda left, right: left % right,
    exp.EQ: lambda left, right: left == right,
    exp.NEQ: lambda left, right: left != right,
    exp.GT: lambda left, right: left > right,
    exp.GTE: lambda left, right: left >= right,
    exp.LT: lambda left, right: left < right,
    exp.LTE: lambda left, right: left <= right,
}

_UNARY_METHODS = {
    exp.Abs: "abs",
    exp.Acos: "acos",
    exp.Asin: "asin",
    exp.Atan: "atan",
    exp.Ceil: "ceil",
    exp.Cos: "cos",
    exp.Exp: "exp",
    exp.Floor: "floor",
    exp.Ln: "ln",
    exp.Round: "round",
    exp.Sign: "sign",
    exp.Sin: "sin",
    exp.Tan: "tan",
    exp.Trunc: "trunc",
}


def _translate_binary(expression: exp.Binary, aliases: tuple[str, ...], *, predicate: bool) -> Expr | None:
    left = _translate(expression.this, aliases, predicate=predicate)
    right = _translate(expression.expression, aliases, predicate=predicate)
    if left is None or right is None:
        return None
    if isinstance(expression, exp.Div):
        # Arrow divides integer arrays with integer semantics. SQL `/` and
        # Klein's existing evaluator produce a floating-point quotient.
        left = left.cast(DataType.float64())
    return _BINARY_OPERATIONS[type(expression)](left, right)


def _translate_unary_method(
    expression: exp.Expression,
    aliases: tuple[str, ...],
    *,
    predicate: bool,
) -> Expr | None:
    operand = _translate(expression.this, aliases, predicate=predicate)
    if operand is None or expression.args.get("decimals") is not None:
        return None
    return getattr(operand, _UNARY_METHODS[type(expression)])()


for _expression_type in _BINARY_OPERATIONS:
    _translate.register(_expression_type)(_translate_binary)

for _expression_type in _UNARY_METHODS:
    _translate.register(_expression_type)(_translate_unary_method)
