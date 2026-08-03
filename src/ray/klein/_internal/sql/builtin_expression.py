# SPDX-License-Identifier: Apache-2.0
"""Evaluation of Klein's portable SQL scalar built-ins."""

from __future__ import annotations

import ast
import base64
import hashlib
import json
from collections.abc import Callable, Sequence
from datetime import date, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal, localcontext
from functools import singledispatch
from typing import Any

import numpy as np
from sqlglot import exp

from ray.klein.api.sql_query_error import SQLQueryError

ExpressionEvaluator = Callable[[exp.Expression], Any]


class UnsupportedBuiltinExpressionError(Exception):
    """Signal that an expression is not one of Klein's SQL built-ins."""


def evaluate_builtin_expression(expression: exp.Expression, evaluate: ExpressionEvaluator) -> Any:
    """Evaluate a supported built-in, wrapping invalid calls consistently."""

    try:
        return _evaluate_builtin(expression, evaluate)
    except UnsupportedBuiltinExpressionError:
        raise
    except SQLQueryError:
        raise
    except Exception as error:
        name = _builtin_name(expression)
        raise SQLQueryError(f"SQL built-in function {name!r} failed: {error}") from error


@singledispatch
def _evaluate_builtin(expression: exp.Expression, _evaluate: ExpressionEvaluator) -> Any:
    raise UnsupportedBuiltinExpressionError(type(expression).__name__)


def _builtin_name(expression: exp.Expression) -> str:
    if isinstance(expression, exp.Anonymous):
        return str(expression.name).upper()
    names = {
        exp.TsOrDsToDate: "TO_DATE",
        exp.TsOrDsAdd: "DATE_ADD",
        exp.JSONExtractScalar: "GET_JSON_OBJECT",
        exp.JSONFormat: "TO_JSON",
        exp.ToBase64: "BASE64",
        exp.FromBase64: "UNBASE64",
        exp.VarMap: "MAP",
    }
    if isinstance(expression, exp.DateSub):
        return "DATE_SUB"
    for expression_type, name in names.items():
        if isinstance(expression, expression_type):
            return name
    return str(expression.sql_name())


def _strict_values(expression: exp.Expression, evaluate: ExpressionEvaluator) -> list[Any] | None:
    values = [evaluate(argument) for argument in expression.expressions]
    return None if any(value is None for value in values) else values


def _text(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)


def _integer(value: Any, label: str) -> int:
    if isinstance(value, bool):
        raise TypeError(f"{label} must be an integer")
    try:
        integer = int(value)
    except (TypeError, ValueError, OverflowError) as error:
        raise TypeError(f"{label} must be an integer") from error
    if isinstance(value, (float, Decimal)) and value != integer:
        raise TypeError(f"{label} must be an integer")
    return integer


@_evaluate_builtin.register
def _evaluate_length(expression: exp.Length, evaluate: ExpressionEvaluator) -> int | None:
    value = evaluate(expression.this)
    return None if value is None else len(value)


@_evaluate_builtin.register
def _evaluate_trim(expression: exp.Trim, evaluate: ExpressionEvaluator) -> str | None:
    value = evaluate(expression.this)
    if value is None:
        return None
    trim_expression = expression.args.get("expression")
    trim_characters = None if trim_expression is None else evaluate(trim_expression)
    if trim_expression is not None and trim_characters is None:
        return None
    text = _text(value)
    characters = None if trim_characters is None else _text(trim_characters)
    position = expression.args.get("position")
    if position == "LEADING":
        return text.lstrip(characters)
    if position == "TRAILING":
        return text.rstrip(characters)
    return text.strip(characters)


@_evaluate_builtin.register
def _evaluate_concat(expression: exp.Concat, evaluate: ExpressionEvaluator) -> Any:
    values = _strict_values(expression, evaluate)
    if values is None:
        return None
    if all(isinstance(value, (list, tuple)) for value in values):
        return [item for value in values for item in value]
    if any(isinstance(value, (list, tuple)) for value in values):
        raise TypeError("CONCAT arguments must be all strings or all arrays")
    return "".join(_text(value) for value in values)


@_evaluate_builtin.register
def _evaluate_concat_ws(expression: exp.ConcatWs, evaluate: ExpressionEvaluator) -> str | None:
    arguments = expression.expressions
    if not arguments:
        raise TypeError("CONCAT_WS requires a separator")
    separator = evaluate(arguments[0])
    if separator is None:
        return None
    values: list[str] = []
    for argument in arguments[1:]:
        value = evaluate(argument)
        if value is None:
            continue
        if isinstance(value, (list, tuple)):
            values.extend(_text(item) for item in value if item is not None)
        else:
            values.append(_text(value))
    return _text(separator).join(values)


@_evaluate_builtin.register
def _evaluate_substring(expression: exp.Substring, evaluate: ExpressionEvaluator) -> Any:
    value = evaluate(expression.this)
    start = evaluate(expression.args["start"])
    if value is None or start is None:
        return None
    if not isinstance(value, (str, bytes)):
        value = _text(value)
    start_index = _integer(start, "SUBSTRING start")
    offset = start_index - 1 if start_index > 0 else len(value) + start_index if start_index < 0 else 0
    offset = max(offset, 0)
    length_expression = expression.args.get("length")
    if length_expression is None:
        return value[offset:]
    length = evaluate(length_expression)
    if length is None:
        return None
    size = _integer(length, "SUBSTRING length")
    return value[offset : offset + max(size, 0)]


@_evaluate_builtin.register
def _evaluate_replace(expression: exp.Replace, evaluate: ExpressionEvaluator) -> str | None:
    value = evaluate(expression.this)
    search = evaluate(expression.expression)
    replacement_expression = expression.args.get("replacement")
    replacement = "" if replacement_expression is None else evaluate(replacement_expression)
    if value is None or search is None or replacement is None:
        return None
    return _text(value).replace(_text(search), _text(replacement))


def _evaluate_string_predicate(expression: exp.Binary, evaluate: ExpressionEvaluator, operation: str) -> bool | None:
    value = evaluate(expression.this)
    fragment = evaluate(expression.expression)
    if value is None or fragment is None:
        return None
    text = _text(value)
    other = _text(fragment)
    if operation == "contains":
        return other in text
    if operation == "startswith":
        return text.startswith(other)
    return text.endswith(other)


@_evaluate_builtin.register
def _evaluate_contains(expression: exp.Contains, evaluate: ExpressionEvaluator) -> bool | None:
    return _evaluate_string_predicate(expression, evaluate, "contains")


@_evaluate_builtin.register
def _evaluate_starts_with(expression: exp.StartsWith, evaluate: ExpressionEvaluator) -> bool | None:
    return _evaluate_string_predicate(expression, evaluate, "startswith")


@_evaluate_builtin.register
def _evaluate_ends_with(expression: exp.EndsWith, evaluate: ExpressionEvaluator) -> bool | None:
    return _evaluate_string_predicate(expression, evaluate, "endswith")


@_evaluate_builtin.register
def _evaluate_if(expression: exp.If, evaluate: ExpressionEvaluator) -> Any:
    condition = evaluate(expression.this)
    branch = expression.args.get("true") if condition is True else expression.args.get("false")
    return None if branch is None else evaluate(branch)


@_evaluate_builtin.register
def _evaluate_nullif(expression: exp.Nullif, evaluate: ExpressionEvaluator) -> Any:
    left = evaluate(expression.this)
    if left is None:
        return None
    right = evaluate(expression.expression)
    return None if right is not None and left == right else left


def _evaluate_extreme(expression: exp.Expression, evaluate: ExpressionEvaluator, *, greatest: bool) -> Any:
    arguments = (expression.this, *expression.expressions)
    values = [evaluate(argument) for argument in arguments]
    non_null = [value for value in values if value is not None]
    if not non_null:
        return None
    return max(non_null) if greatest else min(non_null)


@_evaluate_builtin.register
def _evaluate_greatest(expression: exp.Greatest, evaluate: ExpressionEvaluator) -> Any:
    return _evaluate_extreme(expression, evaluate, greatest=True)


@_evaluate_builtin.register
def _evaluate_least(expression: exp.Least, evaluate: ExpressionEvaluator) -> Any:
    return _evaluate_extreme(expression, evaluate, greatest=False)


@_evaluate_builtin.register
def _evaluate_sqrt(expression: exp.Sqrt, evaluate: ExpressionEvaluator) -> float | None:
    value = evaluate(expression.this)
    if value is None:
        return None
    return float(Decimal(str(value)).sqrt())


@_evaluate_builtin.register
def _evaluate_round(expression: exp.Round, evaluate: ExpressionEvaluator) -> Any:
    value = evaluate(expression.this)
    if value is None:
        return None
    decimals_expression = expression.args.get("decimals")
    decimals = 0 if decimals_expression is None else evaluate(decimals_expression)
    if decimals is None:
        return None
    precision = _integer(decimals, "ROUND precision")
    decimal_value = Decimal(str(value))
    digits = len(decimal_value.as_tuple().digits)
    with localcontext() as context:
        context.prec = max(context.prec, digits + abs(precision) + 2)
        rounded = decimal_value.quantize(Decimal(1).scaleb(-precision), rounding=ROUND_HALF_UP)
    if isinstance(value, Decimal):
        return rounded
    if isinstance(value, int):
        return int(rounded)
    return float(rounded)


def _to_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, bytes):
        value = value.decode("utf-8")
    if not isinstance(value, str):
        raise TypeError("date value must be an ISO string, date, or datetime")
    text = value.strip()
    try:
        return date.fromisoformat(text)
    except ValueError:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).date()


@_evaluate_builtin.register
def _evaluate_to_date(expression: exp.TsOrDsToDate, evaluate: ExpressionEvaluator) -> date | None:
    if expression.args.get("format") is not None:
        raise ValueError("TO_DATE supports ISO values only; custom formats are not supported")
    value = evaluate(expression.this)
    return None if value is None else _to_date(value)


def _evaluate_date_part(expression: exp.Expression, evaluate: ExpressionEvaluator, part: str) -> int | None:
    value = evaluate(expression.this)
    if value is None:
        return None
    parsed = _to_date(value)
    return {"year": parsed.year, "month": parsed.month, "day": parsed.day}[part]


@_evaluate_builtin.register
def _evaluate_year(expression: exp.Year, evaluate: ExpressionEvaluator) -> int | None:
    return _evaluate_date_part(expression, evaluate, "year")


@_evaluate_builtin.register
def _evaluate_month(expression: exp.Month, evaluate: ExpressionEvaluator) -> int | None:
    return _evaluate_date_part(expression, evaluate, "month")


@_evaluate_builtin.register
def _evaluate_day(expression: exp.Day, evaluate: ExpressionEvaluator) -> int | None:
    return _evaluate_date_part(expression, evaluate, "day")


def _date_unit(expression: exp.Expression) -> str:
    unit = expression.args.get("unit")
    if unit is None:
        return "DAY"
    return str(unit.this).upper()


def _date_delta(expression: exp.Expression, evaluate: ExpressionEvaluator) -> int | None:
    delta_expression = expression.expression
    if isinstance(delta_expression, exp.Interval):
        if _date_unit(delta_expression) != "DAY":
            raise ValueError("only DAY date intervals are supported")
        value = evaluate(delta_expression.this)
    else:
        value = evaluate(delta_expression)
    return None if value is None else _integer(value, "date offset")


def _evaluate_date_add_sub(expression: exp.Expression, evaluate: ExpressionEvaluator, sign: int) -> date | None:
    if _date_unit(expression) != "DAY":
        raise ValueError("only DAY date arithmetic is supported")
    value = evaluate(expression.this)
    delta = _date_delta(expression, evaluate)
    if value is None or delta is None:
        return None
    return _to_date(value) + timedelta(days=sign * delta)


@_evaluate_builtin.register
def _evaluate_date_add(expression: exp.DateAdd, evaluate: ExpressionEvaluator) -> date | None:
    return _evaluate_date_add_sub(expression, evaluate, 1)


@_evaluate_builtin.register
def _evaluate_date_sub(expression: exp.DateSub, evaluate: ExpressionEvaluator) -> date | None:
    return _evaluate_date_add_sub(expression, evaluate, -1)


@_evaluate_builtin.register
def _evaluate_ts_or_ds_add(expression: exp.TsOrDsAdd, evaluate: ExpressionEvaluator) -> date | None:
    return _evaluate_date_add_sub(expression, evaluate, 1)


@_evaluate_builtin.register
def _evaluate_date_diff(expression: exp.DateDiff, evaluate: ExpressionEvaluator) -> int | None:
    if _date_unit(expression) != "DAY":
        raise ValueError("DATEDIFF supports DAY differences only")
    end = evaluate(expression.this)
    start = evaluate(expression.expression)
    if end is None or start is None:
        return None
    return (_to_date(end) - _to_date(start)).days


def _parse_json(value: Any) -> Any:
    if isinstance(value, (dict, list, int, float, bool)):
        return value
    if isinstance(value, bytes):
        value = value.decode("utf-8")
    if not isinstance(value, str):
        raise TypeError("JSON input must be a string or JSON-compatible value")
    return json.loads(value, parse_constant=lambda constant: (_ for _ in ()).throw(ValueError(constant)))


def _json_bracket_part(path: str, index: int) -> tuple[Any, int]:
    end = path.find("]", index + 1)
    if end < 0:
        raise ValueError(f"invalid JSON path {path!r}")
    token = path[index + 1 : end].strip()
    if token == "*":
        part = _JSON_WILDCARD
    elif token.startswith(("'", '"')):
        part = ast.literal_eval(token)
        if not isinstance(part, str):
            raise ValueError(f"invalid JSON path {path!r}")
    else:
        part = _integer(token, "JSON array index")
    return part, end + 1


def _json_path(path: Any) -> tuple[Any, ...]:
    if not isinstance(path, str) or not path.startswith("$"):
        raise ValueError("JSON path must start with '$'")
    parts: list[Any] = []
    index = 1
    while index < len(path):
        if path[index] == ".":
            index += 1
            end = index
            while end < len(path) and path[end] not in ".[":
                end += 1
            if end == index:
                raise ValueError(f"invalid JSON path {path!r}")
            key = path[index:end]
            parts.append(_JSON_WILDCARD if key == "*" else key)
            index = end
            continue
        if path[index] == "[":
            part, index = _json_bracket_part(path, index)
            parts.append(part)
            continue
        raise ValueError(f"invalid JSON path {path!r}")
    return tuple(parts)


_JSON_WILDCARD = object()
_JSON_MISSING = object()


def _sqlglot_json_path(path: exp.JSONPath) -> tuple[Any, ...]:
    parts = []
    for component in path.expressions:
        if isinstance(component, exp.JSONPathRoot):
            continue
        value = component.this
        if isinstance(value, exp.JSONPathWildcard):
            parts.append(_JSON_WILDCARD)
        elif isinstance(component, exp.JSONPathKey):
            parts.append(str(value))
        elif isinstance(component, exp.JSONPathSubscript):
            parts.append(_integer(value, "JSON array index"))
        else:
            raise ValueError(f"unsupported JSON path component {component.sql()!r}")
    return tuple(parts)


def _json_child(value: Any, part: Any) -> Any:
    if isinstance(part, int) and isinstance(value, list) and -len(value) <= part < len(value):
        return value[part]
    if isinstance(part, str) and isinstance(value, dict) and part in value:
        return value[part]
    return _JSON_MISSING


def _extract_json(document: Any, path: Any) -> Any:
    values: list[Any] = [document]
    used_wildcard = False
    parts = _sqlglot_json_path(path) if isinstance(path, exp.JSONPath) else _json_path(path)
    for part in parts:
        next_values: list[Any] = []
        for value in values:
            if part is _JSON_WILDCARD:
                used_wildcard = True
                if isinstance(value, dict):
                    next_values.extend(value.values())
                elif isinstance(value, list):
                    next_values.extend(value)
            else:
                child = _json_child(value, part)
                if child is not _JSON_MISSING:
                    next_values.append(child)
        values = next_values
        if not values:
            return None
    return values if used_wildcard else values[0]


def _json_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


def _serialize_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


@_evaluate_builtin.register
def _evaluate_parse_json(expression: exp.ParseJSON, evaluate: ExpressionEvaluator) -> Any:
    value = evaluate(expression.this)
    return None if value is None else _parse_json(value)


@_evaluate_builtin.register
def _evaluate_json_extract_scalar(expression: exp.JSONExtractScalar, evaluate: ExpressionEvaluator) -> str | None:
    document = evaluate(expression.this)
    path_expression = expression.expression
    path = path_expression if isinstance(path_expression, exp.JSONPath) else evaluate(path_expression)
    if document is None or path is None:
        return None
    return _json_text(_extract_json(_parse_json(document), path))


@_evaluate_builtin.register
def _evaluate_json_format(expression: exp.JSONFormat, evaluate: ExpressionEvaluator) -> str | None:
    if expression.args.get("options") is not None:
        raise ValueError("TO_JSON options are not supported")
    value = evaluate(expression.this)
    return None if value is None else _serialize_json(value)


@_evaluate_builtin.register
def _evaluate_array(expression: exp.Array, evaluate: ExpressionEvaluator) -> list[Any]:
    return [evaluate(argument) for argument in expression.expressions]


def _as_map_items(expression: exp.Expression, evaluate: ExpressionEvaluator) -> tuple[Sequence[Any], Sequence[Any]]:
    keys_expression = expression.args.get("keys")
    values_expression = expression.args.get("values")
    if keys_expression is None and values_expression is None:
        return (), ()
    if keys_expression is None or values_expression is None:
        raise TypeError("MAP requires keys and values")
    keys = evaluate(keys_expression)
    values = evaluate(values_expression)
    if not isinstance(keys, (list, tuple)):
        keys = [keys]
    if not isinstance(values, (list, tuple)):
        values = [values]
    return keys, values


def _evaluate_map_value(expression: exp.Expression, evaluate: ExpressionEvaluator) -> dict[Any, Any]:
    keys, values = _as_map_items(expression, evaluate)
    if len(keys) != len(values):
        raise ValueError("MAP requires the same number of keys and values")
    result = {}
    for key, value in zip(keys, values, strict=True):
        if key is None:
            raise ValueError("MAP keys cannot be NULL")
        if key in result:
            raise ValueError(f"MAP key {key!r} is duplicated")
        result[key] = value
    return result


@_evaluate_builtin.register(exp.Map)
@_evaluate_builtin.register(exp.VarMap)
def _evaluate_map(expression: exp.Expression, evaluate: ExpressionEvaluator) -> dict[Any, Any]:
    return _evaluate_map_value(expression, evaluate)


@_evaluate_builtin.register
def _evaluate_array_size(expression: exp.ArraySize, evaluate: ExpressionEvaluator) -> int | None:
    value = evaluate(expression.this)
    if value is None:
        return None
    if not isinstance(value, (list, tuple, np.ndarray)):
        raise TypeError("ARRAY_SIZE requires an array")
    return len(value)


def _binary(value: Any) -> bytes:
    if isinstance(value, bytes):
        return value
    if isinstance(value, (bytearray, memoryview)):
        return bytes(value)
    return _text(value).encode("utf-8")


@_evaluate_builtin.register
def _evaluate_md5(expression: exp.MD5, evaluate: ExpressionEvaluator) -> str | None:
    value = evaluate(expression.this)
    return None if value is None else hashlib.md5(_binary(value), usedforsecurity=False).hexdigest()


@_evaluate_builtin.register
def _evaluate_sha2(expression: exp.SHA2, evaluate: ExpressionEvaluator) -> str | None:
    value = evaluate(expression.this)
    length = evaluate(expression.args["length"])
    if value is None or length is None:
        return None
    bits = _integer(length, "SHA2 bit length")
    if bits == 0:
        bits = 256
    if bits not in {224, 256, 384, 512}:
        raise ValueError("SHA2 bit length must be 0, 224, 256, 384, or 512")
    return hashlib.new(f"sha{bits}", _binary(value)).hexdigest()


@_evaluate_builtin.register
def _evaluate_to_base64(expression: exp.ToBase64, evaluate: ExpressionEvaluator) -> str | None:
    value = evaluate(expression.this)
    return None if value is None else base64.b64encode(_binary(value)).decode("ascii")


@_evaluate_builtin.register
def _evaluate_from_base64(expression: exp.FromBase64, evaluate: ExpressionEvaluator) -> bytes | None:
    value = evaluate(expression.this)
    return None if value is None else base64.b64decode(_binary(value), validate=True)


def _require_arguments(expression: exp.Anonymous, count: int) -> tuple[exp.Expression, ...]:
    arguments = tuple(expression.expressions)
    if len(arguments) != count:
        raise TypeError(f"{expression.name.upper()} requires {count} argument(s), got {len(arguments)}")
    return arguments


@_evaluate_builtin.register
def _evaluate_anonymous(expression: exp.Anonymous, evaluate: ExpressionEvaluator) -> Any:
    name = expression.name.casefold()
    if name == "to_date":
        (value_expression,) = _require_arguments(expression, 1)
        value = evaluate(value_expression)
        return None if value is None else _to_date(value)
    if name == "get_json_object":
        document_expression, path_expression = _require_arguments(expression, 2)
        document = evaluate(document_expression)
        path = evaluate(path_expression)
        if document is None or path is None:
            return None
        return _json_text(_extract_json(_parse_json(document), path))
    if name == "to_json":
        (value_expression,) = _require_arguments(expression, 1)
        value = evaluate(value_expression)
        return None if value is None else _serialize_json(value)
    if name == "base64":
        (value_expression,) = _require_arguments(expression, 1)
        value = evaluate(value_expression)
        return None if value is None else base64.b64encode(_binary(value)).decode("ascii")
    if name == "unbase64":
        (value_expression,) = _require_arguments(expression, 1)
        value = evaluate(value_expression)
        return None if value is None else base64.b64decode(_binary(value), validate=True)
    raise UnsupportedBuiltinExpressionError(expression.name)
