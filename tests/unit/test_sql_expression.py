# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from datetime import date

import numpy as np
import pytest
from ray.data.expressions import DownloadExpr, RandomExpr, col
from sqlglot import parse_one

from ray.klein._internal.sql.expression import evaluate_expression
from ray.klein._internal.sql.ray_data_expression import to_ray_data_expression
from ray.klein.api.sql_query_error import SQLQueryError


@pytest.mark.parametrize(
    ("sql", "row", "expected"),
    [
        ("amount * 2 + 1", {"amount": 4}, 9),
        ("amount BETWEEN 2 AND 5", {"amount": 4}, True),
        ("name LIKE 'A_%'", {"name": "Ada"}, True),
        ("name ILIKE 'a%'", {"name": "ADA"}, True),
        ("COALESCE(missing, 7)", {"missing": None}, 7),
        ("CASE WHEN amount > 3 THEN 'high' ELSE 'low' END", {"amount": 4}, "high"),
        ("CASE amount WHEN 4 THEN 'four' ELSE 'other' END", {"amount": 4}, "four"),
        ("CAST('false' AS BOOLEAN)", {}, False),
        ("2 IN (1, NULL)", {}, None),
        ("1 IN (1, NULL)", {}, True),
        ("FALSE AND NULL", {}, False),
        ("TRUE OR NULL", {}, True),
    ],
)
def test_evaluate_expression_uses_sql_semantics(sql, row, expected) -> None:
    assert evaluate_expression(parse_one(sql), row) == expected


def test_evaluate_expression_resolves_qualified_columns() -> None:
    expression = parse_one("orders.id")

    assert evaluate_expression(expression, {"orders.id": 1, "customers.id": 2}) == 1

    with pytest.raises(SQLQueryError, match="Ambiguous"):
        evaluate_expression(parse_one("id"), {"orders.id": 1, "customers.id": 2})


def test_evaluate_expression_calls_klein_scalar_functions_case_insensitively() -> None:
    expression = parse_one("ADD_SUFFIX(name, '!')")

    assert (
        evaluate_expression(
            expression,
            {"name": "Ada"},
            {"add_suffix": lambda value, suffix: value.lower() + suffix},
        )
        == "ada!"
    )


def test_evaluate_expression_passes_nulls_to_scalar_functions() -> None:
    seen = []

    result = evaluate_expression(
        parse_one("FILL(value)"),
        {"value": None},
        {"fill": lambda value: seen.append(value) or "missing"},
    )

    assert result == "missing"
    assert seen == [None]


def test_evaluate_expression_adds_scalar_function_failure_context() -> None:
    def fail(_value):
        raise ValueError("invalid model output")

    with pytest.raises(SQLQueryError, match="SQL scalar function 'FAIL' failed: invalid model output"):
        evaluate_expression(parse_one("FAIL(value)"), {"value": 1}, {"fail": fail})


def test_evaluate_expression_rejects_dynamically_returned_awaitable() -> None:
    async def result():
        return 1

    with pytest.raises(SQLQueryError, match=r"returned an awaitable.*must be synchronous"):
        evaluate_expression(parse_one("ASYNC_RESULT(value)"), {"value": 1}, {"async_result": lambda _value: result()})


def test_evaluate_expression_rejects_dynamically_returned_async_iterator() -> None:
    async def results():
        yield 1

    with pytest.raises(SQLQueryError, match=r"returned an async iterator.*synchronous scalar values"):
        evaluate_expression(
            parse_one("ASYNC_RESULTS(value)"),
            {"value": 1},
            {"async_results": lambda _value: results()},
        )


@pytest.mark.parametrize(
    ("sql", "value", "expected"),
    [
        ("ABS(IDENTITY(value))", -2, 2),
        ("IDENTITY(ABS(value))", -2, 2),
        ("POWER(IDENTITY(value), 2)", -2, 4),
        ("LOG(10, IDENTITY(value))", 100, 2),
        ("COS(IDENTITY(value))", 0, 1),
        ("SIGN(IDENTITY(value))", -2, -1),
    ],
)
def test_evaluate_expression_composes_scalar_functions_with_numeric_builtins(sql, value, expected) -> None:
    result = evaluate_expression(parse_one(sql), {"value": value}, {"identity": lambda item: item})

    assert result == pytest.approx(expected)


def test_numeric_builtin_propagates_null_returned_by_scalar_function() -> None:
    assert (
        evaluate_expression(parse_one("ABS(IDENTITY(value))"), {"value": None}, {"identity": lambda item: item}) is None
    )


def test_evaluate_expression_rejects_unsupported_forms() -> None:
    with pytest.raises(SQLQueryError, match="Unsupported SQL expression"):
        evaluate_expression(parse_one("ASCII('a')"), {})

    with pytest.raises(SQLQueryError, match="IN subqueries"):
        evaluate_expression(parse_one("1 IN (SELECT 1)"), {})

    with pytest.raises(SQLQueryError, match="Cannot CAST"):
        evaluate_expression(parse_one("CAST('not-a-boolean' AS BOOLEAN)"), {})


def _evaluate_spark(sql: str, row=None):
    return evaluate_expression(parse_one(sql, read="spark"), row or {})


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        ("LENGTH('Klein')", 5),
        ("TRIM('  Klein  ')", "Klein"),
        ("CONCAT('AI', ' ', 'data')", "AI data"),
        ("CONCAT(ARRAY(1, 2), ARRAY(3))", [1, 2, 3]),
        ("CONCAT_WS('-', 'AI', NULL, 'data')", "AI-data"),
        ("SUBSTRING('Klein SQL', 7, 3)", "SQL"),
        ("SUBSTRING('Klein', -3)", "ein"),
        ("REPLACE('AI data', 'data', 'SQL')", "AI SQL"),
        ("CONTAINS('AI data', 'data')", True),
        ("STARTSWITH('AI data', 'AI')", True),
        ("ENDSWITH('AI data', 'data')", True),
    ],
)
def test_evaluate_common_string_builtins(sql, expected) -> None:
    assert _evaluate_spark(sql) == expected


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        ("IF(TRUE, 'yes', 'no')", "yes"),
        ("IF(NULL, 'yes', 'no')", "no"),
        ("NULLIF('same', 'same')", None),
        ("NULLIF('left', NULL)", "left"),
        ("GREATEST(NULL, 3, 1)", 3),
        ("LEAST(NULL, 3, 1)", 1),
        ("GREATEST(NULL, NULL)", None),
    ],
)
def test_evaluate_conditional_builtins(sql, expected) -> None:
    assert _evaluate_spark(sql) == expected


def test_if_evaluates_only_the_selected_branch() -> None:
    assert (
        evaluate_expression(
            parse_one("IF(TRUE, 'selected', FAIL())", read="spark"),
            {},
            {"fail": lambda: (_ for _ in ()).throw(AssertionError("not selected"))},
        )
        == "selected"
    )


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        ("SQRT(9)", 3.0),
        ("ROUND(2.5)", 3.0),
        ("ROUND(-2.5)", -3.0),
        ("ROUND(2.345, 2)", 2.35),
        ("ROUND(-2.345, 2)", -2.35),
        ("ROUND(15, -1)", 20),
    ],
)
def test_evaluate_math_builtins_uses_spark_rounding(sql, expected) -> None:
    assert _evaluate_spark(sql) == expected


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        ("TO_DATE('2024-02-29')", date(2024, 2, 29)),
        ("YEAR('2024-02-29')", 2024),
        ("MONTH('2024-02-29')", 2),
        ("DAY('2024-02-29')", 29),
        ("DATE_ADD('2024-02-28', 2)", date(2024, 3, 1)),
        ("DATE_SUB('2024-03-01', 2)", date(2024, 2, 28)),
        ("DATEDIFF('2024-03-01', '2024-02-28')", 2),
    ],
)
def test_evaluate_iso_date_builtins(sql, expected) -> None:
    assert _evaluate_spark(sql) == expected


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        ('GET_JSON_OBJECT(\'{"user": {"names": ["Ada"]}}\', \'$.user.names[0]\')', "Ada"),
        ("GET_JSON_OBJECT('{\"count\": 2}', '$.count')", "2"),
        ("GET_JSON_OBJECT('{\"count\": 2}', '$.missing')", None),
        ('PARSE_JSON(\'{"name":"Ada","active":true}\')', {"name": "Ada", "active": True}),
        ("TO_JSON(ARRAY(1, NULL, 'AI'))", '[1,null,"AI"]'),
        ("TO_JSON(PARSE_JSON('\"AI\"'))", '"AI"'),
    ],
)
def test_evaluate_json_builtins(sql, expected) -> None:
    assert _evaluate_spark(sql) == expected


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        ("ARRAY(1, NULL, 'AI')", [1, None, "AI"]),
        ("MAP('model', 'gpt', 'count', 2)", {"model": "gpt", "count": 2}),
        ("ARRAY_SIZE(ARRAY(1, NULL, 3))", 3),
    ],
)
def test_evaluate_collection_builtins(sql, expected) -> None:
    assert _evaluate_spark(sql) == expected


def test_array_size_accepts_ray_materialized_numpy_array() -> None:
    assert evaluate_expression(parse_one("ARRAY_SIZE(items)"), {"items": np.array([1, 2, 3])}) == 3


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        ("MD5('abc')", "900150983cd24fb0d6963f7d28e17f72"),
        ("SHA2('abc', 256)", "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"),
        ("BASE64('Klein')", "S2xlaW4="),
        ("UNBASE64('S2xlaW4=')", b"Klein"),
    ],
)
def test_evaluate_encoding_and_hash_builtins(sql, expected) -> None:
    assert _evaluate_spark(sql) == expected


@pytest.mark.parametrize(
    "sql",
    [
        "LENGTH(NULL)",
        "TRIM(NULL)",
        "CONCAT('a', NULL)",
        "SUBSTRING(NULL, 1)",
        "REPLACE(NULL, 'a', 'b')",
        "CONTAINS(NULL, 'a')",
        "SQRT(NULL)",
        "ROUND(NULL, 2)",
        "TO_DATE(NULL)",
        "DATE_ADD(NULL, 1)",
        "DATEDIFF(NULL, '2024-01-01')",
        "GET_JSON_OBJECT(NULL, '$.a')",
        "PARSE_JSON(NULL)",
        "TO_JSON(NULL)",
        "ARRAY_SIZE(NULL)",
        "MD5(NULL)",
        "SHA2(NULL, 256)",
        "BASE64(NULL)",
        "UNBASE64(NULL)",
    ],
)
def test_common_builtins_propagate_null(sql) -> None:
    assert _evaluate_spark(sql) is None


@pytest.mark.parametrize(
    "sql",
    [
        "SQRT(-1)",
        "ROUND(1.25, 1.5)",
        "TO_DATE('not-a-date')",
        "PARSE_JSON('{invalid')",
        "MAP(NULL, 1)",
        "SHA2('abc', 128)",
        "UNBASE64('***')",
    ],
)
def test_invalid_builtin_calls_raise_sql_query_error(sql) -> None:
    with pytest.raises(SQLQueryError, match="SQL built-in function"):
        _evaluate_spark(sql)


def test_get_json_object_rejects_an_invalid_dynamic_path() -> None:
    with pytest.raises(SQLQueryError, match="SQL built-in function"):
        _evaluate_spark("GET_JSON_OBJECT('{}', path)", {"path": "invalid"})


def test_sql_expression_lowers_to_ray_data_expression_ast() -> None:
    expression = to_ray_data_expression(parse_one("amount * 2 + 1"), ("orders",))

    assert expression is not None
    assert expression.structurally_equals(col("orders.amount") * 2 + 1)


def test_sql_internal_columns_are_not_table_qualified_and_round_uses_portable_semantics() -> None:
    internal = to_ray_data_expression(parse_one("_klein_ai_result_0"), ("rows",))

    assert internal is not None
    assert internal.structurally_equals(col("_klein_ai_result_0"))
    assert to_ray_data_expression(parse_one("ROUND(2.5)"), ()) is None


def test_sql_download_lowers_to_dedicated_ray_expression() -> None:
    expression = to_ray_data_expression(parse_one("DOWNLOAD(uri)"), ("files",))

    assert isinstance(expression, DownloadExpr)
    assert expression.uri_column_name == "files.uri"
    assert expression.filesystem is None


def test_sql_supports_ray_synthetic_expressions() -> None:
    expression = to_ray_data_expression(parse_one("RANDOM(42)"), ())

    assert isinstance(expression, RandomExpr)
    assert expression.seed == 42


def test_sql_expression_keeps_three_valued_in_projection_semantics() -> None:
    projected = to_ray_data_expression(parse_one("value IN (1, 2)"), ("rows",))
    predicate = to_ray_data_expression(parse_one("value IN (1, 2)"), ("rows",), predicate=True)

    assert projected is None
    assert predicate is not None


@pytest.mark.parametrize("sql", ["active AND score > 0", "active OR score > 0", "NOT active"])
def test_sql_boolean_logic_uses_row_fallback_for_all_null_arrow_blocks(sql: str) -> None:
    assert to_ray_data_expression(parse_one(sql), ("rows",)) is None
    assert to_ray_data_expression(parse_one(sql), ("rows",), predicate=True) is None


def test_sql_download_rejects_unsupported_composition_and_predicates() -> None:
    with pytest.raises(SQLQueryError, match="standalone"):
        to_ray_data_expression(parse_one("DOWNLOAD(uri) + 'suffix'"), ("files",))

    with pytest.raises(SQLQueryError, match="predicate"):
        to_ray_data_expression(parse_one("DOWNLOAD(uri)"), ("files",), predicate=True)
