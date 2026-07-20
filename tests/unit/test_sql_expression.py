# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

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


def test_evaluate_expression_rejects_unsupported_forms() -> None:
    with pytest.raises(SQLQueryError, match="Unsupported SQL expression"):
        evaluate_expression(parse_one("ABS(-1)"), {})

    with pytest.raises(SQLQueryError, match="IN subqueries"):
        evaluate_expression(parse_one("1 IN (SELECT 1)"), {})

    with pytest.raises(SQLQueryError, match="Cannot CAST"):
        evaluate_expression(parse_one("CAST('not-a-boolean' AS BOOLEAN)"), {})


def test_sql_expression_lowers_to_ray_data_expression_ast() -> None:
    expression = to_ray_data_expression(parse_one("amount * 2 + 1"), ("orders",))

    assert expression is not None
    assert expression.structurally_equals(col("orders.amount") * 2 + 1)


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
