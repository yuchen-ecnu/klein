# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import pytest
from sqlglot import parse_one

from ray.klein._internal.sql.expression import evaluate_expression
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
