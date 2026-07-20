# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import pytest

from ray.klein.api.klein_context import KleinContext
from ray.klein.config.configuration import Configuration
from ray.klein.config.execution_options import ExecutionOptions
from ray.klein.config.runtime_execution_mode import RuntimeExecutionMode
from tests.support.assertions import assert_rows_equal

SQL_SEMANTIC_CASES = (
    pytest.param(
        """
        SELECT
            id,
            score = 10 AS eq_ten,
            score <> 10 AS neq_ten,
            active AND score > 0 AS accepted,
            active OR score > 0 AS maybe_active,
            NOT active AS not_active,
            score IS NULL AS missing_score
        FROM events
        """,
        [
            {"id": 1, "score": 10, "active": True},
            {"id": 2, "score": None, "active": True},
            {"id": 3, "score": -1, "active": None},
        ],
        [
            {
                "id": 1,
                "eq_ten": True,
                "neq_ten": False,
                "accepted": True,
                "maybe_active": True,
                "not_active": False,
                "missing_score": False,
            },
            {
                "id": 2,
                "eq_ten": None,
                "neq_ten": None,
                "accepted": None,
                "maybe_active": True,
                "not_active": False,
                "missing_score": True,
            },
            {
                "id": 3,
                "eq_ten": False,
                "neq_ten": True,
                "accepted": False,
                "maybe_active": None,
                "not_active": None,
                "missing_score": False,
            },
        ],
        id="null-three-valued-logic",
    ),
    pytest.param(
        """
        SELECT
            id,
            CASE
                WHEN amount IS NULL THEN 'missing'
                WHEN amount >= 10 THEN 'large'
                ELSE 'small'
            END AS bucket,
            CAST(raw_amount AS BIGINT) + 2 AS adjusted,
            amount * 2 + 1 AS scaled
        FROM events
        """,
        [
            {"id": 1, "amount": 12, "raw_amount": "7"},
            {"id": 2, "amount": 4, "raw_amount": "3"},
            {"id": 3, "amount": None, "raw_amount": "0"},
        ],
        [
            {"id": 1, "bucket": "large", "adjusted": 9, "scaled": 25},
            {"id": 2, "bucket": "small", "adjusted": 5, "scaled": 9},
            {"id": 3, "bucket": "missing", "adjusted": 2, "scaled": None},
        ],
        id="case-cast-arithmetic",
    ),
    pytest.param(
        """
        SELECT id, LOWER(name) AS normalized_name, score + 1 AS next_score
        FROM events
        WHERE category IN ('a', 'b')
          AND name LIKE 'A%'
          AND (score >= 10 OR priority IS TRUE)
        """,
        [
            {"id": 1, "category": "a", "name": "Ada", "score": 10, "priority": False},
            {"id": 2, "category": "b", "name": "Alan", "score": 5, "priority": True},
            {"id": 3, "category": "c", "name": "Amy", "score": 99, "priority": True},
            {"id": 4, "category": "a", "name": "Bob", "score": 20, "priority": True},
            {"id": 5, "category": "a", "name": "Alice", "score": None, "priority": False},
        ],
        [
            {"id": 1, "normalized_name": "ada", "next_score": 11},
            {"id": 2, "normalized_name": "alan", "next_score": 6},
        ],
        id="in-like-compound-filter-projection",
    ),
)


def _take_all(stream, job_name: str) -> list[dict]:
    sink = stream.take_all()
    return sink.context.execute(job_name, sinks=(sink,)).get()


@pytest.mark.parametrize("mode", [RuntimeExecutionMode.BATCH, RuntimeExecutionMode.STREAMING])
def test_inner_join_has_the_same_null_semantics_in_batch_and_streaming(mode: RuntimeExecutionMode) -> None:
    config = Configuration()
    config.set(ExecutionOptions.MODE, mode)
    context = KleinContext(config)
    orders = context.from_items(
        [
            {"customer_id": 1, "amount": 10},
            {"customer_id": None, "amount": 99},
        ]
    )
    customers = context.from_items(
        [
            {"customer_id": 1, "name": "Ada"},
            {"customer_id": None, "name": "unknown"},
        ]
    )

    actual = _take_all(
        context.sql(
            """
            SELECT o.customer_id AS customer_id, o.amount, c.name
            FROM orders AS o
            JOIN customers AS c ON o.customer_id = c.customer_id
            """,
            tables={"orders": orders, "customers": customers},
        ),
        f"sql-null-join-{mode.value}",
    )

    assert [dict(row) for row in actual] == [{"customer_id": 1, "amount": 10, "name": "Ada"}]


@pytest.mark.parametrize(("query", "rows", "expected"), SQL_SEMANTIC_CASES)
@pytest.mark.parametrize("mode", [RuntimeExecutionMode.BATCH, RuntimeExecutionMode.STREAMING])
def test_scalar_sql_semantics_match_the_same_oracle_in_batch_and_streaming(
    mode: RuntimeExecutionMode,
    query: str,
    rows: list[dict[str, object]],
    expected: list[dict[str, object]],
) -> None:
    config = Configuration()
    config.set(ExecutionOptions.MODE, mode)
    context = KleinContext(config)
    events = context.from_items(rows)

    actual = _take_all(
        context.sql(query, tables={"events": events}),
        f"sql-scalar-semantics-{mode.value}",
    )

    assert_rows_equal([dict(row) for row in actual], expected, order_sensitive=False)
