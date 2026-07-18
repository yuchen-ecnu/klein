# SPDX-License-Identifier: Apache-2.0
from ray.klein.api.collect_function import CollectFunction
from ray.klein.api.klein_context import KleinContext
from ray.klein.api.node_type import NodeType
from ray.klein.api.row_kind import RowKind
from ray.klein.config.configuration import Configuration
from tests.support.streaming import LoopSourceFunction


def test_join_group_expression_uses_ray_native_operators() -> None:
    context = KleinContext()
    context.enable_interactive_mode()
    orders = context.data.from_items(
        [
            {"customer_id": 1, "amount": 10},
            {"customer_id": 1, "amount": 15},
            {"customer_id": 2, "amount": 7},
        ]
    )
    customers = context.data.from_items(
        [
            {"customer_id": 1, "name": "Ada"},
            {"customer_id": 2, "name": "Lin"},
        ]
    )

    totals = context.sql(
        """
        SELECT c.name, SUM(o.amount) AS total
        FROM orders AS o
        JOIN customers AS c USING (customer_id)
        GROUP BY c.name
        ORDER BY c.name
        """,
        tables={"orders": orders, "customers": customers},
    )

    assert totals.data.take_all() == [{"name": "Ada", "total": 25}, {"name": "Lin", "total": 7}]


def test_query_without_input_tables() -> None:
    context = KleinContext()
    context.enable_interactive_mode()

    assert context.sql("SELECT 40 + 2 AS answer", tables={}).data.take_all() == [{"answer": 42}]


def test_streaming_join_group_by_emits_flink_changelog(ray_cluster) -> None:
    context = KleinContext(Configuration("execution.runtime.mode=streaming; state.backend.type=memory"))
    orders = context.from_items(
        [
            {"customer_id": 1, "amount": 10},
            {"customer_id": 1, "amount": 15},
            {"customer_id": 2, "amount": 7},
        ]
    )
    customers = context.from_items(
        [
            {"customer_id": 1, "name": "Ada"},
            {"customer_id": 2, "name": "Lin"},
        ]
    )
    result = context.sql(
        """
        SELECT /*+ STATE_TTL('o'='1h', 'c'='1h') */ c.name, SUM(o.amount) AS total
        FROM orders AS o JOIN customers AS c USING (customer_id)
        GROUP BY c.name
        """,
        tables={"orders": orders, "customers": customers},
    )
    result.write(CollectFunction, concurrency=1, node_type=NodeType.TAKE, name="SQLChangelog")

    handle = context.execute("streaming-sql-changelog")
    handle.wait()
    changes = handle.get()
    by_name = {}
    for row in changes:
        by_name.setdefault(row["name"], []).append((row.row_kind, row["total"]))

    assert by_name == {
        "Ada": [
            (RowKind.INSERT, 10),
            (RowKind.UPDATE_BEFORE, 10),
            (RowKind.UPDATE_AFTER, 25),
        ],
        "Lin": [(RowKind.INSERT, 7)],
    }


def test_unbounded_input_selects_continuous_sql_automatically(ray_cluster) -> None:
    context = KleinContext(Configuration("state.backend.type=memory"))
    events = context.source(
        LoopSourceFunction,
        fn_constructor_kwargs={"record_num": 4, "sleep_interval": 0},
        bounded=False,
        name="events",
    )
    result = context.sql(
        "SELECT idx % 2 AS parity, COUNT(*) AS total FROM events GROUP BY idx % 2",
        tables={"events": events},
    )
    result.write(CollectFunction, concurrency=1, node_type=NodeType.TAKE, name="SQLUnboundedChangelog")

    handle = context.execute("unbounded-streaming-sql")
    handle.wait()
    changes = handle.get()
    by_parity = {}
    for row in changes:
        by_parity.setdefault(row["parity"], []).append((row.row_kind, row["total"]))

    expected_updates = [
        (RowKind.INSERT, 1),
        (RowKind.UPDATE_BEFORE, 1),
        (RowKind.UPDATE_AFTER, 2),
    ]
    assert by_parity == {0: expected_updates, 1: expected_updates}


def test_cte_filter_limit_and_union_all() -> None:
    context = KleinContext()
    context.enable_interactive_mode()
    events = context.data.from_items(
        [
            {"id": 1, "score": 5},
            {"id": 2, "score": 20},
            {"id": 3, "score": 30},
        ]
    )

    result = context.sql(
        """
        WITH selected AS (
            SELECT id, score * 2 AS score FROM events WHERE score >= 20
        )
        SELECT id, score FROM selected
        UNION ALL
        SELECT 99 AS id, 1 AS score
        """,
        tables={"events": events},
    )

    # Distributed datasets do not promise block order without ORDER BY.
    assert sorted(result.data.take_all(), key=lambda row: row["id"]) == [
        {"id": 2, "score": 40},
        {"id": 3, "score": 60},
        {"id": 99, "score": 1},
    ]


def test_flink_ddl_lazily_connects_filesystem_source_and_sink(tmp_path) -> None:
    source_path = tmp_path / "events.json"
    output_path = tmp_path / "output"
    source_path.write_text('{"id": 1, "amount": 5}\n{"id": 2, "amount": 8}\n', encoding="utf-8")
    context = KleinContext()

    context.execute_sql(
        f"""
        CREATE TABLE input_events (id BIGINT, amount BIGINT) WITH (
            'connector'='filesystem',
            'path'='{source_path}',
            'format'='json'
        )
        """
    )
    context.execute_sql(
        f"""
        CREATE TABLE output_events (id BIGINT, doubled BIGINT) WITH (
            'connector'='filesystem',
            'path'='{output_path}',
            'format'='parquet'
        )
        """
    )
    context.execute_sql("INSERT INTO output_events SELECT id, amount * 2 AS doubled FROM input_events")

    context.execute("sql-table-insert").wait()

    import ray.data

    assert ray.data.read_parquet(output_path).sort("id").take_all() == [
        {"id": 1, "doubled": 10},
        {"id": 2, "doubled": 16},
    ]
