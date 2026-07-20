# SPDX-License-Identifier: Apache-2.0
from ray.data.expressions import col, download

from ray.klein.api.collect_function import CollectFunction
from ray.klein.api.klein_context import KleinContext
from ray.klein.api.node_type import NodeType
from ray.klein.api.row_kind import RowKind
from ray.klein.config.configuration import Configuration
from tests.support.streaming import LoopSourceFunction
from tests.support.terminal import execute_terminal


def test_join_group_expression_uses_ray_native_operators() -> None:
    context = KleinContext()
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

    rows = execute_terminal(totals.data.take_all(), job_name="sql-join-group")
    assert rows == [{"name": "Ada", "total": 25}, {"name": "Lin", "total": 7}]


def test_query_without_input_tables() -> None:
    context = KleinContext()

    sink = context.sql("SELECT 40 + 2 AS answer", tables={}).data.take_all()
    assert execute_terminal(sink, job_name="sql-without-input") == [{"answer": 42}]


def test_download_expression_uses_ray_data_operator(tmp_path) -> None:
    payload = b"ray-klein expression download"
    source_file = tmp_path / "payload.bin"
    source_file.write_bytes(payload)
    context = KleinContext()
    files = context.data.from_items([{"id": 1, "uri": f"local://{source_file}"}])

    result = context.sql(
        "SELECT id, DOWNLOAD(uri) AS body FROM files",
        tables={"files": files},
    )

    rows = execute_terminal(result.data.take_all(), job_name="sql-download")
    assert rows == [{"id": 1, "body": payload}]


def test_ray_data_scalar_expressions_execute_natively() -> None:
    context = KleinContext()
    rows = context.data.from_items(
        [
            {"value": -5, "name": "ADA"},
            {"value": 2, "name": "Lin"},
        ]
    )

    result_stream = context.sql(
        "SELECT value / 2 AS ratio, ABS(value) AS magnitude, LOWER(name) AS normalized, "
        "RANDOM(42) AS sample, UUID() AS uid, MONOTONICALLY_INCREASING_ID() AS mid FROM rows",
        tables={"rows": rows},
    )
    result = execute_terminal(result_stream.data.take_all(), job_name="sql-scalar-expressions")

    assert [row["ratio"] for row in result] == [-2.5, 1.0]
    assert [row["magnitude"] for row in result] == [5, 2]
    assert [row["normalized"] for row in result] == ["ada", "lin"]
    assert all(0 <= row["sample"] < 1 for row in result)
    assert len({row["uid"] for row in result}) == 2
    assert len({row["mid"] for row in result}) == 2


def test_streaming_sql_executes_download_and_synthetic_expressions(ray_cluster, tmp_path) -> None:
    first = tmp_path / "first.bin"
    second = tmp_path / "second.bin"
    first.write_bytes(b"first")
    second.write_bytes(b"second")
    context = KleinContext(Configuration("execution.runtime.mode=streaming; state.backend.type=memory"))
    files = context.from_values(
        {"id": 1, "uri": f"local://{first}"},
        {"id": 2, "uri": f"local://{second}"},
    )

    result = context.sql(
        "SELECT *, DOWNLOAD(uri) AS body, RANDOM(42) AS sample, UUID() AS uid, "
        "MONOTONICALLY_INCREASING_ID() AS mid FROM files",
        tables={"files": files},
    )
    sink = result.write(CollectFunction, concurrency=1, node_type=NodeType.TAKE, name="SQLStreamingExpressions")

    handle = context.execute("streaming-sql-expressions", sinks=(sink,))
    handle.wait()
    rows = sorted(handle.get(), key=lambda row: row["id"])

    assert [row["body"] for row in rows] == [b"first", b"second"]
    assert all(0 <= row["sample"] < 1 for row in rows)
    assert len({row["uid"] for row in rows}) == 2
    assert len({row["mid"] for row in rows}) == 2


def test_stream_data_expressions_execute_in_streaming_mode(ray_cluster, tmp_path) -> None:
    payload = tmp_path / "payload.bin"
    payload.write_bytes(b"payload")
    context = KleinContext(Configuration("execution.runtime.mode=streaming; state.backend.type=memory"))
    result = (
        context.from_values({"uri": f"local://{payload}", "value": 3})
        .data.with_column("body", download("uri"))
        .data.with_column("doubled", col("value") * 2)
        .data.filter(expr=col("doubled") == 6)
    )
    sink = result.write(CollectFunction, concurrency=1, node_type=NodeType.TAKE, name="StreamDataExpressions")

    handle = context.execute("stream-data-expressions", sinks=(sink,))
    handle.wait()

    assert handle.get() == [{"uri": f"local://{payload}", "value": 3, "body": b"payload", "doubled": 6}]


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
    sink = result.write(CollectFunction, concurrency=1, node_type=NodeType.TAKE, name="SQLChangelog")

    handle = context.execute("streaming-sql-changelog", sinks=(sink,))
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
    sink = result.write(CollectFunction, concurrency=1, node_type=NodeType.TAKE, name="SQLUnboundedChangelog")

    handle = context.execute("unbounded-streaming-sql", sinks=(sink,))
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


def test_cte_filter_and_union_all() -> None:
    context = KleinContext()
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
    rows = execute_terminal(result.data.take_all(), job_name="sql-cte-union")
    assert sorted(rows, key=lambda row: row["id"]) == [
        {"id": 2, "score": 40},
        {"id": 3, "score": 60},
        {"id": 99, "score": 1},
    ]


def test_order_by_multiple_columns_and_limit() -> None:
    context = KleinContext()
    events = context.from_items(
        [
            {"id": 3, "score": 20},
            {"id": 1, "score": 30},
            {"id": 2, "score": 30},
            {"id": 4, "score": 10},
        ]
    )

    result = context.sql(
        "SELECT id, score FROM events ORDER BY score DESC, id ASC LIMIT 2",
        tables={"events": events},
    )

    rows = execute_terminal(result.take_all(), job_name="sql-order-limit")
    assert rows == [
        {"id": 1, "score": 30},
        {"id": 2, "score": 30},
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
    sink = context.execute_sql("INSERT INTO output_events SELECT id, amount * 2 AS doubled FROM input_events")

    context.execute("sql-table-insert", sinks=(sink,)).wait()

    import ray.data

    assert ray.data.read_parquet(output_path).sort("id").take_all() == [
        {"id": 1, "doubled": 10},
        {"id": 2, "doubled": 16},
    ]
