# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import math
from decimal import Decimal
from types import SimpleNamespace

import pytest
from sqlglot import parse_one

from ray.klein import ChangelogRow, KleinContext, RowKind, SQLQueryError
from ray.klein._internal.sql.execution import _FilterRow
from ray.klein._internal.sql.streaming import (
    _AddStreamingExpressions,
    _AsyncAddStreamingExpressions,
    _AsyncRayProjectChangelogRow,
    _ProjectChangelogRow,
    _RayProjectChangelogRow,
)
from ray.klein.api.collector import Collector
from ray.klein.api.runtime_info import RuntimeInfo
from ray.klein.config.configuration import Configuration
from ray.klein.config.state_options import StateOptions
from ray.klein.observability.metrics.metric_group import JobMetricGroup
from ray.klein.runtime.context.runtime_context import TaskRuntimeContext
from ray.klein.runtime.message import Record
from ray.klein.runtime.operator.sql_aggregate_operator import SQLAggregateOperator
from ray.klein.runtime.operator.sql_join_operator import SQLRegularJoinOperator
from ray.klein.runtime.operator.sql_top_n_operator import SQLTopNOperator
from tests.support.ray_data import logical_function_of


class _RecordingCollector(Collector):
    def __init__(self) -> None:
        super().__init__()
        self.records: list[Record] = []

    def collect(self, record: Record) -> None:
        self.records.append(record)


def _open(operator, name: str = "streaming-sql") -> _RecordingCollector:
    configuration = Configuration({"execution.runtime.mode": "streaming"})
    configuration.set(StateOptions.BACKEND, "memory")
    metrics = JobMetricGroup(name).add_task_group("1", name, 0)
    runtime_context = TaskRuntimeContext(
        name,
        0,
        1,
        configuration,
        metrics,
        None,
        RuntimeInfo(),
        name,
    )
    collector = _RecordingCollector()
    operator.id = 1
    operator.name = name
    operator.open(collector, runtime_context)
    return collector


def _changes(collector: _RecordingCollector) -> list[tuple[RowKind, dict]]:
    return [(record.block.row_kind, dict(record.block)) for record in collector.records]


def test_group_aggregate_emits_flink_retract_changelog() -> None:
    statement = parse_one("SELECT name, SUM(amount) AS total FROM orders GROUP BY name")
    operator = SQLAggregateOperator(
        group_expressions=statement.args["group"].expressions,
        projections=statement.expressions,
    )
    collector = _open(operator)

    operator.process_element(Record({"name": "Ada", "amount": 10}))
    operator.process_element(Record({"name": "Ada", "amount": 15}))
    operator.process_element(Record(ChangelogRow.delete({"name": "Ada", "amount": 10})))

    assert _changes(collector) == [
        (RowKind.INSERT, {"name": "Ada", "total": 10}),
        (RowKind.UPDATE_BEFORE, {"name": "Ada", "total": 10}),
        (RowKind.UPDATE_AFTER, {"name": "Ada", "total": 25}),
        (RowKind.UPDATE_BEFORE, {"name": "Ada", "total": 25}),
        (RowKind.UPDATE_AFTER, {"name": "Ada", "total": 15}),
    ]
    operator.close()


def test_insert_only_aggregate_keeps_incremental_constant_size_state() -> None:
    statement = parse_one("SELECT COUNT(*) AS count, SUM(amount) AS total, AVG(amount) AS average FROM orders")
    operator = SQLAggregateOperator(
        group_expressions=(),
        projections=statement.expressions,
        retractable=False,
    )
    collector = _open(operator)

    for amount in range(1, 1_001):
        operator.process_element(Record({"amount": amount}))

    accumulator = operator._backend.get(operator._aggregate_state)
    assert accumulator["row_count"] == 1_000
    assert accumulator["row_counts"] is None
    assert "rows" not in accumulator
    assert _changes(collector)[-1] == (
        RowKind.UPDATE_AFTER,
        {"count": 1_000, "total": 500_500, "average": 500.5},
    )
    operator.close()


def test_incremental_min_max_and_null_aggregates_handle_retractions() -> None:
    statement = parse_one("SELECT COUNT(value) AS count, MIN(value) AS minimum, MAX(value) AS maximum FROM orders")
    operator = SQLAggregateOperator(group_expressions=(), projections=statement.expressions)
    collector = _open(operator)

    operator.process_element(Record({"value": 3}))
    operator.process_element(Record({"value": None}))
    operator.process_element(Record({"value": 1}))
    operator.process_element(Record(ChangelogRow.delete({"value": 1})))

    assert _changes(collector)[-1] == (
        RowKind.UPDATE_AFTER,
        {"count": 1, "minimum": 3, "maximum": 3},
    )
    operator.close()


def test_incremental_aggregate_tracks_duplicate_rows_as_a_multiset() -> None:
    statement = parse_one("SELECT COUNT(*) AS count, SUM(value) AS total FROM rows")
    operator = SQLAggregateOperator(group_expressions=(), projections=statement.expressions)
    collector = _open(operator)

    operator.process_element(Record({"value": 2}))
    operator.process_element(Record({"value": 2}))
    operator.process_element(Record(ChangelogRow.delete({"value": 2})))

    assert _changes(collector)[-1] == (
        RowKind.UPDATE_AFTER,
        {"count": 1, "total": 2},
    )
    accumulator = operator._backend.get(operator._aggregate_state)
    assert list(accumulator["row_counts"].values()) == [1]
    operator.close()


def test_incremental_aggregate_supports_decimal_values() -> None:
    statement = parse_one("SELECT SUM(value) AS total, AVG(value) AS average FROM rows")
    operator = SQLAggregateOperator(group_expressions=(), projections=statement.expressions)
    collector = _open(operator)

    operator.process_element(Record({"value": Decimal("1.1")}))
    operator.process_element(Record({"value": Decimal("2.2")}))
    operator.process_element(Record(ChangelogRow.delete({"value": Decimal("1.1")})))

    assert _changes(collector)[-1] == (
        RowKind.UPDATE_AFTER,
        {"total": Decimal("2.2"), "average": Decimal("2.2")},
    )
    operator.close()


def test_incremental_float_sum_is_reversible_after_cancellation() -> None:
    statement = parse_one("SELECT SUM(value) AS total, AVG(value) AS average FROM rows")
    operator = SQLAggregateOperator(group_expressions=(), projections=statement.expressions)
    collector = _open(operator)

    operator.process_element(Record({"value": 1e16}))
    operator.process_element(Record({"value": 1.0}))
    operator.process_element(Record(ChangelogRow.delete({"value": 1e16})))

    assert _changes(collector)[-1] == (
        RowKind.UPDATE_AFTER,
        {"total": 1.0, "average": 1.0},
    )
    operator.close()


def test_incremental_aggregate_nan_retraction_restores_finite_results() -> None:
    statement = parse_one(
        "SELECT SUM(value) AS total, AVG(value) AS average, MIN(value) AS minimum, MAX(value) AS maximum FROM rows"
    )
    operator = SQLAggregateOperator(group_expressions=(), projections=statement.expressions)
    collector = _open(operator)

    operator.process_element(Record({"value": 3.0}))
    operator.process_element(Record({"value": float("nan")}))
    with_nan = dict(collector.records[-1].block)
    assert with_nan["minimum"] == 3.0
    assert all(math.isnan(with_nan[name]) for name in ("total", "average", "maximum"))

    operator.process_element(Record(ChangelogRow.delete({"value": float("nan")})))

    assert _changes(collector)[-1] == (
        RowKind.UPDATE_AFTER,
        {"total": 3.0, "average": 3.0, "minimum": 3.0, "maximum": 3.0},
    )
    operator.close()


def test_grouped_aggregate_routes_nan_as_one_group() -> None:
    statement = parse_one("SELECT category, COUNT(*) AS count FROM rows GROUP BY category")
    operator = SQLAggregateOperator(
        group_expressions=statement.args["group"].expressions,
        projections=statement.expressions,
    )
    collector = _open(operator)

    operator.process_element(Record({"category": float("nan")}))
    operator.process_element(Record({"category": float("nan")}))
    operator.process_element(Record(ChangelogRow.delete({"category": float("nan")})))

    kind, result = _changes(collector)[-1]
    assert kind is RowKind.UPDATE_AFTER
    assert math.isnan(result["category"])
    assert result["count"] == 1
    operator.close()


def test_grouped_nan_key_survives_checkpoint_restore() -> None:
    statement = parse_one("SELECT category, COUNT(*) AS count FROM rows GROUP BY category")
    first = SQLAggregateOperator(
        group_expressions=statement.args["group"].expressions,
        projections=statement.expressions,
    )
    _open(first, "nan-group-restore")
    first.process_element(Record({"category": float("nan")}))
    snapshot = first.snapshot_state()
    first.close()

    restored = SQLAggregateOperator(
        group_expressions=statement.args["group"].expressions,
        projections=statement.expressions,
    )
    collector = _open(restored, "nan-group-restore")
    restored.restore_state(snapshot)
    restored.process_element(Record({"category": float("nan")}))

    kind, result = _changes(collector)[-1]
    assert kind is RowKind.UPDATE_AFTER
    assert math.isnan(result["category"])
    assert result["count"] == 2
    restored.close()


def test_incremental_aggregate_migrates_legacy_list_state() -> None:
    statement = parse_one("SELECT category, COUNT(*) AS count, SUM(value) AS total FROM rows GROUP BY category")
    operator = SQLAggregateOperator(
        group_expressions=statement.args["group"].expressions,
        projections=statement.expressions,
    )
    collector = _open(operator)
    operator._backend.current_key = ("a",)
    operator._backend.put(
        operator._aggregate_state,
        [{"category": "a", "value": 2}, {"category": "a", "value": 2}],
    )

    operator.process_element(Record(ChangelogRow.delete({"category": "a", "value": 2})))

    accumulator = operator._backend.get(operator._aggregate_state)
    assert accumulator["version"] == 1
    assert accumulator["row_count"] == 1
    assert _changes(collector) == [
        (RowKind.UPDATE_BEFORE, {"category": "a", "count": 2, "total": 4}),
        (RowKind.UPDATE_AFTER, {"category": "a", "count": 1, "total": 2}),
    ]
    operator.close()


def test_collection_source_infers_declared_changelog_mode() -> None:
    stream = KleinContext().from_values(
        ChangelogRow.insert({"id": 1}),
        ChangelogRow.delete({"id": 2}),
    )

    assert stream.changelog_mode == frozenset({RowKind.INSERT, RowKind.DELETE})


def test_regular_join_emits_insert_and_delete_changes() -> None:
    operator = SQLRegularJoinOperator(
        left_keys=("o.customer_id",),
        right_keys=("c.customer_id",),
    )
    collector = _open(operator)
    left = Record({"o.customer_id": 1, "o.amount": 10})
    left.input_tag = 0
    right = Record({"c.customer_id": 1, "c.name": "Ada"})
    right.input_tag = 1
    delete_left = Record(ChangelogRow.delete(left.block))
    delete_left.input_tag = 0

    operator.process_element(left)
    operator.process_element(right)
    operator.process_element(delete_left)

    expected = {
        "o.customer_id": 1,
        "o.amount": 10,
        "c.customer_id": 1,
        "c.name": "Ada",
    }
    assert _changes(collector) == [
        (RowKind.INSERT, expected),
        (RowKind.DELETE, expected),
    ]
    operator.close()


def test_regular_join_does_not_match_or_retain_null_keys() -> None:
    operator = SQLRegularJoinOperator(
        left_keys=("o.customer_id",),
        right_keys=("c.customer_id",),
    )
    collector = _open(operator)
    left = Record({"o.customer_id": None, "o.amount": 10})
    left.input_tag = 0
    right = Record({"c.customer_id": None, "c.name": "unknown"})
    right.input_tag = 1

    operator.process_element(left)
    operator.process_element(right)

    assert _changes(collector) == []
    assert list(operator._backend.namespaces(operator._left_state)) == []
    assert list(operator._backend.namespaces(operator._right_state)) == []
    operator.close()


def test_streaming_planner_builds_managed_join_and_aggregate() -> None:
    context = KleinContext(Configuration("execution.runtime.mode=streaming"))
    orders = context.from_items([{"customer_id": 1, "amount": 10}])
    customers = context.from_items([{"customer_id": 1, "name": "Ada"}])

    result = context.sql(
        """
        SELECT /*+ STATE_TTL('o'='1h', 'c'='2h') */ c.name, SUM(o.amount) AS total
        FROM orders AS o JOIN customers AS c USING (customer_id)
        GROUP BY c.name
        """,
        tables={"orders": orders, "customers": customers},
    )

    assert isinstance(result.stream_operator, SQLAggregateOperator)
    join = result.input_streams[0]
    assert isinstance(join.stream_operator, SQLRegularJoinOperator)
    assert result.changelog_mode == frozenset(RowKind)


def test_streaming_global_order_by_follows_flink_restriction() -> None:
    context = KleinContext(Configuration("execution.runtime.mode=streaming"))
    orders = context.from_items([{"amount": 10}])

    with pytest.raises(SQLQueryError, match="ascending time attribute"):
        context.sql("SELECT * FROM orders ORDER BY amount", tables={"orders": orders})


@pytest.mark.parametrize("literal", ["1.5", "'2'", "-1"])
def test_streaming_top_n_rejects_non_integer_limits(literal: str) -> None:
    context = KleinContext(Configuration("execution.runtime.mode=streaming"))
    orders = context.from_items([{"amount": 10}])

    with pytest.raises(SQLQueryError, match="non-negative integer literal"):
        context.sql(
            f"SELECT * FROM orders ORDER BY amount LIMIT {literal}",
            tables={"orders": orders},
        )


@pytest.mark.parametrize(
    ("expression", "function", "async_buffer_size"),
    [
        ("DOWNLOAD(uri)", _AsyncRayProjectChangelogRow, 1),
        ("RANDOM()", _RayProjectChangelogRow, None),
        ("UUID()", _RayProjectChangelogRow, None),
        ("MONOTONICALLY_INCREASING_ID()", _RayProjectChangelogRow, None),
    ],
)
def test_streaming_sql_plans_ray_data_expressions(
    expression: str,
    function: type,
    async_buffer_size: int | None,
) -> None:
    context = KleinContext(Configuration("execution.runtime.mode=streaming"))
    files = context.from_items([{"uri": "local:///tmp/file"}])

    result = context.sql(f"SELECT {expression} AS value FROM files", tables={"files": files})

    logical = logical_function_of(result)
    assert logical.function is function
    assert logical.runtime_info.async_buffer_size == async_buffer_size


@pytest.mark.asyncio
async def test_streaming_sql_downloads_share_one_row_byte_budget(monkeypatch) -> None:
    statement = parse_one("SELECT DOWNLOAD(first_uri) AS first, DOWNLOAD(second_uri) AS second")
    configuration = Configuration({"sql.download.max-bytes": 4})
    runtime_context = SimpleNamespace(
        task_index=0,
        task_name="SQLProject",
        config=configuration,
    )
    limits = []

    def downloaded(uri, _filesystem, _column, policy):
        limits.append(policy.max_bytes)
        payload = b"abc" if uri == "memory://first" else b"xy"
        return payload if len(payload) <= policy.max_bytes else None

    monkeypatch.setattr(
        "ray.klein._internal.streaming_expression._download_uri",
        downloaded,
    )
    projection = _AsyncRayProjectChangelogRow(
        statement.expressions,
        (),
        {},
        runtime_context,
    )

    result = await projection({"first_uri": "memory://first", "second_uri": "memory://second"})

    assert result == {"first": b"abc", "second": None}
    assert limits == [4, 1]


def test_streaming_aggregate_precomputes_download_inputs_asynchronously() -> None:
    context = KleinContext(Configuration("execution.runtime.mode=streaming"))
    files = context.from_items([{"uri": "local:///tmp/file"}])

    result = context.sql(
        "SELECT COUNT(DOWNLOAD(uri)) AS downloaded FROM files",
        tables={"files": files},
    )

    assert isinstance(result.stream_operator, SQLAggregateOperator)
    inputs = result.input_streams[0]
    logical = logical_function_of(inputs)
    assert logical.function is _AsyncAddStreamingExpressions
    assert logical.runtime_info.async_buffer_size == 1


def test_streaming_sql_plans_klein_scalar_function_projection_and_filter() -> None:
    context = KleinContext(Configuration("execution.runtime.mode=streaming"))
    rows = context.from_items([{"value": 2}])
    context.sql_session.register_scalar_function("twice", lambda value: value * 2)
    context.sql_session.register_scalar_function("positive", lambda value: value > 0)

    projected = context.sql("SELECT TWICE(value) AS value FROM rows", tables={"rows": rows})
    filtered = context.sql("SELECT * FROM rows WHERE POSITIVE(value)", tables={"rows": rows})

    assert logical_function_of(projected).function is _ProjectChangelogRow
    assert logical_function_of(filtered.input_streams[0]).function is _FilterRow


def test_streaming_aggregate_precomputes_klein_scalar_function_inputs() -> None:
    context = KleinContext(Configuration("execution.runtime.mode=streaming"))
    rows = context.from_items([{"amount": 2}])
    context.sql_session.register_scalar_function("twice", lambda value: value * 2)

    result = context.sql("SELECT SUM(TWICE(amount)) AS total FROM rows", tables={"rows": rows})

    assert isinstance(result.stream_operator, SQLAggregateOperator)
    logical = logical_function_of(result.input_streams[0])
    assert logical.function is _AddStreamingExpressions


def test_streaming_top_n_emits_retractions_when_rank_changes() -> None:
    statement = parse_one("SELECT name, total FROM totals ORDER BY total DESC LIMIT 2")
    operator = SQLTopNOperator(
        order=statement.args["order"].expressions,
        limit=2,
    )
    collector = _open(operator)

    operator.process_element(Record({"name": "Ada", "total": 10}))
    operator.process_element(Record({"name": "Lin", "total": 7}))
    operator.process_element(Record({"name": "Grace", "total": 12}))

    assert _changes(collector) == [
        (RowKind.INSERT, {"name": "Ada", "total": 10}),
        (RowKind.INSERT, {"name": "Lin", "total": 7}),
        (RowKind.DELETE, {"name": "Lin", "total": 7}),
        (RowKind.INSERT, {"name": "Grace", "total": 12}),
    ]
    operator.close()


def test_insert_only_top_n_retains_only_the_requested_prefix() -> None:
    statement = parse_one("SELECT value FROM rows ORDER BY value DESC LIMIT 2")
    operator = SQLTopNOperator(
        order=statement.args["order"].expressions,
        limit=2,
        retractable=False,
    )
    collector = _open(operator)

    for value in range(1_000):
        operator.process_element(Record({"value": value}))

    state = operator._backend.get(operator._rows_state)
    assert state["rows"] == [{"value": 999}, {"value": 998}]
    assert len(state["rows"]) == 2
    assert _changes(collector)[-2:] == [
        (RowKind.DELETE, {"value": 997}),
        (RowKind.INSERT, {"value": 999}),
    ]
    operator.close()


def test_insert_only_top_n_truncates_migrated_legacy_state() -> None:
    statement = parse_one("SELECT value FROM rows ORDER BY value DESC LIMIT 2")
    operator = SQLTopNOperator(
        order=statement.args["order"].expressions,
        limit=2,
        retractable=False,
    )
    _open(operator)
    operator._backend.current_key = "__klein_global_top_n__"
    operator._backend.put(operator._rows_state, [{"value": value} for value in range(10)])

    operator.process_element(Record({"value": 10}))

    state = operator._backend.get(operator._rows_state)
    assert state == {"version": 1, "rows": [{"value": 10}, {"value": 9}]}
    operator.close()


def test_streaming_top_n_orders_and_retracts_nan_deterministically() -> None:
    statement = parse_one("SELECT id, score FROM rows ORDER BY score DESC LIMIT 2")
    operator = SQLTopNOperator(order=statement.args["order"].expressions, limit=2)
    collector = _open(operator)

    operator.process_element(Record({"id": "nan", "score": float("nan")}))
    operator.process_element(Record({"id": "one", "score": 1.0}))
    operator.process_element(Record({"id": "two", "score": 2.0}))
    operator.process_element(Record(ChangelogRow.delete({"id": "nan", "score": float("nan")})))

    assert _changes(collector)[-2][0] is RowKind.DELETE
    assert _changes(collector)[-2][1]["id"] == "nan"
    assert _changes(collector)[-1] == (
        RowKind.INSERT,
        {"id": "one", "score": 1.0},
    )
    state = operator._backend.get(operator._rows_state)
    assert state["rows"] == [
        {"id": "two", "score": 2.0},
        {"id": "one", "score": 1.0},
    ]
    operator.close()


def test_streaming_top_n_respects_explicit_null_ordering() -> None:
    statement = parse_one("SELECT id, score FROM rows ORDER BY score ASC NULLS FIRST LIMIT 2")
    operator = SQLTopNOperator(order=statement.args["order"].expressions, limit=2)
    collector = _open(operator)

    operator.process_element(Record({"id": "null", "score": None}))
    operator.process_element(Record({"id": "two", "score": 2}))
    operator.process_element(Record({"id": "one", "score": 1}))
    operator.process_element(Record(ChangelogRow.delete({"id": "null", "score": None})))

    assert _changes(collector)[-2:] == [
        (RowKind.DELETE, {"id": "null", "score": None}),
        (RowKind.INSERT, {"id": "two", "score": 2}),
    ]
    operator.close()


def test_streaming_planner_marks_insert_only_sql_state_as_bounded_or_incremental() -> None:
    context = KleinContext(Configuration("execution.runtime.mode=streaming"))
    rows = context.from_items([{"value": 1}])

    aggregate = context.sql("SELECT SUM(value) AS total FROM rows", tables={"rows": rows})
    top_n = context.sql("SELECT value FROM rows ORDER BY value DESC LIMIT 2", tables={"rows": rows})

    assert aggregate.stream_operator._retractable is False
    assert top_n.stream_operator._retractable is False
