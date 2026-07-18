# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import pytest
from sqlglot import parse_one

from ray.klein import ChangelogRow, KleinContext, RowKind, SQLQueryError
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
