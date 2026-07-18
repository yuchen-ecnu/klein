# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from typing import Any

import pytest
from sqlglot import parse_one

from ray.klein import KleinContext, SQLQueryError, SQLSession, TableFactory, sql
from ray.klein._internal.sql.execution import _build_aggregate_plan, _shuffle_partitions
from ray.klein.api.source_context import SourceContext
from ray.klein.api.source_function import SourceFunction
from ray.klein.integrations.filesystem.streaming_file_sink import StreamingFileSink
from tests.support.ray_data import FakeDataset, logical_function_of


class _UnboundedSource(SourceFunction):
    def run(self, context: SourceContext) -> None:
        return None

    def cancel(self) -> None:
        return None

    def snapshot_state(self, checkpoint_id: int) -> None:
        return None

    def restore_state(self, state) -> None:
        return None


def test_session_builds_a_lazy_multi_table_query(monkeypatch) -> None:
    context = KleinContext()
    orders = context.data.source(lambda: FakeDataset())
    customers = context.data.source(lambda: FakeDataset())
    captured: list[Any] = []

    def fake_sql_transform(primary, query, table_names, *others, **options):
        captured.append((primary, query, table_names, others, options))
        return primary

    monkeypatch.setattr("ray.klein.api.sql_session.sql_transform", fake_sql_transform)
    result = context.sql_session.sql(
        "SELECT * FROM orders JOIN customers USING (customer_id)",
        tables={"orders": orders, "customers": customers},
        num_cpus=2,
    )

    left, right = FakeDataset(), FakeDataset()
    assert result.input_streams == [orders, customers]
    assert logical_function_of(result).to_batch([left, right]) is left
    assert captured == [
        (
            left,
            "SELECT * FROM orders JOIN customers USING (customer_id)",
            ("orders", "customers"),
            (right,),
            {"num_cpus": 2},
        )
    ]


def test_top_level_sql_discovers_dataframe_variables() -> None:
    context = KleinContext()
    orders = context.data.source(lambda: FakeDataset())

    result = sql("SELECT * FROM orders")

    assert result.context is context
    assert result.input_streams == [orders]


def test_context_discovery_ignores_streams_from_other_contexts() -> None:
    context = KleinContext()
    other_context = KleinContext()
    orders = context.data.source(lambda: FakeDataset())
    other = other_context.data.source(lambda: FakeDataset())

    result = context.sql("SELECT * FROM orders")

    assert other.context is other_context
    assert result.input_streams == [orders]


def test_dataframe_sql_registers_self_and_extra_tables(monkeypatch) -> None:
    context = KleinContext()
    orders = context.data.source(lambda: FakeDataset())
    customers = context.data.source(lambda: FakeDataset())

    def fake_sql_transform(primary, query, table_names, *others, **options):
        assert query == "SELECT * FROM self JOIN customers USING (customer_id)"
        assert table_names == ("self", "customers")
        return primary

    monkeypatch.setattr("ray.klein.api.sql_session.sql_transform", fake_sql_transform)
    result = orders.sql(
        "SELECT * FROM self JOIN customers USING (customer_id)",
        tables={"customers": customers},
    )

    assert len(result.input_streams) == 2


def test_session_temp_views_are_persistent_and_replaceable() -> None:
    context = KleinContext()
    first = context.data.source(lambda: FakeDataset())
    second = context.data.source(lambda: FakeDataset())
    session = SQLSession(context)

    assert session.create_temp_view("items", first) is first
    session.create_temp_view("items", second)
    assert session.temp_views == ("items",)
    assert session.sql("SELECT * FROM items").input_streams == [second]
    session.drop_temp_view("items")
    assert session.temp_views == ()


def test_session_only_compiles_views_referenced_by_the_query() -> None:
    context = KleinContext()
    used = context.data.source(lambda: FakeDataset())
    unused = context.data.source(lambda: FakeDataset())
    session = SQLSession(context)
    session.create_temp_view("used", used)
    session.create_temp_view("unused", unused)

    result = session.sql("WITH rows AS (SELECT * FROM used) SELECT * FROM rows")

    assert result.input_streams == [used]


@pytest.mark.parametrize(
    ("query", "message"),
    [
        ("", "non-empty"),
        ("SELECT 1; SELECT 2", "exactly one"),
        ("CREATE TABLE bad AS SELECT 1", "only SELECT"),
    ],
)
def test_sql_rejects_invalid_or_mutating_statements(query, message) -> None:
    with pytest.raises(SQLQueryError, match=message):
        KleinContext().sql_session.sql(query)


def test_sql_validates_bindings_and_execution_options() -> None:
    context = KleinContext()
    bounded = context.data.source(lambda: FakeDataset())
    unbounded = context.source(_UnboundedSource)

    with pytest.raises(SQLQueryError, match="Invalid SQL table name"):
        context.sql_session.sql("SELECT 1", tables={"not valid": bounded})
    streaming = context.sql_session.sql("SELECT * FROM events", tables={"events": unbounded})
    assert streaming.context is context
    with pytest.raises(SQLQueryError, match="num_cpus"):
        context.sql_session.sql("SELECT 1", num_cpus=0)


def test_sql_rejects_mixed_contexts() -> None:
    left = KleinContext().data.source(lambda: FakeDataset())
    right = KleinContext().data.source(lambda: FakeDataset())

    with pytest.raises(SQLQueryError, match="multiple contexts"):
        sql(
            "SELECT * FROM left_table CROSS JOIN right_table",
            tables={"left_table": left, "right_table": right},
        )


def test_aggregate_projection_planning_resolves_runtime_expression_types() -> None:
    statement = parse_one("SELECT c.name, SUM(o.amount) AS total FROM rows GROUP BY c.name")
    group_expression = statement.args["group"].expressions[0]

    computed, aggregators, outputs = _build_aggregate_plan(
        statement.expressions,
        [("_klein_group_0", group_expression)],
    )

    assert [name for name, _ in computed] == ["_klein_group_0", "_klein_aggregate_input_0"]
    assert len(aggregators) == 1
    assert outputs == [("name", "_klein_group_0"), ("total", "_klein_aggregate_0")]


@pytest.mark.parametrize(
    ("configured", "input_blocks", "expected"),
    [
        (8, (3, 2), 3),
        (2, (8,), 2),
        (0, (), 1),
    ],
)
def test_sql_shuffle_width_is_bounded_by_available_input_blocks(
    monkeypatch,
    configured: int,
    input_blocks: tuple[int, ...],
    expected: int,
) -> None:
    from ray.data import DataContext

    class _BlockCount:
        def __init__(self, blocks: int) -> None:
            self._blocks = blocks

        def num_blocks(self) -> int:
            return self._blocks

    monkeypatch.setattr(DataContext.get_current(), "min_parallelism", configured)

    datasets = tuple(_BlockCount(blocks) for blocks in input_blocks)
    assert _shuffle_partitions(*datasets) == expected


def test_sql_shuffle_width_uses_cluster_capacity_for_lazy_datasets(monkeypatch) -> None:
    from ray.data import DataContext

    import ray

    class _LazyDataset:
        @staticmethod
        def num_blocks() -> int:
            raise NotImplementedError

    monkeypatch.setattr(DataContext.get_current(), "min_parallelism", 200)
    monkeypatch.setattr(ray, "is_initialized", lambda: True)
    monkeypatch.setattr(ray, "cluster_resources", lambda: {"CPU": 4})

    assert _shuffle_partitions(_LazyDataset()) == 4
    assert _shuffle_partitions(_LazyDataset(), known_blocks=(3,)) == 3


def test_flink_style_create_and_drop_table() -> None:
    session = SQLSession(KleinContext())

    table = session.execute_sql(
        """
        CREATE TEMPORARY TABLE events (
            event_id BIGINT NOT NULL,
            payload STRING
        ) WITH (
            'connector' = 'filesystem',
            'path' = '/tmp/events',
            'format' = 'json',
            'source.override_num_blocks' = '4'
        )
        """
    )

    assert table.name == "events"
    assert [(column.name, column.data_type, column.nullable) for column in table.columns] == [
        ("event_id", "BIGINT", False),
        ("payload", "TEXT", True),
    ]
    assert table.temporary is True
    assert session.tables == ("events",)
    assert (
        session.execute_sql(
            "CREATE TABLE IF NOT EXISTS events (id BIGINT) "
            "WITH ('connector'='filesystem', 'path'='/ignored', 'format'='json')"
        )
        is table
    )

    session.execute_sql("DROP TABLE events")
    assert session.tables == ()
    session.execute_sql("DROP TABLE IF EXISTS events")


def test_filesystem_insert_builds_transactional_stream_sink(tmp_path) -> None:
    context = KleinContext()
    session = context.sql_session
    session.create_temp_view("source_events", context.data.from_items([{"id": 1, "payload": "a"}]))
    session.execute_sql(
        f"""
        CREATE TABLE output_events (id BIGINT, payload STRING) WITH (
            'connector'='filesystem',
            'path'='{tmp_path / "output"}',
            'format'='json',
            'sink.parallelism'='2',
            'sink.filename-prefix'='events',
            'sink.rolling-policy.file-size'='64 MiB',
            'sink.rolling-policy.rollover-interval'='5 min'
        )
        """
    )

    sink = session.execute_sql("INSERT INTO output_events SELECT id, payload FROM source_events")
    logical_function = logical_function_of(sink)

    assert logical_function.function is StreamingFileSink
    assert logical_function.constructor_args == (str(tmp_path / "output"), "json")
    assert logical_function.constructor_kwargs["filename_prefix"] == "events"
    assert logical_function.constructor_kwargs["max_bytes_per_file"] == 64 * (1 << 20)
    assert sink.resources.effective_concurrency == 2


class _RecordingFactory(TableFactory):
    identifier = "recording"

    def __init__(self) -> None:
        self.validated = []
        self.sources = []
        self.sinks = []

    def validate(self, table) -> None:
        self.validated.append(table)

    def create_source(self, context, table):
        self.sources.append(table)
        return context.data.source(lambda: FakeDataset())

    def create_sink(self, stream, table):
        self.sinks.append((stream, table))
        return stream


def test_table_factories_are_validated_eagerly_and_instantiated_lazily() -> None:
    session = SQLSession(KleinContext())
    factory = _RecordingFactory()
    session.register_table_factory(factory)

    session.execute_sql("CREATE TABLE source_table (id BIGINT) WITH ('connector'='recording')")
    session.execute_sql("CREATE TABLE sink_table (id BIGINT) WITH ('connector'='recording')")

    assert len(factory.validated) == 2
    assert factory.sources == []
    assert factory.sinks == []

    result = session.execute_sql("INSERT INTO sink_table SELECT id FROM source_table")

    assert len(factory.sources) == 1
    assert len(factory.sinks) == 1
    assert result is factory.sinks[0][0]
