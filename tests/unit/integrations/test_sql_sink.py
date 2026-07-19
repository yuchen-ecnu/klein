# SPDX-License-Identifier: Apache-2.0
"""Unit tests for the streaming DB-API 2.0 sink lifecycle."""

import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace

import pytest

from ray.klein.api.functions.logical_function import LogicalFunction
from ray.klein.api.runtime_info import RuntimeInfo
from ray.klein.api.source_function import SourceFunction
from ray.klein.config.configuration import Configuration
from ray.klein.integrations.sql import StreamingSQLSink
from ray.klein.observability.metrics.metric_group import JobMetricGroup
from ray.klein.runtime.context.runtime_context import TaskRuntimeContext
from ray.klein.runtime.operator.chained_source_operator import ChainedSourceOperator
from ray.klein.runtime.operator.sink import SinkOperator
from ray.klein.runtime.operator.source import SourceFunctionOperator


def _opened_sqlite_sink(database: Path) -> StreamingSQLSink:
    sink = StreamingSQLSink(
        "INSERT INTO events(id, name) VALUES(?, ?)",
        lambda: sqlite3.connect(database),
    )
    sink.open(SimpleNamespace(task_index=2))
    return sink


def test_streaming_sql_sink_batches_rows_and_preserves_first_record_column_order(tmp_path: Path) -> None:
    database = tmp_path / "events.db"
    with sqlite3.connect(database) as connection:
        connection.execute("CREATE TABLE events(id, name)")

    sink = _opened_sqlite_sink(database)
    sink.write({"id": 1, "name": "first"})
    sink.write({"name": "second", "id": 2})

    with sqlite3.connect(database) as connection:
        assert connection.execute("SELECT * FROM events").fetchall() == []

    sink.flush()
    sink.close()

    with sqlite3.connect(database) as connection:
        assert connection.execute("SELECT * FROM events ORDER BY id").fetchall() == [
            (1, "first"),
            (2, "second"),
        ]


def test_streaming_sql_sink_commits_when_ray_data_batch_size_is_reached() -> None:
    connection = _Connection()
    sink = StreamingSQLSink("INSERT INTO events VALUES(?)", lambda: connection)
    sink.open(SimpleNamespace(task_index=0))

    for value in range(StreamingSQLSink.MAX_ROWS_PER_WRITE):
        sink.write({"id": value})

    assert connection.cursor_instance.batches == [[(value,) for value in range(128)]]
    assert connection.commit_count == 1


def test_streaming_sql_sink_creates_connection_on_the_writer_thread() -> None:
    connection = _Connection()
    connection_threads = []

    def connection_factory():
        connection_threads.append(threading.get_ident())
        return connection

    sink = StreamingSQLSink("INSERT INTO events VALUES(?)", connection_factory)
    sink.open(SimpleNamespace(task_index=0))
    assert connection_threads == []

    def write_and_close():
        writer_thread = threading.get_ident()
        sink.write({"id": 1})
        sink.close()
        return writer_thread

    with ThreadPoolExecutor(max_workers=1) as executor:
        writer_thread = executor.submit(write_and_close).result()

    assert connection_threads == [writer_thread]


def test_streaming_sql_sink_consumes_a_source_operator_chain(tmp_path: Path) -> None:
    database = tmp_path / "chained-events.db"
    with sqlite3.connect(database) as connection:
        connection.execute("CREATE TABLE events(id)")

    root = SourceFunctionOperator(
        LogicalFunction(_ValuesSource, fn_constructor_args=[[{"id": 1}, {"id": 2}]]),
        bounded=True,
    )
    sink = SinkOperator(
        LogicalFunction(
            StreamingSQLSink,
            fn_constructor_args=["INSERT INTO events VALUES(?)", lambda: sqlite3.connect(database)],
        )
    )
    root.id, root.name = 1, "source"
    sink.id, sink.name = 2, "sql-sink"
    chain = ChainedSourceOperator(root, [sink])
    chain.open(None, _task_context())

    def run_and_close() -> None:
        chain.run()
        chain.flush()
        chain.close()

    with ThreadPoolExecutor(max_workers=1) as executor:
        executor.submit(run_and_close).result()

    with sqlite3.connect(database) as connection:
        assert connection.execute("SELECT id FROM events ORDER BY id").fetchall() == [(1,), (2,)]


def test_streaming_sql_sink_rolls_back_and_keeps_batch_when_write_fails() -> None:
    connection = _Connection(fail_writes=True)
    sink = StreamingSQLSink("INSERT INTO events VALUES(?)", lambda: connection)
    sink.open(SimpleNamespace(task_index=0))
    sink.write({"id": 1})

    with pytest.raises(RuntimeError, match="write failed"):
        sink.flush()

    assert connection.rollback_count == 1
    assert sink._buffer == [(1,)]


def test_streaming_sql_sink_rejects_schema_changes() -> None:
    sink = StreamingSQLSink("INSERT INTO events VALUES(?)", _Connection)
    sink.open(SimpleNamespace(task_index=0))
    sink.write({"id": 1})

    with pytest.raises(ValueError, match="columns changed"):
        sink.write({"other": 2})


class _Cursor:
    def __init__(self, *, fail_writes: bool = False) -> None:
        self.fail_writes = fail_writes
        self.batches = []
        self.closed = False

    def executemany(self, _sql, values) -> None:
        if self.fail_writes:
            raise RuntimeError("write failed")
        self.batches.append(list(values))

    def close(self) -> None:
        self.closed = True


class _ValuesSource(SourceFunction):
    def __init__(self, values) -> None:
        self.values = values

    def run(self, context) -> None:
        for value in self.values:
            context.collect(value)

    def cancel(self) -> None:
        return None

    def snapshot_state(self, checkpoint_id: int):
        return checkpoint_id

    def restore_state(self, state) -> None:
        return None


class _Connection:
    def __init__(self, *, fail_writes: bool = False) -> None:
        self.cursor_instance = _Cursor(fail_writes=fail_writes)
        self.commit_count = 0
        self.rollback_count = 0
        self.closed = False

    def cursor(self) -> _Cursor:
        return self.cursor_instance

    def commit(self) -> None:
        self.commit_count += 1

    def rollback(self) -> None:
        self.rollback_count += 1

    def close(self) -> None:
        self.closed = True


def _task_context() -> TaskRuntimeContext:
    task_metrics = JobMetricGroup("test").add_task_group("1:0", "source -> sql-sink", 0)
    return TaskRuntimeContext(
        "source -> sql-sink",
        0,
        1,
        Configuration(),
        task_metrics,
        SimpleNamespace(),
        RuntimeInfo(),
        "test",
    )
