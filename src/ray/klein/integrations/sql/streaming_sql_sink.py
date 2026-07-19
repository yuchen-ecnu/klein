# SPDX-License-Identifier: Apache-2.0
"""Buffered DB-API 2.0 sink for Klein streaming jobs."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from ray.klein._internal.logging import get_logger
from ray.klein.api.runtime_context import RuntimeContext
from ray.klein.api.sink_function import SinkFunction

logger = get_logger(__name__)

Connection = Any
Cursor = Any


class StreamingSQLSink(SinkFunction):
    """Write streaming records through a DB-API 2.0 ``executemany`` call.

    Full batches and checkpoint flushes are committed independently. A commit
    can therefore become visible before its corresponding Klein checkpoint is
    durable, so recovery provides at-least-once delivery.
    """

    MAX_ROWS_PER_WRITE = 128

    def __init__(self, sql: str, connection_factory: Callable[[], Connection]) -> None:
        if not isinstance(sql, str) or not sql.strip():
            raise ValueError("sql must be a non-empty string")
        if not callable(connection_factory):
            raise TypeError("connection_factory must be callable")
        self._sql = sql
        self._connection_factory = connection_factory
        self._connection: Connection | None = None
        self._cursor: Cursor | None = None
        self._columns: tuple[str, ...] | None = None
        self._buffer: list[tuple[Any, ...]] = []
        self._opened = False
        self._task_index = -1

    def open(self, runtime_context: RuntimeContext) -> None:
        if self._opened:
            return
        # Function.open() runs on the actor event-loop thread, while record
        # processing runs on the operator's dedicated executor. Many DB-API
        # connections are thread-affine, so create the connection lazily from
        # the first write/flush on that executor instead of here.
        self._opened = True
        self._task_index = runtime_context.task_index

    def _connect(self) -> tuple[Connection, Cursor]:
        connection = self._connection_factory()
        try:
            _check_connection(connection)
            cursor = connection.cursor()
            _check_cursor(cursor)
        except Exception:
            _close_if_supported(connection)
            raise
        self._connection = connection
        self._cursor = cursor
        logger.info("Opened SQL sink connection for subtask %s", self._task_index)
        return connection, cursor

    def write(self, value: dict[str, Any]) -> None:
        if not isinstance(value, Mapping):
            raise TypeError("SQL sink records must be mappings")
        self._require_open()
        columns = tuple(value)
        if self._columns is None:
            self._columns = columns
        elif set(columns) != set(self._columns):
            missing = [column for column in self._columns if column not in value]
            extra = [column for column in columns if column not in self._columns]
            raise ValueError(f"SQL sink record columns changed; missing={missing}, extra={extra}")

        self._buffer.append(tuple(value[column] for column in self._columns))
        if len(self._buffer) >= self.MAX_ROWS_PER_WRITE:
            self.flush()

    def flush(self) -> None:
        if not self._buffer:
            return
        connection, cursor = self._require_open()
        batch = list(self._buffer)
        try:
            cursor.executemany(self._sql, batch)
            connection.commit()
        except Exception:
            _rollback_if_supported(connection)
            raise
        self._buffer.clear()

    def close(self) -> None:
        first_error: Exception | None = None
        try:
            self.flush()
        except Exception as error:
            first_error = error

        cursor, self._cursor = self._cursor, None
        connection, self._connection = self._connection, None
        self._opened = False
        for resource in (cursor, connection):
            try:
                _close_if_supported(resource)
            except Exception as error:
                first_error = first_error or error
        if first_error is not None:
            raise first_error.with_traceback(first_error.__traceback__)

    def _require_open(self) -> tuple[Connection, Cursor]:
        if not self._opened:
            raise RuntimeError("SQL sink must be opened before use")
        if self._connection is None or self._cursor is None:
            return self._connect()
        return self._connection, self._cursor

    def __repr__(self) -> str:
        return f"StreamingSQLSink(sql={self._sql!r})"


def _check_connection(connection: Connection) -> None:
    for attribute in ("close", "commit", "cursor"):
        if not callable(getattr(connection, attribute, None)):
            raise ValueError(
                f"connection_factory created a non-DB-API 2.0 connection without a callable {attribute!r} attribute"
            )


def _check_cursor(cursor: Cursor) -> None:
    if not callable(getattr(cursor, "executemany", None)):
        raise ValueError("The DB-API 2.0 connection created a cursor without a callable 'executemany' attribute")


def _rollback_if_supported(connection: Connection) -> None:
    try:
        connection.rollback()
    except Exception as error:
        if isinstance(error, AttributeError) or error.__class__.__name__ == "NotSupportedError":
            return
        raise


def _close_if_supported(resource: Any) -> None:
    close = getattr(resource, "close", None)
    if callable(close):
        close()
