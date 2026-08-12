# SPDX-License-Identifier: Apache-2.0
"""Declarative multi-sink submission for one explicit pipeline."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Literal
from uuid import uuid4

from ray.klein.api.node_type import NodeType
from ray.klein.api.stream_sink import StreamSink

if TYPE_CHECKING:
    from types import TracebackType

    from ray.klein.api.job_handle import JobHandle
    from ray.klein.api.pipeline import Pipeline


class StatementSet:
    """Collect multiple side-effect terminals and submit them as one job.

    Use :meth:`add` with a terminal builder, or enter the statement set as a
    context while calling multiple ``write_*`` methods. Result-producing
    ``collect`` and ``take`` terminals are intentionally rejected. Leaving the
    context finishes graph construction; call :meth:`execute` to submit the
    grouped job.

    Example::

        statements = pipeline.create_statement_set()
        with statements:
            prepared.write_parquet("s3://bucket/archive")
            prepared.write_kafka("events", "localhost:9092")
        statements.execute().wait()
    """

    def __init__(self, pipeline: Pipeline) -> None:
        from ray.klein.api.pipeline import Pipeline

        if not isinstance(pipeline, Pipeline):
            raise TypeError("pipeline must be a Pipeline")
        self._pipeline = pipeline
        self._owner_token = uuid4().hex
        self._sinks: list[StreamSink] = []
        self._capture_scope: Any = None
        self._entry_size = 0

    @property
    def pipeline(self) -> Pipeline:
        """Pipeline that owns every statement in this set."""

        return self._pipeline

    @property
    def sinks(self) -> tuple[StreamSink, ...]:
        """Side-effect terminals currently waiting for submission."""

        return tuple(self._sinks)

    def add(
        self,
        terminal: Callable[..., Any],
        /,
        *args: Any,
        **kwargs: Any,
    ) -> StatementSet:
        """Build and add exactly one terminal without submitting it.

        Example::

            statements.add(stream.write_parquet, "s3://bucket/archive")
        """

        if not callable(terminal):
            raise TypeError("terminal must be a callable that builds one sink")
        start = len(self._sinks)
        try:
            with self._pipeline._capture_statement_set(self):
                terminal(*args, **kwargs)
        except BaseException:
            self._rollback(start)
            raise
        added = len(self._sinks) - start
        if added != 1:
            self._rollback(start)
            raise ValueError(f"StatementSet.add() must build exactly one sink, got {added}")
        return self

    def add_sink(self, sink: StreamSink) -> StatementSet:
        """Add an already-built pending side-effect terminal."""

        self._register(sink)
        return self

    def add_insert_sql(self, statement: str, *, num_cpus: float = 1.0) -> StatementSet:
        """Build and add one SQL ``INSERT INTO`` statement."""

        if not isinstance(statement, str):
            raise TypeError("statement must be a string")
        return self.add(self._pipeline.execute_sql, statement, num_cpus=num_cpus)

    def execute(self, job_name: str | None = None) -> JobHandle:
        """Submit every statement as one job and clear the set on success."""

        if self._capture_scope is not None:
            raise RuntimeError("leave the StatementSet context before executing it")
        if not self._sinks:
            raise ValueError("StatementSet has no sinks to execute")
        sinks = self.sinks
        handle = self._pipeline._execute_statement_set(
            self,
            job_name or self._pipeline.name,
            sinks,
        )
        for sink in sinks:
            sink._statement_set_owner_token = None
        self._sinks.clear()
        return handle

    def explain(self, job_name: str | None = None) -> str:
        """Compile all statements together without submitting them."""

        if self._capture_scope is not None:
            raise RuntimeError("leave the StatementSet context before explaining it")
        if not self._sinks:
            raise ValueError("StatementSet has no sinks to explain")
        return self._pipeline._explain_statement_set(
            self,
            job_name or self._pipeline.name,
            self.sinks,
        )

    def __enter__(self) -> StatementSet:
        if self._capture_scope is not None:
            raise RuntimeError("StatementSet is already active")
        self._entry_size = len(self._sinks)
        self._capture_scope = self._pipeline._capture_statement_set(self)
        try:
            self._capture_scope.__enter__()
        except BaseException:
            self._capture_scope = None
            raise
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> Literal[False]:
        scope = self._capture_scope
        self._capture_scope = None
        if scope is None:
            raise RuntimeError("StatementSet is not active")
        try:
            if exc_type is not None:
                self._rollback(self._entry_size)
        finally:
            scope.__exit__(exc_type, exc_value, traceback)
        return False

    def _register(self, sink: StreamSink) -> None:
        if not isinstance(sink, StreamSink):
            raise TypeError("StatementSet accepts only StreamSink terminal operations")
        if sink.context is not self._pipeline:
            raise ValueError("all StatementSet sinks must belong to its owning Pipeline")
        if sink.node_type is NodeType.TAKE:
            raise ValueError("StatementSet accepts side-effect sinks; use collect().result() separately")
        if any(existing is sink for existing in self._sinks):
            raise ValueError("a terminal operation may be added to a StatementSet only once")
        if sink._statement_set_owner_token is not None:
            raise ValueError("a terminal operation already belongs to a StatementSet")
        if not any(pending is sink for pending in self._pipeline.sinks):
            raise ValueError("a StatementSet sink must still be pending")
        sink._statement_set_owner_token = self._owner_token
        self._sinks.append(sink)

    def _rollback(self, start: int) -> None:
        added = self._sinks[start:]
        del self._sinks[start:]
        for sink in added:
            sink._statement_set_owner_token = None
            self._pipeline._discard_sink(sink)


__all__ = ["StatementSet"]
