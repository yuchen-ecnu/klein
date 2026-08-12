# SPDX-License-Identifier: Apache-2.0
"""Explicit user-facing owner for one Klein dataflow."""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any, ClassVar, cast

from ray.klein.api.klein_context import KleinContext
from ray.klein.config.configuration import ConfigInput

if TYPE_CHECKING:
    from ray.klein.api.job_handle import JobHandle
    from ray.klein.api.statement_set import StatementSet
    from ray.klein.api.stream_sink import StreamSink


class Pipeline(KleinContext):
    """Explicit owner for one isolated Klein dataflow.

    ``Pipeline`` defaults to strict framework configuration so misspelled keys
    fail during construction. A terminal submits itself as one job unless it
    is captured by a :class:`~ray.klein.StatementSet`. Pass
    ``strict_config=False`` only when the same mapping intentionally carries
    application-owned metadata.
    """

    _active_statement_set: ClassVar[ContextVar[StatementSet | None]] = ContextVar(
        "ray_klein_active_statement_set",
        default=None,
    )

    def __init__(
        self,
        configuration: ConfigInput = None,
        *,
        name: str | None = None,
        strict_config: bool = True,
    ) -> None:
        if name is not None and not isinstance(name, str):
            raise TypeError("name must be a string or None")
        if name is not None and not name.strip():
            raise ValueError("name must not be empty")
        super().__init__(configuration, strict_config=strict_config)
        self._name = name

    @property
    def name(self) -> str | None:
        """Default job name used by directly submitted single terminals."""

        return self._name

    def create_statement_set(self) -> StatementSet:
        """Create a declarative group that submits multiple sinks as one job."""

        from ray.klein.api.statement_set import StatementSet

        return StatementSet(self)

    @contextmanager
    def _capture_statement_set(self, statement_set: StatementSet) -> Iterator[None]:
        active = self._active_statement_set.get()
        if active is statement_set:
            yield
            return
        if active is not None:
            raise RuntimeError("a different StatementSet is already being built in this execution context")
        token = self._active_statement_set.set(statement_set)
        try:
            yield
        finally:
            self._active_statement_set.reset(token)

    def _finalize_sink(self, sink: StreamSink, *, job_name: str | None = None) -> Any:
        active = self._active_statement_set.get()
        if active is not None:
            if active.pipeline is not self:
                self._discard_sink(sink)
                raise ValueError("an active StatementSet can capture sinks only from its owning Pipeline")
            try:
                active._register(sink)
            except BaseException:
                self._discard_sink(sink)
                raise
            return sink
        if self.interactive_mode_enabled:
            return super()._finalize_sink(sink, job_name=job_name)
        return self.execute(job_name or self._name, sinks=(sink,))

    def execute(
        self,
        job_name: str | None = None,
        *,
        sinks: Sequence[StreamSink] | None = None,
    ) -> JobHandle:
        """Submit explicitly staged legacy terminals.

        Ordinary single terminals submit themselves. Sinks captured by a
        :class:`~ray.klein.StatementSet` must be submitted through that set.
        """

        selected = self.sinks if sinks is None else tuple(sinks)
        self._reject_statement_set_sinks(selected)
        return super().execute(job_name, sinks=sinks)

    def explain(
        self,
        job_name: str | None = None,
        *,
        sinks: Sequence[StreamSink] | None = None,
    ) -> str:
        """Explain explicitly staged legacy terminals.

        Use :meth:`StatementSet.explain
        <ray.klein.StatementSet.explain>` for captured sinks.
        """

        selected = self.sinks if sinks is None else tuple(sinks)
        self._reject_statement_set_sinks(selected)
        return cast(str, super().explain(job_name, sinks=sinks))

    def _execute_statement_set(
        self,
        statement_set: StatementSet,
        job_name: str | None,
        sinks: Sequence[StreamSink],
    ) -> JobHandle:
        self._validate_statement_set_sinks(statement_set, sinks)
        return super().execute(job_name, sinks=sinks)

    def _explain_statement_set(
        self,
        statement_set: StatementSet,
        job_name: str | None,
        sinks: Sequence[StreamSink],
    ) -> str:
        self._validate_statement_set_sinks(statement_set, sinks)
        return cast(str, super().explain(job_name, sinks=sinks))

    @staticmethod
    def _reject_statement_set_sinks(sinks: Sequence[StreamSink]) -> None:
        if any(getattr(sink, "_statement_set_owner_id", None) is not None for sink in sinks):
            raise RuntimeError("submit sinks captured by a StatementSet through that StatementSet")

    def _validate_statement_set_sinks(
        self,
        statement_set: StatementSet,
        sinks: Sequence[StreamSink],
    ) -> None:
        if statement_set.pipeline is not self:
            raise ValueError("StatementSet belongs to a different Pipeline")
        owner_id = id(statement_set)
        if any(sink._statement_set_owner_id != owner_id for sink in sinks):
            raise ValueError("all submitted sinks must belong to this StatementSet")
