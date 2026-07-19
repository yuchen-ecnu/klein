# SPDX-License-Identifier: Apache-2.0
"""Operator-facing output port.

The public operator contract deliberately stops at lifecycle + record emission.
Task data-plane concerns such as routing, replay, actor resolution and async
delivery live in ``runtime.collector.TaskOutput`` instead of leaking into every
collector implementation.
"""

from abc import ABC, abstractmethod

from ray.klein.api.runtime_context import RuntimeContext
from ray.klein.runtime.message import Record


class Collector(ABC):
    """A lifecycle-bound sink for records emitted by an operator."""

    def __init__(self) -> None:
        self._runtime_context: RuntimeContext | None = None
        self._closed = False

    def open(self, runtime_context: RuntimeContext) -> None:
        """Bind the collector once to an operator runtime context."""
        if self._runtime_context is not None:
            raise RuntimeError(f"{type(self).__name__} is already open")
        self._runtime_context = runtime_context
        self._closed = False
        try:
            self._on_open(runtime_context)
        except BaseException:
            self._runtime_context = None
            self._closed = True
            raise

    def _on_open(self, runtime_context: RuntimeContext) -> None:
        """Subclass initialization hook."""
        del runtime_context

    def _ensure_open(self) -> RuntimeContext:
        context = self._runtime_context
        if context is None or self._closed:
            raise RuntimeError(f"{type(self).__name__} is not open")
        return context

    @abstractmethod
    def collect(self, record: Record) -> None:
        """Accept one ordered operator output."""

    def flush(self, force: bool = False) -> None:
        """Flush buffered output when supported."""
        del force

    def close(self) -> None:
        """Close once; repeated calls are harmless."""
        if self._runtime_context is None or self._closed:
            return
        try:
            self._on_close()
        finally:
            self._closed = True
            self._runtime_context = None

    def _on_close(self) -> None:  # noqa: B027 - optional lifecycle hook
        """Subclass teardown hook."""

    @property
    def records_out(self) -> int:
        """Logical rows emitted through this output port."""
        return 0

    @property
    def bytes_out(self) -> int:
        """Estimated logical payload bytes emitted through this output port."""
        return 0
