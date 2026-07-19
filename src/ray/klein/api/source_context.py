# SPDX-License-Identifier: Apache-2.0
from abc import ABC, abstractmethod
from collections.abc import Iterable, Mapping
from typing import Any


class SourceContext(ABC):
    @abstractmethod
    def collect(self, data: dict[str, Any]) -> None:
        """Emit one record in source order."""

    def collect_many(self, records: Iterable[Mapping[str, Any]]) -> None:
        """Emit a source batch, falling back to ordered row emission."""
        for record in records:
            self.collect(dict(record))

    @property
    def batch_collect_supported(self) -> bool:
        """Whether ``collect_many`` is one atomic columnar runtime emission."""
        return False

    def flush(self) -> None:
        """Force buffered source output toward the next task when supported."""
        return

    def collect_durable(self, data: dict[str, Any]) -> None:
        """Emit one record and force its local transport buffers to drain."""
        self.collect(data)
        self.flush()

    def on_idle(self) -> None:
        """Signal that a source poll produced no data.

        Connectors that block-poll an external system (Kafka, etc.) should call
        this on an empty poll so downstream watermarks are not blocked and the
        runtime's time-based checkpoint trigger can still fire. A concrete
        context may implement this as an immediate idle transition."""
        return

    @abstractmethod
    def emit_watermark(self, timestamp: int) -> None:
        """Emit explicit event-time progress after all preceding source data."""

    @abstractmethod
    def mark_idle(self) -> None:
        """Exclude this source input from downstream watermark calculation."""

    @abstractmethod
    def mark_active(self, resume_watermark: int | None = None) -> None:
        """Reactivate an idle source input before it emits more data."""
