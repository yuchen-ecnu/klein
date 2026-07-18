# SPDX-License-Identifier: Apache-2.0
"""Portable lineage data types and emitter protocol."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True, slots=True)
class DatasetInfo:
    namespace: str
    name: str
    bootstrap_servers: str | None = None


class LineageEmitter(Protocol):
    """Application-provided adapter for OpenLineage or another event backend."""

    def emit(
        self,
        event_type: str,
        job_name: str,
        _run_id: str,
        inputs: Sequence[DatasetInfo],
        outputs: Sequence[DatasetInfo],
        *,
        error: str | None = None,
    ) -> None: ...
