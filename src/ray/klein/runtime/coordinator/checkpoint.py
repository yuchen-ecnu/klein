# SPDX-License-Identifier: Apache-2.0
import time
from dataclasses import dataclass, field

from ray.klein.api._terminal_status_enum import _TerminalStatusEnum
from ray.klein.runtime.execution_graph.execution_vertex_id import ExecutionVertexId


class CheckpointStatus(_TerminalStatusEnum):
    """Lifecycle state of an in-flight or completed checkpoint."""

    CREATED = "CREATED", False
    IN_PROGRESS = "IN_PROGRESS", False
    NOTIFYING = "NOTIFYING", False
    COMPLETED = "COMPLETED", True
    FAILED = "FAILED", True


@dataclass(slots=True)
class Checkpoint:
    """Mutable acknowledgement state for one checkpoint barrier."""

    barrier_id: int
    required_acknowledgements: int
    trigger_sources: tuple[ExecutionVertexId, ...]
    coordinated: bool = False
    domain_id: str | None = None
    required_committers: tuple[ExecutionVertexId, ...] = ()
    acknowledgements: int = 0
    reason: str = ""
    triggered_at_ms: int | None = None
    last_acknowledged_at_ms: int | None = None
    completed_at_ms: int | None = None
    status: CheckpointStatus = CheckpointStatus.CREATED
    _acknowledged_by: set[ExecutionVertexId] = field(default_factory=set, repr=False)
    operator_metrics: dict[ExecutionVertexId, dict[str, int | float]] = field(default_factory=dict, repr=False)

    def __post_init__(self) -> None:
        self.trigger_sources = tuple(self.trigger_sources)
        self.required_committers = tuple(self.required_committers)

    def mark_complete(self) -> None:
        now_ms = int(time.time() * 1000)
        self.last_acknowledged_at_ms = now_ms
        self.completed_at_ms = now_ms
        self.status = CheckpointStatus.COMPLETED

    def mark_failed(self, reason: str = "") -> None:
        now_ms = int(time.time() * 1000)
        self.last_acknowledged_at_ms = now_ms
        self.completed_at_ms = now_ms
        self.status = CheckpointStatus.FAILED
        self.reason = reason

    def mark_in_progress(self) -> None:
        self.triggered_at_ms = int(time.time() * 1000)
        self.status = CheckpointStatus.IN_PROGRESS

    def mark_notifying(self) -> None:
        self.status = CheckpointStatus.NOTIFYING

    def acknowledge(self, committer: ExecutionVertexId) -> bool:
        """Record one idempotent acknowledgement and report finalization readiness."""
        if self.required_committers and committer not in self.required_committers:
            return False
        # Idempotent per committer: a committer that acks twice (e.g. a batched
        # notify retried after a transient failure) is counted once.
        if committer in self._acknowledged_by:
            return self.status == CheckpointStatus.NOTIFYING
        self._acknowledged_by.add(committer)
        self.last_acknowledged_at_ms = int(time.time() * 1000)
        self.acknowledgements += 1
        # Guard required_acknowledgements > 0: a 0/negative target would otherwise flip to
        # NOTIFYING on the first ack (or even with zero acks), completing a
        # checkpoint that never actually aligned. Registration already rejects
        # non-positive counts, so this is defense in depth.
        if self.required_acknowledgements > 0 and self.acknowledgements >= self.required_acknowledgements:
            self.mark_notifying()
        return self.status == CheckpointStatus.NOTIFYING

    def record_operator_metrics(
        self,
        vertex_id: ExecutionVertexId,
        metrics: dict[str, int | float],
    ) -> None:
        """Record the latest idempotent metrics report from one subtask."""

        self.operator_metrics[vertex_id] = dict(metrics)
