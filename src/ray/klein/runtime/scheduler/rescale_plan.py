# SPDX-License-Identifier: Apache-2.0
"""Typed plan and phase model for one local operator rescale."""

from dataclasses import dataclass
from enum import IntEnum

from ray.klein.runtime.execution_graph.execution_graph import ExecutionGraph
from ray.klein.runtime.execution_graph.execution_job_vertex import ExecutionJobVertex
from ray.klein.runtime.execution_graph.execution_vertex import ExecutionVertex


@dataclass(frozen=True, slots=True)
class RescaleDelta:
    """Physical actor identity sets for one target-parallelism change."""

    retained: tuple[ExecutionVertex, ...]
    added: tuple[ExecutionVertex, ...]
    removed: tuple[ExecutionVertex, ...]

    @classmethod
    def between(cls, old_target: ExecutionJobVertex, new_target: ExecutionJobVertex) -> "RescaleDelta":
        old_indices = set(old_target.execution_vertices)
        new_indices = set(new_target.execution_vertices)
        return cls(
            retained=tuple(new_target.execution_vertex(index) for index in sorted(old_indices & new_indices)),
            added=tuple(new_target.execution_vertex(index) for index in sorted(new_indices - old_indices)),
            removed=tuple(old_target.execution_vertex(index) for index in sorted(old_indices - new_indices)),
        )


@dataclass(frozen=True, slots=True)
class RescalePlan:
    """A scheduler-owned topology change with no caller-controlled graph."""

    operation_id: str
    target_id: int
    old_graph: ExecutionGraph
    new_graph: ExecutionGraph
    old_target: ExecutionJobVertex
    new_target: ExecutionJobVertex
    delta: RescaleDelta

    @classmethod
    def build(
        cls,
        current_graph: ExecutionGraph,
        target_id: int,
        parallelism: int,
        operation_id: str,
    ) -> "RescalePlan":
        if not isinstance(operation_id, str) or not operation_id:
            raise ValueError("rescale operation_id must not be empty")
        new_graph = current_graph.resize_operator(target_id, parallelism)
        if new_graph is current_graph:
            raise ValueError("rescale target parallelism must differ from the current parallelism")
        old_target = current_graph.job_vertex(target_id)
        new_target = new_graph.job_vertex(target_id)
        return cls(
            operation_id=operation_id,
            target_id=target_id,
            old_graph=current_graph,
            new_graph=new_graph,
            old_target=old_target,
            new_target=new_target,
            delta=RescaleDelta.between(old_target, new_target),
        )


class RescalePhase(IntEnum):
    """Ordered compensation boundary; each value means that phase was attempted."""

    INITIAL = 0
    CANDIDATES = 1
    CHECKPOINT_GATE = 2
    PARTICIPANTS = 3
    RUNTIMES = 4
    ROUTES = 5
    COORDINATOR = 6
    COMMITTED = 7
    RELEASED = 8


@dataclass(slots=True)
class RescaleTransaction:
    """Mutable transaction cursor with only valid, ordered states."""

    plan: RescalePlan
    phase: RescalePhase = RescalePhase.INITIAL

    def enter(self, phase: RescalePhase) -> None:
        if phase <= self.phase:
            raise RuntimeError(f"rescale phase cannot move backward from {self.phase.name} to {phase.name}")
        expected = RescalePhase(self.phase.value + 1)
        if phase is not expected:
            raise RuntimeError(f"rescale phase cannot skip from {self.phase.name} to {phase.name}")
        self.phase = phase

    def attempted(self, phase: RescalePhase) -> bool:
        return self.phase >= phase

    @property
    def committed(self) -> bool:
        return self.attempted(RescalePhase.COMMITTED)
