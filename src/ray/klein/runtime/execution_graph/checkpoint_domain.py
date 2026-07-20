# SPDX-License-Identifier: Apache-2.0
"""Immutable checkpoint-coordination domains in the physical execution graph."""

from dataclasses import dataclass

from ray.klein.runtime.execution_graph.execution_vertex_id import ExecutionVertexId


@dataclass(frozen=True, slots=True)
class CheckpointDomain:
    """One weakly connected component of the physical execution graph.

    Checkpoints only need to coordinate sources and sinks that can exchange data
    through a shared physical component. IDs are stable for an unchanged member
    set; a rescale that changes physical membership therefore creates a new ID.
    """

    id: str
    vertex_ids: tuple[ExecutionVertexId, ...]
    source_vertex_ids: tuple[ExecutionVertexId, ...]
    sink_vertex_ids: tuple[ExecutionVertexId, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.id, str) or not self.id:
            raise ValueError("checkpoint domain id must not be empty")
        if not self.vertex_ids:
            raise ValueError("checkpoint domain must contain at least one execution vertex")
        members = frozenset(self.vertex_ids)
        if len(members) != len(self.vertex_ids):
            raise ValueError("checkpoint domain execution vertices must be unique")
        if not frozenset(self.source_vertex_ids).issubset(members):
            raise ValueError("checkpoint domain sources must be domain members")
        if not frozenset(self.sink_vertex_ids).issubset(members):
            raise ValueError("checkpoint domain sinks must be domain members")
