# SPDX-License-Identifier: Apache-2.0
from dataclasses import dataclass

from ray.klein.runtime.graph.vertex_id import VertexId


@dataclass(frozen=True, slots=True)
class SubtaskId:
    """Physical subtask identity — the single source of an actor's name.

    failover re-resolves a downstream/coordinator handle by this name after a
    restart, so it must be deterministic and collision-free.
    """

    vertex: VertexId
    index: int  # 0 .. parallelism-1

    @property
    def actor_name(self) -> str:
        return f"{self.vertex}#{self.index}"

    def __str__(self) -> str:
        return self.actor_name
