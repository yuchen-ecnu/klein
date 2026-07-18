# SPDX-License-Identifier: Apache-2.0
from dataclasses import dataclass

from ray.klein.runtime.graph.vertex_id import VertexId
from ray.klein.runtime.partitioning.partitioner import Partitioner


@dataclass(frozen=True, slots=True)
class EdgeSpec:
    """Specification for an edge/connection between vertices."""

    source: VertexId
    target: VertexId
    partitioner: Partitioner
