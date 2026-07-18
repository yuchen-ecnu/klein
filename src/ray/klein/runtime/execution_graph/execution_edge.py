# SPDX-License-Identifier: Apache-2.0
from dataclasses import dataclass

from ray.klein.runtime.execution_graph.execution_vertex import ExecutionVertex


@dataclass(frozen=True, slots=True, eq=False)
class ExecutionEdge:
    """Physical edge between two execution vertices."""

    source: ExecutionVertex
    target: ExecutionVertex
