# SPDX-License-Identifier: Apache-2.0
"""Validated physical-vertex subset selection shared by scheduler mechanics."""

from collections.abc import Iterable

from ray.klein.runtime.execution_graph.execution_job_vertex import ExecutionJobVertex
from ray.klein.runtime.execution_graph.execution_vertex import ExecutionVertex


def select_vertices(
    job_vertex: ExecutionJobVertex,
    vertices: Iterable[ExecutionVertex] | None,
) -> tuple[ExecutionVertex, ...]:
    """Return a validated, duplicate-free subset owned by ``job_vertex``."""

    if vertices is None:
        # Preserve the whole-job-vertex path and lightweight scheduler test
        # doubles, which expose the same execution_vertices mapping contract.
        return tuple(job_vertex.execution_vertices.values())
    selected = tuple(vertices)
    seen: set[int] = set()
    for vertex in selected:
        if not isinstance(vertex, ExecutionVertex):
            raise TypeError("vertex subsets must contain ExecutionVertex values")
        if job_vertex.execution_vertices.get(vertex.index) is not vertex:
            raise ValueError(f"ExecutionVertex '{vertex}' does not belong to operator {job_vertex.name}")
        if vertex.index in seen:
            raise ValueError(f"duplicate ExecutionVertex index {vertex.index}")
        seen.add(vertex.index)
    return selected
