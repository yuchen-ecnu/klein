# SPDX-License-Identifier: Apache-2.0
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ExecutionVertexId:
    """Identity of one physical subtask."""

    job_vertex_id: int
    index: int

    def __repr__(self) -> str:
        return f"(job_vertex_id:{self.job_vertex_id}, index:{self.index})"
