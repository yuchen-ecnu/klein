# SPDX-License-Identifier: Apache-2.0
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class VertexId:
    """Logical vertex identity. Stable across optimization."""

    job: str
    index: int

    def __str__(self) -> str:
        return f"{self.job}/{self.index}"
