# SPDX-License-Identifier: Apache-2.0
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ResourceEdge:
    """Directed edge in a serialized resource plan."""

    source: int
    target: int
    strategy: str
