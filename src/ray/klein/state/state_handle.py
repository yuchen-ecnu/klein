# SPDX-License-Identifier: Apache-2.0
from dataclasses import dataclass
from typing import Any

from ray.klein.state.state_partition import StatePartition


@dataclass(frozen=True, slots=True)
class StateHandle:
    """Ephemeral handle to an immutable value in Ray's Object Store."""

    partition: StatePartition
    version: int
    object_ref: Any
    input_sequence: int
    size_bytes: int = 0

    def __post_init__(self) -> None:
        if self.version < 1:
            raise ValueError("version must be positive")
        if self.input_sequence < 0:
            raise ValueError("input_sequence must be non-negative")
        if self.size_bytes < 0:
            raise ValueError("size_bytes must be non-negative")
