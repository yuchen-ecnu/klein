# SPDX-License-Identifier: Apache-2.0
from dataclasses import dataclass

from ray.klein.state.checkpoint_file_scope import CheckpointFileScope
from ray.klein.state.state_partition import StatePartition


@dataclass(frozen=True, slots=True)
class StateCheckpointEntry:
    """Durable location and integrity metadata for one state partition."""

    partition: StatePartition
    version: int
    input_sequence: int
    uri: str
    checksum: str
    size_bytes: int
    scope: CheckpointFileScope = CheckpointFileScope.EXCLUSIVE

    def __post_init__(self) -> None:
        if self.version < 1:
            raise ValueError("version must be positive")
        if self.input_sequence < 0:
            raise ValueError("input_sequence must be non-negative")
        if not self.uri:
            raise ValueError("uri must not be empty")
        if not self.checksum:
            raise ValueError("checksum must not be empty")
        if self.size_bytes < 0:
            raise ValueError("size_bytes must be non-negative")
        if not isinstance(self.scope, CheckpointFileScope):
            raise TypeError("scope must be a CheckpointFileScope")
