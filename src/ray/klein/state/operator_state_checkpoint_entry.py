# SPDX-License-Identifier: Apache-2.0
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class OperatorStateCheckpointEntry:
    """Durable location of one task-local managed-state snapshot."""

    task_key: str
    uri: str
    checksum: str
    size_bytes: int

    def __post_init__(self) -> None:
        if not self.task_key:
            raise ValueError("task_key must not be empty")
        if not self.uri:
            raise ValueError("uri must not be empty")
        if not self.checksum:
            raise ValueError("checksum must not be empty")
        if self.size_bytes < 0:
            raise ValueError("size_bytes must be non-negative")
