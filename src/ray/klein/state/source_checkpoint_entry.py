# SPDX-License-Identifier: Apache-2.0
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class SourceCheckpointEntry:
    """One source subtask's opaque state at a completed checkpoint.

    ``task_key`` scopes the state to a physical source subtask. The runtime
    deliberately does not inspect ``state``: partition offsets, split state,
    and connector-specific versioning belong to the source implementation.
    """

    task_key: str
    checkpoint_id: int
    state: Any

    def __post_init__(self) -> None:
        if not self.task_key:
            raise ValueError("task_key must not be empty")
        if isinstance(self.checkpoint_id, bool) or not isinstance(self.checkpoint_id, int):
            raise TypeError("checkpoint_id must be an integer")
        if self.checkpoint_id < 0:
            raise ValueError("checkpoint_id must be non-negative")
