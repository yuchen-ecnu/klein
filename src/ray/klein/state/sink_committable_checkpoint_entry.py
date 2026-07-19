# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from dataclasses import dataclass
from typing import cast

from ray.klein.api.sink_committable import SinkCommittable


@dataclass(frozen=True, slots=True)
class SinkCommittableCheckpointEntry:
    """One sink transaction owned by a task and global checkpoint."""

    task_key: str
    checkpoint_id: int
    committable: SinkCommittable

    def __post_init__(self) -> None:
        if not isinstance(self.task_key, str) or not self.task_key.strip():
            raise ValueError("sink committable task_key must be a non-empty string")
        if isinstance(self.checkpoint_id, bool) or not isinstance(self.checkpoint_id, int) or self.checkpoint_id < 0:
            raise ValueError("sink committable checkpoint_id must be a non-negative integer")
        if not isinstance(self.committable, SinkCommittable):
            raise TypeError("committable must be a SinkCommittable")
        if not isinstance(self.transaction_id, str) or not self.transaction_id.strip():
            raise ValueError("sink committable transaction_id must be a non-empty string")

    @property
    def transaction_id(self) -> str:
        return cast(str, self.committable.transaction_id)
