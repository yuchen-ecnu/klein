# SPDX-License-Identifier: Apache-2.0
from abc import ABC, abstractmethod
from typing import Any

from ray.klein.state.checkpoint_file_scope import CheckpointFileScope
from ray.klein.state.state_checkpoint_entry import StateCheckpointEntry
from ray.klein.state.state_checkpoint_manifest import StateCheckpointManifest
from ray.klein.state.state_handle import StateHandle


class CheckpointStore(ABC):
    """Durable storage boundary used for owner and cluster recovery."""

    @abstractmethod
    def write_state(
        self,
        checkpoint_id: int,
        handle: StateHandle,
        value: Any,
        *,
        scope: CheckpointFileScope = CheckpointFileScope.EXCLUSIVE,
    ) -> StateCheckpointEntry:
        """Persist one immutable state value and return its durable entry."""

    @abstractmethod
    def commit(self, manifest: StateCheckpointManifest) -> None:
        """Atomically publish a complete manifest."""

    @abstractmethod
    def latest(self, job_id: str) -> StateCheckpointManifest | None:
        """Return the latest atomically committed manifest for a job."""

    @abstractmethod
    def read_state(self, entry: StateCheckpointEntry) -> Any:
        """Read and verify one state value."""
