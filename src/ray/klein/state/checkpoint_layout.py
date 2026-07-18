# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import posixpath
from dataclasses import dataclass
from urllib.parse import quote

from ray.klein.state.checkpoint_file_scope import CheckpointFileScope
from ray.klein.state.state_handle import StateHandle


@dataclass(frozen=True, slots=True)
class CheckpointLayout:
    """Flink-compatible relative paths below a checkpoint root URI."""

    job_id: str

    def __post_init__(self) -> None:
        if not self.job_id:
            raise ValueError("job_id must not be empty")

    @property
    def job_directory(self) -> str:
        return _component(self.job_id)

    @property
    def shared_directory(self) -> str:
        return posixpath.join(self.job_directory, "shared")

    @property
    def task_owned_directory(self) -> str:
        return posixpath.join(self.job_directory, "taskowned")

    @property
    def latest_pointer(self) -> str:
        return posixpath.join(self.job_directory, "_latest")

    def checkpoint_directory(self, checkpoint_id: int) -> str:
        _validate_checkpoint_id(checkpoint_id)
        return posixpath.join(self.job_directory, f"chk-{checkpoint_id}")

    def metadata_path(self, checkpoint_id: int) -> str:
        return posixpath.join(self.checkpoint_directory(checkpoint_id), "_metadata")

    def operator_state_path(
        self,
        checkpoint_id: int,
        task_key: str,
        checksum: str,
    ) -> str:
        """Return the exclusive managed-state path for one runtime subtask."""

        _validate_checkpoint_id(checkpoint_id)
        digest = checksum.removeprefix("sha256:")
        return posixpath.join(
            self.checkpoint_directory(checkpoint_id),
            f"op-{_component(task_key)}",
            f"managed-state-{digest[:16]}.bin",
        )

    def state_path(
        self,
        checkpoint_id: int,
        handle: StateHandle,
        scope: CheckpointFileScope,
        checksum: str,
    ) -> str:
        digest = checksum.removeprefix("sha256:")
        operator_directory = posixpath.join(
            f"op-{_component(handle.partition.operator_id)}",
            f"kg-{handle.partition.key_group}",
        )
        filename = f"state-v{handle.version}-{digest[:16]}.bin"
        if scope == CheckpointFileScope.SHARED:
            base = self.shared_directory
            filename = f"sha256-{digest}.bin"
        elif scope == CheckpointFileScope.TASK_OWNED:
            base = self.task_owned_directory
        else:
            base = self.checkpoint_directory(checkpoint_id)
        return posixpath.join(base, operator_directory, filename)


def _component(value: str) -> str:
    return quote(str(value), safe="")


def _validate_checkpoint_id(checkpoint_id: int) -> None:
    if isinstance(checkpoint_id, bool) or not isinstance(checkpoint_id, int):
        raise TypeError("checkpoint_id must be an integer")
    if checkpoint_id < 0:
        raise ValueError("checkpoint_id must be non-negative")
