# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ray.klein.api.sink_committable import SinkCommittable
from ray.klein.integrations.filesystem._file_part import FilePart
from ray.klein.state.checkpoint_file_system import CheckpointFileSystem


@dataclass(frozen=True, slots=True)
class FileSinkCommittable(SinkCommittable):
    """Idempotent second-phase publication of prepared part files."""

    root_uri: str
    storage_options: dict[str, Any] | None
    parts: tuple[FilePart, ...]
    _transaction_id: str

    @property
    def transaction_id(self) -> str:
        return self._transaction_id

    def commit(self) -> None:
        filesystem = CheckpointFileSystem(self.root_uri, self.storage_options)
        for part in self.parts:
            filesystem.move_file(part.pending_path, part.final_path)

    def abort(self) -> None:
        filesystem = CheckpointFileSystem(self.root_uri, self.storage_options)
        first_error: Exception | None = None
        for part in self.parts:
            try:
                filesystem.delete_file(part.pending_path)
            except Exception as error:
                if first_error is None:
                    first_error = error
        if first_error is not None:
            raise first_error.with_traceback(first_error.__traceback__)
