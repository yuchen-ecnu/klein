# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import re
import time
from collections.abc import Callable, Mapping, Sequence
from typing import Any
from uuid import uuid4

from ray.klein.api.runtime_context import RuntimeContext
from ray.klein.api.sink_committable import SinkCommittable
from ray.klein.api.two_phase_commit_sink_function import TwoPhaseCommitSinkFunction
from ray.klein.integrations.filesystem._file_part import FilePart
from ray.klein.integrations.filesystem._part_file_writer import PartFileWriter
from ray.klein.integrations.filesystem.file_sink_committable import FileSinkCommittable
from ray.klein.state.checkpoint_file_system import CheckpointFileSystem

_FORMATS = frozenset({"csv", "json", "parquet", "text"})
_EXTENSIONS = {"csv": "csv", "json": "json", "parquet": "parquet", "text": "txt"}


class StreamingFileSink(TwoPhaseCommitSinkFunction):
    """Checkpoint-transactional file sink for local and object filesystems."""

    def __init__(
        self,
        path: str,
        data_format: str,
        *,
        columns: Sequence[str] | None = None,
        storage_options: dict[str, Any] | None = None,
        filename_prefix: str = "part",
        max_rows_per_file: int | None = None,
        max_bytes_per_file: int | None = None,
        rollover_interval_seconds: float | None = None,
        inactivity_interval_seconds: float | None = None,
    ) -> None:
        if not isinstance(path, str) or not path.strip():
            raise ValueError("file sink path must be a non-empty string")
        if data_format not in _FORMATS:
            raise ValueError(f"file sink format must be one of {sorted(_FORMATS)}, got {data_format!r}")
        if not isinstance(filename_prefix, str) or not filename_prefix.strip():
            raise ValueError("filename_prefix must be a non-empty string")
        _validate_optional_positive(max_rows_per_file, "max_rows_per_file")
        _validate_optional_positive(max_bytes_per_file, "max_bytes_per_file")
        _validate_optional_positive(rollover_interval_seconds, "rollover_interval_seconds")
        _validate_optional_positive(inactivity_interval_seconds, "inactivity_interval_seconds")
        self._path = path
        self._format = data_format
        self._columns = tuple(columns) if columns else None
        self._storage_options = dict(storage_options) if storage_options else None
        self._filename_prefix = filename_prefix.strip()
        self._max_rows_per_file = max_rows_per_file
        self._max_bytes_per_file = max_bytes_per_file
        self._rollover_interval_seconds = rollover_interval_seconds
        self._inactivity_interval_seconds = inactivity_interval_seconds
        self._filesystem: CheckpointFileSystem | None = None
        self._writer: PartFileWriter | None = None
        self._writer_path: str | None = None
        self._writer_started_at = 0.0
        self._last_write_at = 0.0
        self._writer_rows = 0
        self._task_index = 0
        self._job_id = "default"
        self._attempt_id = ""
        self._part_sequence = 0
        self._pending_parts: list[FilePart] = []
        self._parquet_schema = None

    def open(self, runtime_context: RuntimeContext) -> None:
        self._filesystem = CheckpointFileSystem(self._path, self._storage_options)
        self._task_index = runtime_context.task_index
        self._job_id = _safe_component(runtime_context.job_id)
        self._attempt_id = uuid4().hex

    def write(self, value: dict[str, Any]) -> None:
        if not isinstance(value, Mapping):
            raise TypeError("file sink records must be mappings")
        now = time.monotonic()
        if self._should_roll_before_write(now):
            self._roll_part()
        writer = self._ensure_writer(now)
        writer.write(value)
        self._columns = writer.columns or self._columns
        self._parquet_schema = writer.parquet_schema or self._parquet_schema
        self._writer_rows += 1
        self._last_write_at = now
        if self._should_roll_after_write():
            self._roll_part()

    def flush(self) -> None:
        if self._writer is not None:
            self._writer.flush()

    def prepare_commit(self, checkpoint_id: int) -> SinkCommittable | None:
        self._roll_part()
        if not self._pending_parts:
            return None
        parts = tuple(self._pending_parts)
        self._pending_parts.clear()
        transaction_id = f"{self._job_id}-{self._task_index}-{self._attempt_id}-chk-{checkpoint_id}"
        return FileSinkCommittable(
            root_uri=self._path,
            storage_options=self._storage_options,
            parts=parts,
            _transaction_id=transaction_id,
        )

    def abort_current_transaction(self) -> None:
        filesystem = self._filesystem
        if filesystem is None:
            return
        first_error: Exception | None = None
        if self._writer is not None:
            first_error = _capture_cleanup_error(first_error, self._writer.close)
            self._writer = None
        if self._writer_path is not None:
            first_error = _capture_cleanup_error(first_error, filesystem.delete_file, self._writer_path)
            self._writer_path = None
        for part in self._pending_parts:
            first_error = _capture_cleanup_error(first_error, filesystem.delete_file, part.pending_path)
        self._pending_parts.clear()
        if first_error is not None:
            raise first_error.with_traceback(first_error.__traceback__)

    def close(self) -> None:
        self.abort_current_transaction()
        self._filesystem = None

    def _ensure_writer(self, now: float) -> PartFileWriter:
        if self._writer is not None:
            return self._writer
        filesystem = self._require_filesystem()
        part_name = self._part_name(self._part_sequence)
        staging = self._staging_directory()
        self._writer_path = f"{staging}/.{part_name}.inprogress"
        self._writer = PartFileWriter(
            filesystem,
            self._writer_path,
            self._format,
            columns=self._columns,
            parquet_schema=self._parquet_schema,
        )
        self._writer_started_at = now
        self._last_write_at = now
        self._writer_rows = 0
        return self._writer

    def _roll_part(self) -> None:
        writer = self._writer
        if writer is None:
            return
        writer.close()
        self._columns = writer.columns or self._columns
        self._parquet_schema = writer.parquet_schema or self._parquet_schema
        part_name = self._part_name(self._part_sequence)
        pending_path = f"{self._staging_directory()}/.{part_name}.pending"
        self._require_filesystem().move_file(self._writer_path, pending_path)
        self._pending_parts.append(FilePart(pending_path, part_name))
        self._part_sequence += 1
        self._writer = None
        self._writer_path = None
        self._writer_rows = 0

    def _should_roll_before_write(self, now: float) -> bool:
        if self._writer is None:
            return False
        if (
            self._rollover_interval_seconds is not None
            and now - self._writer_started_at >= self._rollover_interval_seconds
        ):
            return True
        return (
            self._inactivity_interval_seconds is not None
            and now - self._last_write_at >= self._inactivity_interval_seconds
        )

    def _should_roll_after_write(self) -> bool:
        if self._max_rows_per_file is not None and self._writer_rows >= self._max_rows_per_file:
            return True
        return (
            self._max_bytes_per_file is not None
            and self._writer is not None
            and self._writer.size_bytes >= self._max_bytes_per_file
        )

    def _part_name(self, sequence: int) -> str:
        extension = _EXTENSIONS[self._format]
        return f"{self._filename_prefix}-{self._task_index:05d}-{self._attempt_id}-{sequence:05d}.{extension}"

    def _staging_directory(self) -> str:
        return f".klein-staging/{self._job_id}/{self._task_index:05d}-{self._attempt_id}"

    def _require_filesystem(self) -> CheckpointFileSystem:
        if self._filesystem is None:
            raise RuntimeError("file sink must be opened before use")
        return self._filesystem


def _safe_component(value: str) -> str:
    sanitized = re.sub(r"[^A-Za-z0-9_.-]+", "-", value).strip("-.")
    return sanitized or "default"


def _validate_optional_positive(value: int | float | None, name: str) -> None:
    if value is None:
        return
    if isinstance(value, bool) or not isinstance(value, int | float) or value <= 0:
        raise ValueError(f"{name} must be greater than zero or None")


def _capture_cleanup_error(
    first_error: Exception | None,
    cleanup: Callable[..., None],
    *args: Any,
) -> Exception | None:
    try:
        cleanup(*args)
    except Exception as error:
        return first_error or error
    return first_error
