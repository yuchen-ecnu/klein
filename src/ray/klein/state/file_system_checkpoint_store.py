# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import hashlib
import json
import pickle
import re
from collections.abc import Mapping
from typing import Any

from ray.klein.state.checkpoint_file_scope import CheckpointFileScope
from ray.klein.state.checkpoint_file_system import CheckpointFileSystem
from ray.klein.state.checkpoint_layout import CheckpointLayout
from ray.klein.state.checkpoint_store import CheckpointStore
from ray.klein.state.state_checkpoint_entry import StateCheckpointEntry
from ray.klein.state.state_checkpoint_manifest import StateCheckpointManifest
from ray.klein.state.state_handle import StateHandle

_CHECKPOINT_DIRECTORY = re.compile(r"^chk-(\d+)$")


class FileSystemCheckpointStore(CheckpointStore):
    """Durable checkpoint store for local filesystems and object stores.

    State objects are written before ``chk-N/_metadata``. The metadata object
    is therefore the checkpoint's commit marker. ``_latest`` is only a lookup
    optimization; recovery scans committed ``chk-N`` directories if it is
    absent, stale, or corrupt.
    """

    def __init__(
        self,
        root_uri: str,
        storage_options: Mapping[str, Any] | None = None,
    ) -> None:
        self._filesystem = CheckpointFileSystem(root_uri, storage_options)

    def write_state(
        self,
        checkpoint_id: int,
        handle: StateHandle,
        value: Any,
        *,
        scope: CheckpointFileScope = CheckpointFileScope.EXCLUSIVE,
    ) -> StateCheckpointEntry:
        if not isinstance(scope, CheckpointFileScope):
            raise TypeError("scope must be a CheckpointFileScope")
        payload = pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
        checksum = f"sha256:{hashlib.sha256(payload).hexdigest()}"
        layout = CheckpointLayout(handle.partition.job_id)
        relative_path = layout.state_path(checkpoint_id, handle, scope, checksum)
        if not self._filesystem.exists(relative_path):
            self._filesystem.write_bytes(relative_path, payload)
        return StateCheckpointEntry(
            partition=handle.partition,
            version=handle.version,
            input_sequence=handle.input_sequence,
            uri=self._filesystem.uri(relative_path),
            checksum=checksum,
            size_bytes=len(payload),
            scope=scope,
        )

    def commit(self, manifest: StateCheckpointManifest) -> None:
        for entry in manifest.entries:
            relative_path = self._filesystem.relative_path(entry.uri)
            if not self._filesystem.exists(relative_path):
                raise FileNotFoundError(f"checkpoint state object does not exist: {entry.uri}")

        layout = CheckpointLayout(manifest.job_id)
        metadata_path = layout.metadata_path(manifest.checkpoint_id)
        metadata = _encode_manifest(manifest)
        if self._filesystem.exists(metadata_path):
            existing = self._filesystem.read_bytes(metadata_path)
            if existing != metadata:
                raise ValueError(f"checkpoint {manifest.checkpoint_id} is already committed with different metadata")
        else:
            # The last state-changing write for a checkpoint. A completed
            # checkpoint is discoverable if and only if this object exists.
            self._filesystem.write_bytes(metadata_path, metadata, atomic=True)
        self._filesystem.write_bytes(
            layout.latest_pointer,
            _encode_latest(manifest.checkpoint_id),
            atomic=True,
        )

    def latest(self, job_id: str) -> StateCheckpointManifest | None:
        layout = CheckpointLayout(job_id)
        candidate_ids: list[int] = []
        if self._filesystem.exists(layout.latest_pointer):
            try:
                pointer = json.loads(self._filesystem.read_bytes(layout.latest_pointer))
                if pointer.get("format_version") != 1:
                    raise ValueError("unsupported latest-pointer format")
                pointer_id = int(pointer["checkpoint_id"])
                if pointer_id >= 0:
                    candidate_ids.append(pointer_id)
            except (KeyError, TypeError, ValueError, json.JSONDecodeError, UnicodeDecodeError):
                pass
        candidate_ids.extend(reversed(self.list_completed_checkpoints(job_id)))
        candidate_ids.sort(reverse=True)

        seen: set[int] = set()
        for checkpoint_id in candidate_ids:
            if checkpoint_id in seen:
                continue
            seen.add(checkpoint_id)
            manifest = self._read_manifest(layout, checkpoint_id)
            if manifest is not None:
                return manifest
        return None

    def read_state(self, entry: StateCheckpointEntry) -> Any:
        payload = self._filesystem.read_bytes(self._filesystem.relative_path(entry.uri))
        if len(payload) != entry.size_bytes:
            raise ValueError(
                f"checkpoint state size mismatch for {entry.uri}: expected {entry.size_bytes}, got {len(payload)}"
            )
        checksum = f"sha256:{hashlib.sha256(payload).hexdigest()}"
        if checksum != entry.checksum:
            raise ValueError(
                f"checkpoint state checksum mismatch for {entry.uri}: expected {entry.checksum}, got {checksum}"
            )
        return pickle.loads(payload)

    def list_completed_checkpoints(self, job_id: str) -> tuple[int, ...]:
        layout = CheckpointLayout(job_id)
        completed: list[int] = []
        for name in self._filesystem.list_directories(layout.job_directory):
            match = _CHECKPOINT_DIRECTORY.fullmatch(name)
            if match is None:
                continue
            checkpoint_id = int(match.group(1))
            if self._filesystem.exists(layout.metadata_path(checkpoint_id)):
                completed.append(checkpoint_id)
        return tuple(sorted(completed))

    def delete_checkpoint(self, job_id: str, checkpoint_id: int) -> None:
        """Delete exclusive checkpoint data, retaining shared/task-owned state."""

        layout = CheckpointLayout(job_id)
        self._filesystem.delete_dir(layout.checkpoint_directory(checkpoint_id))

    def cleanup_checkpoints(self, job_id: str, retained_count: int = 1) -> None:
        """Retain the newest completed checkpoints and their exclusive state."""

        if retained_count < 1:
            raise ValueError("retained_count must be at least 1")
        checkpoint_ids = self.list_completed_checkpoints(job_id)
        for checkpoint_id in checkpoint_ids[:-retained_count]:
            self.delete_checkpoint(job_id, checkpoint_id)

    def _read_manifest(
        self,
        layout: CheckpointLayout,
        checkpoint_id: int,
    ) -> StateCheckpointManifest | None:
        metadata_path = layout.metadata_path(checkpoint_id)
        if not self._filesystem.exists(metadata_path):
            return None
        try:
            raw = json.loads(self._filesystem.read_bytes(metadata_path))
            if raw.get("format_version") != 1:
                raise ValueError("unsupported checkpoint-manifest format")
            manifest = StateCheckpointManifest.from_dict(raw["manifest"])
        except (KeyError, TypeError, ValueError, json.JSONDecodeError, UnicodeDecodeError):
            return None
        if manifest.job_id != layout.job_id or manifest.checkpoint_id != checkpoint_id:
            return None
        return manifest


def _encode_manifest(manifest: StateCheckpointManifest) -> bytes:
    value = {"format_version": 1, "manifest": manifest.to_dict()}
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _encode_latest(checkpoint_id: int) -> bytes:
    value = {"format_version": 1, "checkpoint_id": checkpoint_id}
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
