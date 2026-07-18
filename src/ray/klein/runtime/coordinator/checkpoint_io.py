# SPDX-License-Identifier: Apache-2.0
"""Checkpoint metadata persistence, validation, discovery, and retention."""

import hashlib
import pickle
import re
from collections.abc import Mapping
from typing import Any

from ray.klein.runtime.execution_graph.execution_graph import ExecutionGraph
from ray.klein.runtime.execution_graph.execution_vertex_id import ExecutionVertexId
from ray.klein.state.checkpoint_file_system import CheckpointFileSystem
from ray.klein.state.checkpoint_layout import CheckpointLayout
from ray.klein.state.operator_state_checkpoint_entry import (
    OperatorStateCheckpointEntry,
)
from ray.klein.state.sink_committable_checkpoint_entry import (
    SinkCommittableCheckpointEntry,
)
from ray.klein.state.source_checkpoint_entry import SourceCheckpointEntry

_CHECKPOINT_DIRECTORY = re.compile(r"^chk-(\d+)$")
_CHECKPOINT_FORMAT_VERSION = 3


def coordinator_ack_counts(
    execution_graph: ExecutionGraph,
) -> dict[ExecutionVertexId, int]:
    """Return the required sink acknowledgements for each source task."""
    alignments = execution_graph.barrier_splits
    acknowledgements = {}
    for vertex in execution_graph.sink_execution_vertices:
        for source_vertex_id, split_count in alignments[vertex.id].items():
            acknowledgements[source_vertex_id] = acknowledgements.get(source_vertex_id, 0) + (
                0 if split_count == 0 else 1
            )
    return acknowledgements


def barrier_split_counts(
    execution_graph: ExecutionGraph,
) -> dict[ExecutionVertexId, dict[ExecutionVertexId, int]]:
    """Return cached barrier-alignment fan-in counts for every task."""
    return execution_graph.barrier_splits


def restore_checkpoint(
    checkpoint_path: str,
    storage_options: Mapping[str, Any] | None = None,
) -> tuple[int, list[SourceCheckpointEntry], int]:
    """Restore source-owned state and the barrier high-water mark.

    ``barrier_high_water`` is the largest barrier id the coordinator had
    allocated as of this snapshot; a rebuilt coordinator seeds its barrier
    generator above it so a fresh barrier id can't collide with one still in
    flight in a downstream aligner.
    """
    if not checkpoint_path:
        return 0, [], 0

    filesystem = CheckpointFileSystem(checkpoint_path, storage_options)
    metadata_revision, source_states, high_water, _operator_state = _decode_checkpoint(
        filesystem.read_bytes("_metadata")
    )
    return metadata_revision, source_states, high_water


def restore_operator_state_entries(
    checkpoint_path: str,
    storage_options: Mapping[str, Any] | None = None,
) -> dict[str, OperatorStateCheckpointEntry]:
    """Read managed-state metadata without materializing state payloads."""

    if not checkpoint_path:
        return {}
    filesystem = CheckpointFileSystem(checkpoint_path, storage_options)
    _metadata_revision, _source_states, _high_water, entries = _decode_checkpoint(filesystem.read_bytes("_metadata"))
    return {entry.task_key: entry for entry in entries}


def restore_sink_committable_entries(
    checkpoint_path: str,
    storage_options: Mapping[str, Any] | None = None,
) -> tuple[SinkCommittableCheckpointEntry, ...]:
    """Read durable, possibly not-yet-committed sink transactions."""

    if not checkpoint_path:
        return ()
    filesystem = CheckpointFileSystem(checkpoint_path, storage_options)
    data = _decode_checkpoint_mapping(filesystem.read_bytes("_metadata"))
    entries = data.get("sink_committables", ())
    _validate_sink_committables(entries)
    return entries


def read_operator_state(
    checkpoint_path: str,
    entry: OperatorStateCheckpointEntry,
    storage_options: Mapping[str, Any] | None = None,
) -> bytes:
    """Read and verify one task-local managed-state snapshot."""

    filesystem = CheckpointFileSystem(checkpoint_path, storage_options)
    payload = filesystem.read_bytes(filesystem.relative_path(entry.uri))
    if len(payload) != entry.size_bytes:
        raise ValueError(
            f"operator state size mismatch for {entry.uri}: expected {entry.size_bytes}, got {len(payload)}"
        )
    checksum = f"sha256:{hashlib.sha256(payload).hexdigest()}"
    if checksum != entry.checksum:
        raise ValueError(f"operator state checksum mismatch for {entry.uri}: expected {entry.checksum}, got {checksum}")
    return payload


def write_checkpoint(
    source_states: list[SourceCheckpointEntry],
    metadata_revision: int,
    checkpoint_directory: str,
    barrier_high_water: int = 0,
    job_id: str = "default",
    storage_options: Mapping[str, Any] | None = None,
    operator_states: Mapping[str, bytes] | None = None,
    sink_committables: tuple[SinkCommittableCheckpointEntry, ...] = (),
) -> str:
    source_state_tuple = tuple(source_states)
    _validate_source_states(source_state_tuple)
    sink_committable_tuple = tuple(sink_committables)
    _validate_sink_committables(sink_committable_tuple)
    filesystem = CheckpointFileSystem(checkpoint_directory, storage_options)
    layout = CheckpointLayout(job_id)
    metadata_path = layout.metadata_path(metadata_revision)
    state_entries = []
    for task_key, state in sorted((operator_states or {}).items()):
        if not isinstance(state, bytes):
            raise TypeError(f"operator state for {task_key!r} must be bytes")
        checksum = f"sha256:{hashlib.sha256(state).hexdigest()}"
        state_path = layout.operator_state_path(metadata_revision, task_key, checksum)
        if not filesystem.exists(state_path):
            filesystem.write_bytes(state_path, state)
        state_entries.append(
            OperatorStateCheckpointEntry(
                task_key=task_key,
                uri=filesystem.uri(state_path),
                checksum=checksum,
                size_bytes=len(state),
            )
        )
    payload = pickle.dumps(
        {
            "version": _CHECKPOINT_FORMAT_VERSION,
            "metadata_revision": metadata_revision,
            "source_states": source_state_tuple,
            "barrier_high_water": barrier_high_water,
            "operator_states": tuple(state_entries),
            "sink_committables": sink_committable_tuple,
        },
        protocol=pickle.HIGHEST_PROTOCOL,
    )
    if filesystem.exists(metadata_path):
        if filesystem.read_bytes(metadata_path) != payload:
            raise ValueError(f"checkpoint revision {metadata_revision} already has different metadata")
    else:
        # _metadata is the completion marker. For local filesystems this is
        # temp+rename; on object stores it is one final object PUT.
        filesystem.write_bytes(metadata_path, payload, atomic=True)
    filesystem.write_bytes(
        layout.latest_pointer,
        pickle.dumps(metadata_revision, protocol=pickle.HIGHEST_PROTOCOL),
        atomic=True,
    )
    return filesystem.uri(layout.checkpoint_directory(metadata_revision))


def latest_checkpoint(
    checkpoint_directory: str,
    job_id: str = "default",
    storage_options: Mapping[str, Any] | None = None,
) -> str | None:
    """Discover the newest readable completed checkpoint for a job."""

    filesystem = CheckpointFileSystem(checkpoint_directory, storage_options)
    layout = CheckpointLayout(job_id)
    candidates: list[int] = []
    if filesystem.exists(layout.latest_pointer):
        try:
            pointer_id = int(pickle.loads(filesystem.read_bytes(layout.latest_pointer)))
            if pointer_id >= 0:
                candidates.append(pointer_id)
        except (TypeError, ValueError, pickle.UnpicklingError, EOFError):
            pass
    candidates.extend(
        reversed(
            list_completed_checkpoints(
                checkpoint_directory,
                job_id,
                storage_options,
            )
        )
    )
    candidates.sort(reverse=True)

    seen: set[int] = set()
    for checkpoint_id in candidates:
        if checkpoint_id in seen:
            continue
        seen.add(checkpoint_id)
        relative_path = layout.metadata_path(checkpoint_id)
        if not filesystem.exists(relative_path):
            continue
        try:
            restored_id, _source_states, _high_water, _operator_state = _decode_checkpoint(
                filesystem.read_bytes(relative_path)
            )
        except (TypeError, ValueError, pickle.UnpicklingError, EOFError):
            continue
        if restored_id == checkpoint_id:
            return filesystem.uri(layout.checkpoint_directory(checkpoint_id))
    return None


def list_completed_checkpoints(
    checkpoint_directory: str,
    job_id: str = "default",
    storage_options: Mapping[str, Any] | None = None,
) -> tuple[int, ...]:
    filesystem = CheckpointFileSystem(checkpoint_directory, storage_options)
    layout = CheckpointLayout(job_id)
    completed: list[int] = []
    for name in filesystem.list_directories(layout.job_directory):
        match = _CHECKPOINT_DIRECTORY.fullmatch(name)
        if match is None:
            continue
        checkpoint_id = int(match.group(1))
        if filesystem.exists(layout.metadata_path(checkpoint_id)):
            completed.append(checkpoint_id)
    return tuple(sorted(completed))


def cleanup_checkpoints(
    checkpoint_directory: str,
    job_id: str = "default",
    retained_count: int = 1,
    storage_options: Mapping[str, Any] | None = None,
) -> None:
    if retained_count < 1:
        raise ValueError("retained_count must be at least 1")
    filesystem = CheckpointFileSystem(checkpoint_directory, storage_options)
    layout = CheckpointLayout(job_id)
    checkpoints = list_completed_checkpoints(
        checkpoint_directory,
        job_id,
        storage_options,
    )
    for checkpoint_id in checkpoints[:-retained_count]:
        # Deletes only chk-N. Flink's shared/ and taskowned/ areas are not
        # checkpoint-private and must survive ordinary retention cleanup.
        filesystem.delete_dir(layout.checkpoint_directory(checkpoint_id))


def _decode_checkpoint(
    payload: bytes,
) -> tuple[
    int,
    list[SourceCheckpointEntry],
    int,
    tuple[OperatorStateCheckpointEntry, ...],
]:
    data = _decode_checkpoint_mapping(payload)
    metadata_revision = data["metadata_revision"]
    source_states = data["source_states"]
    barrier_high_water = data["barrier_high_water"]
    operator_states = data["operator_states"]
    return metadata_revision, list(source_states), barrier_high_water, operator_states


def _decode_checkpoint_mapping(payload: bytes) -> dict[str, Any]:
    data = pickle.loads(payload)
    if not isinstance(data, dict):
        raise ValueError("checkpoint metadata must contain a versioned mapping")
    if data.get("version") != _CHECKPOINT_FORMAT_VERSION:
        raise ValueError(f"unsupported checkpoint format version: {data.get('version')!r}")
    required = {"metadata_revision", "source_states", "barrier_high_water", "operator_states"}
    missing = required.difference(data)
    if missing:
        raise ValueError(f"checkpoint metadata is missing fields: {sorted(missing)}")
    metadata_revision = data["metadata_revision"]
    source_states = data["source_states"]
    barrier_high_water = data["barrier_high_water"]
    operator_states = data["operator_states"]
    _validate_checkpoint_id("metadata_revision", metadata_revision)
    _validate_checkpoint_id("barrier_high_water", barrier_high_water)
    _validate_source_states(source_states)
    _validate_operator_states(operator_states)
    _validate_sink_committables(data.get("sink_committables", ()))
    return data


def _validate_checkpoint_id(name: str, value: Any) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"checkpoint {name} must be a non-negative integer")


def _validate_source_states(source_states: Any) -> None:
    if not isinstance(source_states, tuple) or not all(
        isinstance(item, SourceCheckpointEntry) for item in source_states
    ):
        raise ValueError("checkpoint source_states must be SourceCheckpointEntry values")
    task_keys = [item.task_key for item in source_states]
    if len(task_keys) != len(set(task_keys)):
        raise ValueError("checkpoint source_states must contain at most one entry per task_key")


def _validate_operator_states(operator_states: Any) -> None:
    if not isinstance(operator_states, tuple) or not all(
        isinstance(item, OperatorStateCheckpointEntry) for item in operator_states
    ):
        raise ValueError("checkpoint operator_states must be OperatorStateCheckpointEntry values")


def _validate_sink_committables(sink_committables: Any) -> None:
    if not isinstance(sink_committables, tuple) or not all(
        isinstance(item, SinkCommittableCheckpointEntry) for item in sink_committables
    ):
        raise ValueError("checkpoint sink_committables must be SinkCommittableCheckpointEntry values")
    task_checkpoints = [(item.task_key, item.checkpoint_id) for item in sink_committables]
    if len(task_checkpoints) != len(set(task_checkpoints)):
        raise ValueError("checkpoint sink_committables must contain at most one transaction per task and checkpoint")
