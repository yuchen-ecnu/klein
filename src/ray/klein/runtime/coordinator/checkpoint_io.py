# SPDX-License-Identifier: Apache-2.0
"""Checkpoint metadata persistence, validation, discovery, and retention."""

import base64
import binascii
import hashlib
import json
import pickle
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, cast

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
_CHECKPOINT_FORMAT_VERSION = 4
_CHECKPOINT_METADATA_MAGIC = b"KLEIN-CHECKPOINT-METADATA-V4\n"
_LATEST_POINTER_FORMAT_VERSION = 1
_SHA256 = re.compile(r"^sha256:[0-9a-f]{64}$")


@dataclass(frozen=True, slots=True)
class _CheckpointEnvelope:
    metadata_revision: int
    barrier_high_water: int
    source_payloads: tuple[tuple[str, int, bytes], ...]
    operator_states: tuple[OperatorStateCheckpointEntry, ...]
    sink_payloads: tuple[tuple[str, int, str, bytes], ...]


def coordinator_ack_counts(
    execution_graph: ExecutionGraph,
) -> dict[ExecutionVertexId, int]:
    """Return the required sink acknowledgements for each source task."""
    alignments = execution_graph.barrier_splits
    acknowledgements: dict[ExecutionVertexId, int] = {}
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
    return cast(dict[ExecutionVertexId, dict[ExecutionVertexId, int]], execution_graph.barrier_splits)


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
    envelope = _decode_checkpoint_envelope(filesystem.read_bytes("_metadata"))
    return {entry.task_key: entry for entry in envelope.operator_states}


def restore_sink_committable_entries(
    checkpoint_path: str,
    storage_options: Mapping[str, Any] | None = None,
) -> tuple[SinkCommittableCheckpointEntry, ...]:
    """Read durable, possibly not-yet-committed sink transactions."""

    if not checkpoint_path:
        return ()
    filesystem = CheckpointFileSystem(checkpoint_path, storage_options)
    envelope = _decode_checkpoint_envelope(filesystem.read_bytes("_metadata"))
    entries = _load_sink_committables(envelope.sink_payloads)
    _validate_sink_committables(entries)
    return entries


def read_operator_state(
    checkpoint_path: str,
    entry: OperatorStateCheckpointEntry,
    storage_options: Mapping[str, Any] | None = None,
) -> bytes:
    """Read and verify one task-local managed-state snapshot."""

    filesystem = CheckpointFileSystem(checkpoint_path, storage_options)
    payload = cast(bytes, filesystem.read_bytes(filesystem.relative_path(entry.uri)))
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
    _validate_checkpoint_id("metadata_revision", metadata_revision)
    _validate_checkpoint_id("barrier_high_water", barrier_high_water)
    source_state_tuple = tuple(source_states)
    _validate_source_states(source_state_tuple)
    sink_committable_tuple = tuple(sink_committables)
    _validate_sink_committables(sink_committable_tuple)
    filesystem = CheckpointFileSystem(checkpoint_directory, storage_options)
    layout = CheckpointLayout(job_id)
    metadata_path = layout.metadata_path(metadata_revision)
    state_entries = []
    operator_state_items = []
    for task_key, state in (operator_states or {}).items():
        if not isinstance(task_key, str) or not task_key.strip():
            raise ValueError("operator state task keys must be non-empty strings")
        if not isinstance(state, bytes):
            raise TypeError(f"operator state for {task_key!r} must be bytes")
        operator_state_items.append((task_key, state))
    for task_key, state in sorted(operator_state_items):
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
    payload = _encode_checkpoint_mapping(
        {
            "version": _CHECKPOINT_FORMAT_VERSION,
            "metadata_revision": metadata_revision,
            "source_states": source_state_tuple,
            "barrier_high_water": barrier_high_water,
            "operator_states": tuple(state_entries),
            "sink_committables": sink_committable_tuple,
        }
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
        json.dumps(
            {
                "version": _LATEST_POINTER_FORMAT_VERSION,
                "checkpoint_id": metadata_revision,
            },
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8"),
        atomic=True,
    )
    return cast(str, filesystem.uri(layout.checkpoint_directory(metadata_revision)))


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
            pointer = _decode_json_mapping(filesystem.read_bytes(layout.latest_pointer), context="latest pointer")
            _require_fields(pointer, {"version", "checkpoint_id"}, context="latest pointer")
            _require_format_version(
                pointer.get("version"),
                expected=_LATEST_POINTER_FORMAT_VERSION,
                context="latest pointer",
            )
            pointer_id = _validate_checkpoint_id("latest pointer checkpoint_id", pointer.get("checkpoint_id"))
            candidates.append(pointer_id)
        except (TypeError, ValueError, UnicodeDecodeError, json.JSONDecodeError):
            pass
    completed_checkpoints = list_completed_checkpoints(
        checkpoint_directory,
        job_id,
        storage_options,
    )
    candidates.extend(reversed(completed_checkpoints))
    candidates.sort(reverse=True)

    seen: set[int] = set()
    metadata_failures: list[str] = []
    metadata_candidates = 0
    for checkpoint_id in candidates:
        if checkpoint_id in seen:
            continue
        seen.add(checkpoint_id)
        relative_path = layout.metadata_path(checkpoint_id)
        if not filesystem.exists(relative_path):
            continue
        metadata_candidates += 1
        try:
            restored_id = _decode_checkpoint_envelope(filesystem.read_bytes(relative_path)).metadata_revision
        except (TypeError, ValueError, UnicodeDecodeError, json.JSONDecodeError) as error:
            metadata_failures.append(f"chk-{checkpoint_id}: {error}")
            continue
        if restored_id == checkpoint_id:
            return cast(str, filesystem.uri(layout.checkpoint_directory(checkpoint_id)))
        metadata_failures.append(
            f"chk-{checkpoint_id}: metadata revision {restored_id} does not match its checkpoint directory"
        )
    if completed_checkpoints or metadata_candidates:
        details = (
            "; ".join(metadata_failures) if metadata_failures else "checkpoint metadata disappeared during discovery"
        )
        raise ValueError(f"completed checkpoints exist but none has readable v4 metadata: {details}")
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
    envelope = _decode_checkpoint_envelope(payload)
    source_states = _load_source_states(envelope.source_payloads)
    _validate_source_states(source_states)
    return (
        envelope.metadata_revision,
        list(source_states),
        envelope.barrier_high_water,
        envelope.operator_states,
    )


def _decode_checkpoint_envelope(payload: bytes) -> _CheckpointEnvelope:
    if not isinstance(payload, bytes):
        raise TypeError("checkpoint metadata must be bytes")
    if not payload.startswith(_CHECKPOINT_METADATA_MAGIC):
        raise ValueError(
            "checkpoint metadata is missing the safe v4 prefix; legacy pickle metadata is not loaded automatically"
        )
    encoded = _decode_json_mapping(
        payload[len(_CHECKPOINT_METADATA_MAGIC) :],
        context="checkpoint metadata",
    )
    _require_fields(
        encoded,
        {
            "version",
            "metadata_revision",
            "source_states",
            "barrier_high_water",
            "operator_states",
            "sink_committables",
        },
        context="checkpoint metadata",
    )
    _require_format_version(
        encoded["version"],
        expected=_CHECKPOINT_FORMAT_VERSION,
        context="checkpoint",
    )
    metadata_revision = _validate_checkpoint_id("metadata_revision", encoded["metadata_revision"])
    barrier_high_water = _validate_checkpoint_id("barrier_high_water", encoded["barrier_high_water"])

    source_payloads = _decode_source_envelopes(encoded["source_states"])
    operator_states = _decode_operator_envelopes(encoded["operator_states"])
    sink_payloads = _decode_sink_envelopes(encoded["sink_committables"])

    return _CheckpointEnvelope(
        metadata_revision=metadata_revision,
        barrier_high_water=barrier_high_water,
        source_payloads=source_payloads,
        operator_states=operator_states,
        sink_payloads=sink_payloads,
    )


def _load_source_states(
    source_payloads: tuple[tuple[str, int, bytes], ...],
) -> tuple[SourceCheckpointEntry, ...]:
    """Enter the trusted source-owned application payload boundary."""

    return tuple(
        SourceCheckpointEntry(
            task_key,
            checkpoint_id,
            _loads_trusted_application_payload(application_payload, context=f"source state {task_key!r}"),
        )
        for task_key, checkpoint_id, application_payload in source_payloads
    )


def _load_sink_committables(
    sink_payloads: tuple[tuple[str, int, str, bytes], ...],
) -> tuple[SinkCommittableCheckpointEntry, ...]:
    """Enter the trusted sink-owned application payload boundary."""

    return tuple(
        _decode_sink_application_payload(task_key, checkpoint_id, transaction_id, application_payload)
        for task_key, checkpoint_id, transaction_id, application_payload in sink_payloads
    )


def _encode_checkpoint_mapping(data: Mapping[str, Any]) -> bytes:
    source_states = data["source_states"]
    operator_states = data["operator_states"]
    sink_committables = data["sink_committables"]
    encoded = {
        "version": data["version"],
        "metadata_revision": data["metadata_revision"],
        "barrier_high_water": data["barrier_high_water"],
        "source_states": [
            {
                "task_key": entry.task_key,
                "checkpoint_id": entry.checkpoint_id,
                **_encode_application_payload(entry.state),
            }
            for entry in source_states
        ],
        "operator_states": [
            {
                "task_key": entry.task_key,
                "uri": entry.uri,
                "checksum": entry.checksum,
                "size_bytes": entry.size_bytes,
            }
            for entry in operator_states
        ],
        "sink_committables": [
            {
                "task_key": entry.task_key,
                "checkpoint_id": entry.checkpoint_id,
                "transaction_id": entry.transaction_id,
                **_encode_application_payload(entry.committable),
            }
            for entry in sink_committables
        ],
    }
    body = json.dumps(encoded, allow_nan=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return _CHECKPOINT_METADATA_MAGIC + body


def _encode_application_payload(value: Any) -> dict[str, Any]:
    payload = pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
    return {
        "payload_b64": base64.b64encode(payload).decode("ascii"),
        "size_bytes": len(payload),
        "checksum": f"sha256:{hashlib.sha256(payload).hexdigest()}",
    }


def _decode_source_envelopes(value: Any) -> tuple[tuple[str, int, bytes], ...]:
    records = _require_json_array(value, context="checkpoint source_states")
    decoded = []
    task_keys: set[str] = set()
    for index, record in enumerate(records):
        context = f"checkpoint source_states[{index}]"
        mapping = _require_json_mapping(record, context=context)
        _require_fields(
            mapping,
            {"task_key", "checkpoint_id", "payload_b64", "size_bytes", "checksum"},
            context=context,
        )
        task_key = _require_non_empty_string(mapping["task_key"], context=f"{context}.task_key")
        if task_key in task_keys:
            raise ValueError("checkpoint source_states must contain at most one entry per task_key")
        task_keys.add(task_key)
        checkpoint_id = _validate_checkpoint_id(
            f"source_states[{index}].checkpoint_id",
            mapping["checkpoint_id"],
        )
        decoded.append((task_key, checkpoint_id, _decode_application_envelope(mapping, context=context)))
    return tuple(decoded)


def _decode_operator_envelopes(value: Any) -> tuple[OperatorStateCheckpointEntry, ...]:
    records = _require_json_array(value, context="checkpoint operator_states")
    decoded = []
    task_keys: set[str] = set()
    for index, record in enumerate(records):
        context = f"checkpoint operator_states[{index}]"
        mapping = _require_json_mapping(record, context=context)
        _require_fields(mapping, {"task_key", "uri", "checksum", "size_bytes"}, context=context)
        task_key = _require_non_empty_string(mapping["task_key"], context=f"{context}.task_key")
        if task_key in task_keys:
            raise ValueError("checkpoint operator_states must contain at most one entry per task_key")
        task_keys.add(task_key)
        uri = _require_non_empty_string(mapping["uri"], context=f"{context}.uri")
        checksum = _require_sha256(mapping["checksum"], context=f"{context}.checksum")
        size_bytes = _validate_size(mapping["size_bytes"], context=f"{context}.size_bytes")
        decoded.append(
            OperatorStateCheckpointEntry(
                task_key=task_key,
                uri=uri,
                checksum=checksum,
                size_bytes=size_bytes,
            )
        )
    return tuple(decoded)


def _decode_sink_envelopes(value: Any) -> tuple[tuple[str, int, str, bytes], ...]:
    records = _require_json_array(value, context="checkpoint sink_committables")
    decoded = []
    task_checkpoints: set[tuple[str, int]] = set()
    for index, record in enumerate(records):
        context = f"checkpoint sink_committables[{index}]"
        mapping = _require_json_mapping(record, context=context)
        _require_fields(
            mapping,
            {
                "task_key",
                "checkpoint_id",
                "transaction_id",
                "payload_b64",
                "size_bytes",
                "checksum",
            },
            context=context,
        )
        task_key = _require_non_empty_string(mapping["task_key"], context=f"{context}.task_key")
        checkpoint_id = _validate_checkpoint_id(
            f"sink_committables[{index}].checkpoint_id",
            mapping["checkpoint_id"],
        )
        transaction_id = _require_non_empty_string(
            mapping["transaction_id"],
            context=f"{context}.transaction_id",
        )
        task_checkpoint = (task_key, checkpoint_id)
        if task_checkpoint in task_checkpoints:
            raise ValueError(
                "checkpoint sink_committables must contain at most one transaction per task and checkpoint"
            )
        task_checkpoints.add(task_checkpoint)
        decoded.append(
            (
                task_key,
                checkpoint_id,
                transaction_id,
                _decode_application_envelope(mapping, context=context),
            )
        )
    return tuple(decoded)


def _decode_application_envelope(mapping: Mapping[str, Any], *, context: str) -> bytes:
    encoded_payload = mapping["payload_b64"]
    if not isinstance(encoded_payload, str):
        raise ValueError(f"{context}.payload_b64 must be a base64 string")
    size_bytes = _validate_size(mapping["size_bytes"], context=f"{context}.size_bytes")
    expected_checksum = _require_sha256(mapping["checksum"], context=f"{context}.checksum")
    expected_base64_size = 4 * ((size_bytes + 2) // 3)
    if len(encoded_payload) != expected_base64_size:
        raise ValueError(f"{context} base64 size mismatch: expected {expected_base64_size}, got {len(encoded_payload)}")
    try:
        payload = base64.b64decode(encoded_payload, validate=True)
    except (ValueError, binascii.Error) as error:
        raise ValueError(f"{context}.payload_b64 is not valid base64") from error
    if len(payload) != size_bytes:
        raise ValueError(f"{context} payload size mismatch: expected {size_bytes}, got {len(payload)}")
    checksum = f"sha256:{hashlib.sha256(payload).hexdigest()}"
    if checksum != expected_checksum:
        raise ValueError(f"{context} payload checksum mismatch: expected {expected_checksum}, got {checksum}")
    return payload


def _decode_sink_application_payload(
    task_key: str,
    checkpoint_id: int,
    transaction_id: str,
    payload: bytes,
) -> SinkCommittableCheckpointEntry:
    committable = _loads_trusted_application_payload(payload, context=f"sink committable {task_key!r}")
    entry = SinkCommittableCheckpointEntry(task_key, checkpoint_id, committable)
    if entry.transaction_id != transaction_id:
        raise ValueError(
            f"sink committable transaction_id mismatch for {task_key!r}: "
            f"expected {transaction_id!r}, got {entry.transaction_id!r}"
        )
    return entry


def _loads_trusted_application_payload(payload: bytes, *, context: str) -> Any:
    """Load an application-owned value after its safe envelope was validated."""

    try:
        return pickle.loads(payload)
    except Exception as error:
        raise ValueError(f"unable to deserialize trusted {context} payload") from error


def _decode_json_mapping(payload: bytes, *, context: str) -> dict[str, Any]:
    if not isinstance(payload, bytes):
        raise TypeError(f"{context} must be bytes")

    def reject_constant(value: str) -> None:
        raise ValueError(f"{context} contains invalid JSON constant {value}")

    def reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        mapping: dict[str, Any] = {}
        for key, value in pairs:
            if key in mapping:
                raise ValueError(f"{context} contains duplicate key {key!r}")
            mapping[key] = value
        return mapping

    decoded = json.loads(
        payload.decode("utf-8"),
        object_pairs_hook=reject_duplicate_keys,
        parse_constant=reject_constant,
    )
    return _require_json_mapping(decoded, context=context)


def _require_json_mapping(value: Any, *, context: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{context} must contain a JSON object")
    return cast(dict[str, Any], value)


def _require_json_array(value: Any, *, context: str) -> list[Any]:
    if not isinstance(value, list):
        raise ValueError(f"{context} must contain a JSON array")
    return value


def _require_fields(mapping: Mapping[str, Any], fields: set[str], *, context: str) -> None:
    actual = set(mapping)
    missing = fields - actual
    if missing:
        raise ValueError(f"{context} is missing fields: {sorted(missing)}")
    unexpected = actual - fields
    if unexpected:
        raise ValueError(f"{context} contains unexpected fields: {sorted(unexpected)}")


def _require_non_empty_string(value: Any, *, context: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{context} must be a non-empty string")
    return value


def _require_sha256(value: Any, *, context: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise ValueError(f"{context} must be a lowercase sha256 checksum")
    return value


def _require_format_version(value: Any, *, expected: int, context: str) -> None:
    if type(value) is not int or value != expected:
        raise ValueError(f"unsupported {context} format version: {value!r}")


def _validate_size(value: Any, *, context: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{context} must be a non-negative integer")
    return int(value)


def _validate_checkpoint_id(name: str, value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"checkpoint {name} must be a non-negative integer")
    return int(value)


def _validate_source_states(source_states: Any) -> None:
    if not isinstance(source_states, tuple) or not all(
        isinstance(item, SourceCheckpointEntry) for item in source_states
    ):
        raise ValueError("checkpoint source_states must be SourceCheckpointEntry values")
    if not all(isinstance(item.task_key, str) and item.task_key.strip() for item in source_states):
        raise ValueError("checkpoint source_states task keys must be non-empty strings")
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
