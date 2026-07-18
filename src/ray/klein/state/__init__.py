# SPDX-License-Identifier: Apache-2.0
"""Progress tracking and Ray-native computational state primitives."""

from typing import Any

from ray.klein._internal.lazy_exports import resolve_lazy_export

_EXPORTS = {
    "CheckpointFileScope": ("ray.klein.state.checkpoint_file_scope", "CheckpointFileScope"),
    "CheckpointFileSystem": ("ray.klein.state.checkpoint_file_system", "CheckpointFileSystem"),
    "CheckpointLayout": ("ray.klein.state.checkpoint_layout", "CheckpointLayout"),
    "CheckpointStore": ("ray.klein.state.checkpoint_store", "CheckpointStore"),
    "FileSystemCheckpointStore": (
        "ray.klein.state.file_system_checkpoint_store",
        "FileSystemCheckpointStore",
    ),
    "KeyedStateContext": ("ray.klein.state.keyed_state_context", "KeyedStateContext"),
    "KeyGroupRange": ("ray.klein.state.key_group_range", "KeyGroupRange"),
    "ListState": ("ray.klein.state.list_state", "ListState"),
    "ListStateDescriptor": (
        "ray.klein.state.list_state_descriptor",
        "ListStateDescriptor",
    ),
    "ManagedStateBackend": (
        "ray.klein.state.managed_state_backend",
        "ManagedStateBackend",
    ),
    "MapState": ("ray.klein.state.map_state", "MapState"),
    "MapStateDescriptor": (
        "ray.klein.state.map_state_descriptor",
        "MapStateDescriptor",
    ),
    "MemoryStateBackend": (
        "ray.klein.state.memory_state_backend",
        "MemoryStateBackend",
    ),
    "ObjectStoreSnapshotCache": (
        "ray.klein.state.object_store_snapshot_cache",
        "ObjectStoreSnapshotCache",
    ),
    "ObjectStoreStateBackend": ("ray.klein.state.object_store_state_backend", "ObjectStoreStateBackend"),
    "RocksDBStateBackend": (
        "ray.klein.state.rocks_db_state_backend",
        "RocksDBStateBackend",
    ),
    "SourceCheckpointEntry": (
        "ray.klein.state.source_checkpoint_entry",
        "SourceCheckpointEntry",
    ),
    "StateCheckpointEntry": ("ray.klein.state.state_checkpoint_entry", "StateCheckpointEntry"),
    "StateCheckpointManifest": ("ray.klein.state.state_checkpoint_manifest", "StateCheckpointManifest"),
    "StateConflictError": ("ray.klein.state.state_conflict_error", "StateConflictError"),
    "StateHandle": ("ray.klein.state.state_handle", "StateHandle"),
    "StatePartition": ("ray.klein.state.state_partition", "StatePartition"),
    "StateSnapshot": ("ray.klein.state.state_snapshot", "StateSnapshot"),
    "StateSnapshotReference": (
        "ray.klein.state.state_snapshot_reference",
        "StateSnapshotReference",
    ),
    "StateTTLConfig": ("ray.klein.state.state_ttl_config", "StateTTLConfig"),
    "StateTTLUpdateType": (
        "ray.klein.state.state_ttl_update_type",
        "StateTTLUpdateType",
    ),
    "StateVisibility": (
        "ray.klein.state.state_visibility",
        "StateVisibility",
    ),
    "TimerDomain": ("ray.klein.state.timer_domain", "TimerDomain"),
    "TimerEvent": ("ray.klein.state.timer_event", "TimerEvent"),
    "TimerService": ("ray.klein.state.timer_service", "TimerService"),
    "ValueState": ("ray.klein.state.value_state", "ValueState"),
    "ValueStateDescriptor": (
        "ray.klein.state.value_state_descriptor",
        "ValueStateDescriptor",
    ),
}

__all__ = sorted(_EXPORTS)


def __getattr__(name: str) -> Any:
    return resolve_lazy_export(name, _EXPORTS, globals(), __name__)
