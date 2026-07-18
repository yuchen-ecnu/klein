# SPDX-License-Identifier: Apache-2.0
from collections.abc import Callable
from typing import Any

from ray.klein.state.state_conflict_error import StateConflictError
from ray.klein.state.state_handle import StateHandle
from ray.klein.state.state_partition import StatePartition
from ray.klein.state.state_snapshot import StateSnapshot


class ObjectStoreStateBackend:
    """MVCC metadata owner for immutable values held in Ray's Object Store.

    This class is deliberately independent of Ray's actor decorator so it can be
    tested locally. Production code must instantiate it inside the long-lived
    state registry actor: keeping the returned ``ObjectRef`` objects in this
    instance is what pins the hot state through Ray's distributed ref counting.
    """

    def __init__(self, put: Callable[[Any], Any], get: Callable[[Any], Any]) -> None:
        self._put = put
        self._get = get
        self._current: dict[StatePartition, StateHandle] = {}
        self._snapshots: dict[int, StateSnapshot] = {}

    def current_handle(self, partition: StatePartition) -> StateHandle | None:
        return self._current.get(partition)

    def read(self, partition: StatePartition, default: Any = None) -> Any:
        handle = self.current_handle(partition)
        return default if handle is None else self._get(handle.object_ref)

    def commit(
        self,
        partition: StatePartition,
        value: Any,
        *,
        expected_version: int,
        input_sequence: int,
        size_bytes: int = 0,
    ) -> StateHandle:
        """Put and atomically commit an immutable value.

        A retry with the same expected version and input sequence returns the
        already committed handle. This makes an RPC retry after a lost response
        idempotent rather than creating another version.
        """

        if size_bytes < 0:
            raise ValueError("size_bytes must be non-negative")
        replay = self._validate_update(partition, expected_version, input_sequence)
        if replay is not None:
            return replay
        object_ref = self._put(value)
        return self._commit_validated_ref(
            partition,
            object_ref,
            expected_version=expected_version,
            input_sequence=input_sequence,
            size_bytes=size_bytes,
        )

    def commit_ref(
        self,
        partition: StatePartition,
        object_ref: Any,
        *,
        expected_version: int,
        input_sequence: int,
        size_bytes: int = 0,
    ) -> StateHandle:
        """Adopt a registry-owned task result without copying its value."""

        if size_bytes < 0:
            raise ValueError("size_bytes must be non-negative")
        replay = self._validate_update(partition, expected_version, input_sequence)
        if replay is not None:
            return replay
        return self._commit_validated_ref(
            partition,
            object_ref,
            expected_version=expected_version,
            input_sequence=input_sequence,
            size_bytes=size_bytes,
        )

    def begin_snapshot(self, checkpoint_id: int, epoch: int) -> StateSnapshot:
        """Pin an immutable view of every committed partition."""

        existing = self._snapshots.get(checkpoint_id)
        if existing is not None:
            if existing.epoch != epoch:
                raise StateConflictError(
                    f"checkpoint {checkpoint_id} already belongs to epoch {existing.epoch}, not {epoch}"
                )
            return existing
        snapshot = StateSnapshot(
            checkpoint_id=checkpoint_id,
            epoch=epoch,
            handles=tuple(sorted(self._current.values(), key=lambda handle: handle.partition)),
        )
        self._snapshots[checkpoint_id] = snapshot
        return snapshot

    def release_snapshot(self, checkpoint_id: int) -> None:
        """Drop registry references after a durable checkpoint is superseded."""

        self._snapshots.pop(checkpoint_id, None)

    def restore_hot(self, snapshot: StateSnapshot) -> None:
        """Restore handles while their original Ray owner is still alive."""

        self._current = {handle.partition: handle for handle in snapshot.handles}
        self._snapshots[snapshot.checkpoint_id] = snapshot

    @property
    def retained_snapshot_ids(self) -> tuple[int, ...]:
        return tuple(sorted(self._snapshots))

    @property
    def partitions(self) -> tuple[StatePartition, ...]:
        return tuple(sorted(self._current))

    def _validate_update(
        self,
        partition: StatePartition,
        expected_version: int,
        input_sequence: int,
    ) -> StateHandle | None:
        if expected_version < 0:
            raise ValueError("expected_version must be non-negative")
        if input_sequence < 0:
            raise ValueError("input_sequence must be non-negative")

        current = self._current.get(partition)
        current_version = 0 if current is None else current.version
        if current is not None and current.version == expected_version + 1 and current.input_sequence == input_sequence:
            return current
        if current_version != expected_version:
            raise StateConflictError(
                f"state version conflict for {partition.storage_key}: expected {expected_version}, "
                f"current {current_version}"
            )
        if current is not None and input_sequence <= current.input_sequence:
            raise StateConflictError(
                f"input sequence for {partition.storage_key} must advance beyond {current.input_sequence}"
            )
        return None

    def _commit_validated_ref(
        self,
        partition: StatePartition,
        object_ref: Any,
        *,
        expected_version: int,
        input_sequence: int,
        size_bytes: int,
    ) -> StateHandle:
        handle = StateHandle(
            partition=partition,
            version=expected_version + 1,
            object_ref=object_ref,
            input_sequence=input_sequence,
            size_bytes=size_bytes,
        )
        self._current[partition] = handle
        return handle
