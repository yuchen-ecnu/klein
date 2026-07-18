# SPDX-License-Identifier: Apache-2.0
import pytest

from ray.klein.state.object_store_state_backend import ObjectStoreStateBackend
from ray.klein.state.state_conflict_error import StateConflictError
from ray.klein.state.state_partition import StatePartition


class FakeObjectStore:
    def __init__(self):
        self.values = {}
        self.put_count = 0

    def put(self, value):
        self.put_count += 1
        ref = f"ref-{self.put_count}"
        self.values[ref] = value
        return ref

    def get(self, ref):
        return self.values[ref]


@pytest.fixture
def partition():
    return StatePartition("job", "operator", 3)


@pytest.fixture
def backend():
    store = FakeObjectStore()
    return ObjectStoreStateBackend(store.put, store.get), store


def test_commits_immutable_versions_and_reads_current_value(backend, partition):
    state, _ = backend

    first = state.commit(partition, {"count": 1}, expected_version=0, input_sequence=10)
    second = state.commit(partition, {"count": 2}, expected_version=1, input_sequence=11)

    assert first.version == 1
    assert second.version == 2
    assert state.read(partition) == {"count": 2}
    assert state.partitions == (partition,)


def test_rejects_stale_version_or_non_monotonic_sequence(backend, partition):
    state, _ = backend
    state.commit(partition, "v1", expected_version=0, input_sequence=10)

    with pytest.raises(StateConflictError, match="version conflict"):
        state.commit(partition, "stale", expected_version=0, input_sequence=11)
    with pytest.raises(StateConflictError, match="must advance"):
        state.commit(partition, "duplicate", expected_version=1, input_sequence=10)


def test_lost_response_retry_is_idempotent_and_does_not_put_again(backend, partition):
    state, store = backend
    original = state.commit(partition, "value", expected_version=0, input_sequence=10)

    replay = state.commit(partition, "value", expected_version=0, input_sequence=10)

    assert replay is original
    assert store.put_count == 1


def test_snapshot_keeps_old_version_while_current_state_advances(backend, partition):
    state, _ = backend
    first = state.commit(partition, "v1", expected_version=0, input_sequence=10)
    snapshot = state.begin_snapshot(checkpoint_id=7, epoch=2)
    state.commit(partition, "v2", expected_version=1, input_sequence=11)

    assert snapshot.handles == (first,)
    assert state.current_handle(partition).version == 2
    assert state.retained_snapshot_ids == (7,)

    state.release_snapshot(7)
    assert state.retained_snapshot_ids == ()


def test_snapshot_creation_is_idempotent_but_epoch_is_fenced(backend, partition):
    state, _ = backend
    state.commit(partition, "v1", expected_version=0, input_sequence=10)

    original = state.begin_snapshot(checkpoint_id=7, epoch=2)
    assert state.begin_snapshot(checkpoint_id=7, epoch=2) is original
    with pytest.raises(StateConflictError, match="already belongs to epoch"):
        state.begin_snapshot(checkpoint_id=7, epoch=3)


def test_can_restore_hot_snapshot_while_refs_are_valid(backend, partition):
    state, store = backend
    first = state.commit(partition, "v1", expected_version=0, input_sequence=10)
    snapshot = state.begin_snapshot(checkpoint_id=7, epoch=2)
    state.commit(partition, "v2", expected_version=1, input_sequence=11)

    restored = ObjectStoreStateBackend(store.put, store.get)
    restored.restore_hot(snapshot)

    assert restored.current_handle(partition) == first
    assert restored.read(partition) == "v1"
