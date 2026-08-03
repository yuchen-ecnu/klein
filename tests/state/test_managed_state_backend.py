# SPDX-License-Identifier: Apache-2.0
import io
import os
import pickle
import tarfile
from array import array
from datetime import timedelta
from pathlib import Path

import pytest

from ray.klein.state.key_encoding import KEY_ENCODING_VERSION
from ray.klein.state.key_group_range import (
    assign_key_group_range,
    key_group_for_key,
    key_group_owner,
)
from ray.klein.state.list_state_descriptor import ListStateDescriptor
from ray.klein.state.managed_state_snapshot import repartition_managed_state_snapshots
from ray.klein.state.memory_state_backend import MemoryStateBackend
from ray.klein.state.rocks_db_state_backend import (
    _KEY_ENCODING_METADATA_KEY,
    RocksDBStateBackend,
    _extract_checkpoint_archive,
)
from ray.klein.state.state_codec import encode_state_value
from ray.klein.state.state_ttl_config import StateTTLConfig
from ray.klein.state.state_ttl_update_type import StateTTLUpdateType
from ray.klein.state.timer_domain import TimerDomain
from ray.klein.state.value_state_descriptor import ValueStateDescriptor


class _DistinctInt(int):
    def __eq__(self, other):
        return type(other) is type(self) and int(self) == int(other)

    def __ne__(self, other):
        return not self == other

    __hash__ = int.__hash__


class _SnapshotGadget:
    def __reduce__(self):
        return (
            eval,
            ("__import__('os').environ.__setitem__('KLEIN_BACKEND_GADGET_EXECUTED', '1')",),
        )


@pytest.fixture(params=["memory", "rocksdb"])
def backend(request, tmp_path: Path):
    now = [1_000]

    def clock():
        return now[0]

    if request.param == "memory":
        state_backend = MemoryStateBackend(clock=clock)
    else:
        state_backend = RocksDBStateBackend(str(tmp_path / "rocks"), clock=clock)
    yield state_backend, now
    state_backend.close()


def test_key_namespace_state_and_physical_snapshot_round_trip(backend):
    state_backend, _now = backend
    value = ValueStateDescriptor("value")
    values = ListStateDescriptor("values")
    state_backend.current_key = "customer-1"
    state_backend.put(value, {"total": 3})
    state_backend.put(values, [1, 2], namespace=(0, 10))
    snapshot = state_backend.snapshot()
    state_backend.put(value, {"total": 99})

    state_backend.restore(snapshot)
    state_backend.current_key = "customer-1"

    assert state_backend.get(value) == {"total": 3}
    assert state_backend.get(values, namespace=(0, 10)) == [1, 2]
    assert state_backend.namespaces(values) == ((0, 10),)


def test_equal_python_keys_share_managed_state_and_timers(backend):
    state_backend, _now = backend
    value = ValueStateDescriptor("value")
    first_key = {"tenant": 1, "labels": frozenset({"a", "b"})}
    equal_key = {"labels": frozenset({"b", "a"}), "tenant": 1.0}

    state_backend.current_key = first_key
    state_backend.put(value, "shared", namespace={"end": 2, "start": 1})
    state_backend.register_timer(10, {"end": 2, "start": 1}, TimerDomain.EVENT_TIME)

    state_backend.current_key = equal_key
    assert state_backend.get(value, namespace={"start": True, "end": 2.0}) == "shared"
    state_backend.register_timer(10, {"start": True, "end": 2.0}, TimerDomain.EVENT_TIME)

    timers = state_backend.pop_due_timers(10, TimerDomain.EVENT_TIME)
    assert len(timers) == 1


def test_unequal_custom_numeric_keys_keep_independent_state(backend):
    state_backend, _now = backend
    value = ValueStateDescriptor("value")
    custom_key = _DistinctInt(1)

    state_backend.current_key = 1
    state_backend.put(value, "builtin")
    state_backend.current_key = custom_key
    assert state_backend.get(value) is None
    state_backend.put(value, "custom")

    state_backend.current_key = 1
    assert state_backend.get(value) == "builtin"
    state_backend.current_key = custom_key
    assert state_backend.get(value) == "custom"


def test_format_sensitive_memoryview_cannot_alias_bytes_state(backend):
    state_backend, _now = backend
    value = ValueStateDescriptor("value")
    view = memoryview(array("I", [1]))
    payload = bytes(view)

    state_backend.current_key = payload
    state_backend.put(value, "bytes")
    state_backend.current_key = view
    with pytest.raises(TypeError, match="memoryview"):
        state_backend.put(value, "view")

    state_backend.current_key = payload
    assert state_backend.get(value) == "bytes"


def test_ttl_never_returns_expired_and_incremental_cleanup(backend):
    state_backend, now = backend
    descriptor = ValueStateDescriptor(
        "session",
        ttl_config=StateTTLConfig(
            timedelta(milliseconds=50),
            update_type=StateTTLUpdateType.ON_CREATE_AND_WRITE,
        ),
    )
    state_backend.current_key = "key"
    state_backend.put(descriptor, "alive")
    now[0] += 49
    assert state_backend.get(descriptor) == "alive"
    now[0] += 1
    assert state_backend.get(descriptor) is None
    assert state_backend.cleanup_expired(limit=10) == 0


def test_read_refreshes_ttl(backend):
    state_backend, now = backend
    descriptor = ValueStateDescriptor(
        "session",
        ttl_config=StateTTLConfig(
            timedelta(milliseconds=50),
            update_type=StateTTLUpdateType.ON_READ_AND_WRITE,
        ),
    )
    state_backend.current_key = "key"
    state_backend.put(descriptor, "alive")
    now[0] += 40
    assert state_backend.get(descriptor) == "alive"
    now[0] += 40
    assert state_backend.get(descriptor) == "alive"


def test_ttl_refresh_replaces_the_previous_expiry_index(backend):
    state_backend, now = backend
    descriptor = ValueStateDescriptor(
        "session",
        ttl_config=StateTTLConfig(
            timedelta(milliseconds=50),
            update_type=StateTTLUpdateType.ON_CREATE_AND_WRITE,
        ),
    )
    state_backend.current_key = "key"
    state_backend.put(descriptor, "first")
    now[0] += 40
    state_backend.put(descriptor, "second")

    now[0] += 10
    assert state_backend.cleanup_expired(limit=1) == 0
    assert state_backend.get(descriptor) == "second"


def test_timers_are_deduplicated_ordered_and_checkpointed(backend):
    state_backend, _now = backend
    state_backend.current_key = "key-b"
    state_backend.register_timer(30, "window-b", TimerDomain.EVENT_TIME)
    state_backend.current_key = "key-a"
    state_backend.register_timer(10, "window-a", TimerDomain.EVENT_TIME)
    state_backend.register_timer(10, "window-a", TimerDomain.EVENT_TIME)
    state_backend.register_timer(5, None, TimerDomain.PROCESSING_TIME)
    snapshot = state_backend.snapshot()

    first = state_backend.pop_due_timers(15, TimerDomain.EVENT_TIME)
    assert [(event.timestamp, event.key, event.namespace) for event in first] == [(10, "key-a", "window-a")]
    state_backend.restore(snapshot)
    processing = state_backend.pop_due_timers(5, TimerDomain.PROCESSING_TIME)
    assert processing[0].key == "key-a"
    assert [event.timestamp for event in state_backend.pop_due_timers(100, TimerDomain.EVENT_TIME)] == [10, 30]


def test_legacy_key_group_snapshot_is_rejected_before_state_is_replaced(backend):
    state_backend, _now = backend
    descriptor = ValueStateDescriptor("value")
    state_backend.current_key = "key"
    state_backend.put(descriptor, "preserved")
    group = key_group_for_key("key", 16)
    snapshot = pickle.loads(state_backend.snapshot_key_groups(16, (group,))[group])
    snapshot.pop("key_encoding_version")

    with pytest.raises(ValueError, match="legacy key encoding"):
        state_backend.restore_key_groups({group: pickle.dumps(snapshot)})

    state_backend.current_key = "key"
    assert state_backend.get(descriptor) == "preserved"


def test_key_group_snapshot_rejects_pickle_globals_before_state_is_replaced(backend, monkeypatch):
    state_backend, _now = backend
    descriptor = ValueStateDescriptor("value")
    state_backend.current_key = "key"
    state_backend.put(descriptor, "preserved")
    monkeypatch.delenv("KLEIN_BACKEND_GADGET_EXECUTED", raising=False)

    with pytest.raises(pickle.UnpicklingError, match=r"builtins\.eval is not allowed"):
        state_backend.restore_key_groups({0: pickle.dumps(_SnapshotGadget())})

    assert "KLEIN_BACKEND_GADGET_EXECUTED" not in os.environ
    state_backend.current_key = "key"
    assert state_backend.get(descriptor) == "preserved"


def test_memory_full_snapshot_rejects_legacy_encoding_before_mutation():
    state_backend = MemoryStateBackend()
    descriptor = ValueStateDescriptor("value")
    state_backend.current_key = "key"
    state_backend.put(descriptor, "preserved")
    legacy = pickle.dumps(({}, {}, {}), protocol=pickle.HIGHEST_PROTOCOL)

    with pytest.raises(ValueError, match="legacy key encoding"):
        state_backend.restore(legacy)

    assert state_backend.get(descriptor) == "preserved"


def test_rocksdb_full_snapshot_rejects_legacy_encoding_before_mutation(tmp_path):
    descriptor = ValueStateDescriptor("value")
    source = RocksDBStateBackend(str(tmp_path / "source"))
    source.current_key = "key"
    source.put(descriptor, "legacy")
    source._delete_cf_key("metadata", _KEY_ENCODING_METADATA_KEY)
    legacy = source.snapshot()
    source.close()

    target = RocksDBStateBackend(str(tmp_path / "target"))
    target.current_key = "key"
    target.put(descriptor, "preserved")
    with pytest.raises(ValueError, match="legacy key encoding"):
        target.restore(legacy)

    assert target.get(descriptor) == "preserved"
    target.close()


@pytest.mark.parametrize(
    ("state_entries", "message"),
    [
        ([(b"missing-value",)], "entry must contain key and value"),
        (
            [(b"truncated-state-key", encode_state_value(b"payload", None))],
            "invalid 'state' entry",
        ),
    ],
)
def test_rocksdb_key_group_restore_validates_entries_before_mutation(
    tmp_path,
    state_entries,
    message,
):
    target = RocksDBStateBackend(str(tmp_path / "target"))
    descriptor = ValueStateDescriptor("value")
    target.current_key = "key"
    target.put(descriptor, "preserved")
    malformed = pickle.dumps(
        {
            "format_version": 2,
            "key_encoding_version": KEY_ENCODING_VERSION,
            "state": state_entries,
            "expiry": [],
            "timers": [],
            "metadata": [],
        },
        protocol=pickle.HIGHEST_PROTOCOL,
    )

    with pytest.raises(ValueError, match=message):
        target.restore_key_groups({0: malformed})

    target.current_key = "key"
    assert target.get(descriptor) == "preserved"
    target.close()


def test_logical_key_group_snapshot_restores_only_selected_partition(backend):
    state_backend, _now = backend
    descriptor = ValueStateDescriptor("value")
    first = "key-0"
    first_group = key_group_for_key(first, 16)
    second = next(f"key-{index}" for index in range(1, 100) if key_group_for_key(f"key-{index}", 16) != first_group)
    second_group = key_group_for_key(second, 16)
    state_backend.current_key = first
    state_backend.put(descriptor, "first")
    state_backend.register_timer(10, "first-timer", TimerDomain.EVENT_TIME)
    state_backend.current_key = second
    state_backend.put(descriptor, "second")
    state_backend.register_timer(10, "second-timer", TimerDomain.EVENT_TIME)

    snapshots = state_backend.snapshot_key_groups(16, range(16))
    state_backend.restore_key_groups({first_group: snapshots[first_group]})

    state_backend.current_key = first
    assert state_backend.get(descriptor) == "first"
    state_backend.current_key = second
    assert state_backend.get(descriptor) is None
    timers = state_backend.pop_due_timers(10, TimerDomain.EVENT_TIME)
    assert [(event.key, event.namespace) for event in timers] == [(first, "first-timer")]
    assert second_group in snapshots


def test_coordinator_repartitioned_snapshot_restores_backend_owned_groups(backend):
    state_backend, _now = backend
    descriptor = ValueStateDescriptor("value")
    keys = [f"key-{index}" for index in range(24)]
    for index, key in enumerate(keys):
        state_backend.current_key = key
        state_backend.put(descriptor, index)

    old_fragments = []
    for old_index in range(2):
        old_range = assign_key_group_range(16, 2, old_index)
        old_fragments.append(
            pickle.dumps(
                {
                    "format_version": 2,
                    "key_encoding_version": KEY_ENCODING_VERSION,
                    "max_parallelism": 16,
                    "key_group_range": old_range,
                    "key_groups": dict(state_backend.snapshot_key_groups(16, old_range)),
                    "watermark": 11,
                },
                protocol=pickle.HIGHEST_PROTOCOL,
            )
        )

    target_fragments = repartition_managed_state_snapshots(old_fragments, 3)

    for target_index, target_fragment in enumerate(target_fragments):
        payload = pickle.loads(target_fragment)
        assert payload["key_group_range"] == assign_key_group_range(16, 3, target_index)
        assert all(key_group_owner(group, 16, 3) == target_index for group in payload["key_groups"])
        state_backend.restore_key_groups(payload["key_groups"])
        for index, key in enumerate(keys):
            state_backend.current_key = key
            expected = index if key_group_owner(key_group_for_key(key, 16), 16, 3) == target_index else None
            assert state_backend.get(descriptor) == expected


@pytest.mark.parametrize(
    ("member_name", "member_type"),
    [("../escape", tarfile.REGTYPE), ("link", tarfile.SYMTYPE)],
)
def test_rocksdb_checkpoint_archive_rejects_unsafe_members(
    tmp_path: Path,
    member_name: str,
    member_type: bytes,
) -> None:
    payload = io.BytesIO()
    with tarfile.open(fileobj=payload, mode="w") as archive:
        member = tarfile.TarInfo(member_name)
        member.type = member_type
        member.linkname = "../escape"
        archive.addfile(member, io.BytesIO(b"") if member.isfile() else None)

    with (
        tarfile.open(fileobj=io.BytesIO(payload.getvalue()), mode="r:") as archive,
        pytest.raises(ValueError, match=r"escapes|non-file"),
    ):
        _extract_checkpoint_archive(archive, tmp_path / "restore")
