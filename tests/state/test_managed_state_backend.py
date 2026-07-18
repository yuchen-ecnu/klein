# SPDX-License-Identifier: Apache-2.0
import io
import tarfile
from datetime import timedelta
from pathlib import Path

import pytest

from ray.klein.state.key_group_range import key_group_for_key
from ray.klein.state.list_state_descriptor import ListStateDescriptor
from ray.klein.state.memory_state_backend import MemoryStateBackend
from ray.klein.state.rocks_db_state_backend import (
    RocksDBStateBackend,
    _extract_checkpoint_archive,
)
from ray.klein.state.state_ttl_config import StateTTLConfig
from ray.klein.state.state_ttl_update_type import StateTTLUpdateType
from ray.klein.state.timer_domain import TimerDomain
from ray.klein.state.value_state_descriptor import ValueStateDescriptor


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
