# SPDX-License-Identifier: Apache-2.0
from dataclasses import replace
from pathlib import Path

import pytest

from ray.klein.state.checkpoint_file_scope import CheckpointFileScope
from ray.klein.state.checkpoint_file_system import CheckpointFileSystem
from ray.klein.state.checkpoint_layout import CheckpointLayout
from ray.klein.state.file_system_checkpoint_store import FileSystemCheckpointStore
from ray.klein.state.state_checkpoint_manifest import StateCheckpointManifest
from ray.klein.state.state_handle import StateHandle
from ray.klein.state.state_partition import StatePartition


def _handle(job_id: str = "job/one", version: int = 1) -> StateHandle:
    return StateHandle(
        partition=StatePartition(job_id, "map operator", 3),
        version=version,
        object_ref=object(),
        input_sequence=17,
    )


def _manifest(checkpoint_id: int, entry) -> StateCheckpointManifest:
    return StateCheckpointManifest(
        job_id=entry.partition.job_id,
        checkpoint_id=checkpoint_id,
        epoch=2,
        entries=(entry,),
    )


def test_commit_marker_makes_checkpoint_visible_and_state_readable(tmp_path: Path):
    store = FileSystemCheckpointStore(tmp_path.as_uri())
    entry = store.write_state(7, _handle(), {"count": 42})
    layout = CheckpointLayout("job/one")
    filesystem = CheckpointFileSystem(tmp_path.as_uri())

    assert entry.scope == CheckpointFileScope.EXCLUSIVE
    assert "/job%2Fone/chk-7/op-map%20operator/kg-3/" in entry.uri
    assert not filesystem.exists(layout.metadata_path(7))
    assert store.latest("job/one") is None

    manifest = _manifest(7, entry)
    store.commit(manifest)

    assert filesystem.exists(layout.metadata_path(7))
    assert store.latest("job/one") == manifest
    assert store.read_state(entry) == {"count": 42}


def test_latest_scans_committed_directories_when_pointer_is_corrupt(tmp_path: Path):
    root_uri = tmp_path.as_uri()
    store = FileSystemCheckpointStore(root_uri)
    for checkpoint_id in (2, 9):
        entry = store.write_state(checkpoint_id, _handle(version=checkpoint_id), checkpoint_id)
        store.commit(_manifest(checkpoint_id, entry))

    filesystem = CheckpointFileSystem(root_uri)
    layout = CheckpointLayout("job/one")
    filesystem.write_bytes(layout.latest_pointer, b"corrupt", atomic=True)
    filesystem.create_dir(layout.checkpoint_directory(100))

    latest = store.latest("job/one")

    assert latest is not None
    assert latest.checkpoint_id == 9
    assert store.list_completed_checkpoints("job/one") == (2, 9)

    filesystem.write_bytes(
        layout.latest_pointer,
        b'{"checkpoint_id":2,"format_version":1}',
        atomic=True,
    )
    assert store.latest("job/one").checkpoint_id == 9


def test_latest_skips_manifest_with_unsupported_format(tmp_path: Path):
    root_uri = tmp_path.as_uri()
    store = FileSystemCheckpointStore(root_uri)
    for checkpoint_id in (2, 9):
        entry = store.write_state(checkpoint_id, _handle(version=checkpoint_id), checkpoint_id)
        store.commit(_manifest(checkpoint_id, entry))

    filesystem = CheckpointFileSystem(root_uri)
    layout = CheckpointLayout("job/one")
    filesystem.write_bytes(
        layout.metadata_path(9),
        b'{"format_version":2,"manifest":{}}',
        atomic=True,
    )

    latest = store.latest("job/one")

    assert latest is not None
    assert latest.checkpoint_id == 2


def test_scopes_follow_flink_layout_and_cleanup_preserves_nonexclusive_state(tmp_path: Path):
    root_uri = tmp_path.as_uri()
    store = FileSystemCheckpointStore(root_uri)
    shared_1 = store.write_state(
        1,
        _handle(version=1),
        [1, 2, 3],
        scope=CheckpointFileScope.SHARED,
    )
    shared_2 = store.write_state(
        2,
        _handle(version=2),
        [1, 2, 3],
        scope=CheckpointFileScope.SHARED,
    )
    task_owned = store.write_state(
        1,
        _handle(version=1),
        "owned",
        scope=CheckpointFileScope.TASK_OWNED,
    )
    exclusive = store.write_state(1, _handle(version=1), "exclusive")
    store.commit(
        StateCheckpointManifest(
            job_id="job/one",
            checkpoint_id=1,
            epoch=0,
            entries=(shared_1, task_owned, exclusive),
        )
    )

    assert shared_1.uri == shared_2.uri
    assert "/shared/" in shared_1.uri
    assert "/taskowned/" in task_owned.uri
    assert "/chk-1/" in exclusive.uri

    store.delete_checkpoint("job/one", 1)

    filesystem = CheckpointFileSystem(root_uri)
    assert not filesystem.exists(CheckpointLayout("job/one").checkpoint_directory(1))
    assert filesystem.exists(filesystem.relative_path(shared_1.uri))
    assert filesystem.exists(filesystem.relative_path(task_owned.uri))


def test_retention_keeps_newest_completed_checkpoint(tmp_path: Path):
    store = FileSystemCheckpointStore(tmp_path.as_uri())
    for checkpoint_id in (1, 2, 3):
        entry = store.write_state(checkpoint_id, _handle(version=checkpoint_id), checkpoint_id)
        store.commit(_manifest(checkpoint_id, entry))

    store.cleanup_checkpoints("job/one", retained_count=2)

    assert store.list_completed_checkpoints("job/one") == (2, 3)
    assert store.latest("job/one").checkpoint_id == 3


def test_read_state_rejects_corruption_and_commit_rejects_missing_object(tmp_path: Path):
    root_uri = tmp_path.as_uri()
    store = FileSystemCheckpointStore(root_uri)
    entry = store.write_state(1, _handle(), {"value": 1})
    filesystem = CheckpointFileSystem(root_uri)
    relative_path = filesystem.relative_path(entry.uri)
    filesystem.write_bytes(relative_path, b"tampered")

    with pytest.raises(ValueError, match=r"(?:size|checksum) mismatch"):
        store.read_state(entry)

    missing = replace(entry, uri=f"{root_uri}/missing.bin")
    with pytest.raises(FileNotFoundError, match="does not exist"):
        store.commit(_manifest(1, missing))
