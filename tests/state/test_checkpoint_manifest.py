# SPDX-License-Identifier: Apache-2.0
import pytest

from ray.klein.state.state_checkpoint_entry import StateCheckpointEntry
from ray.klein.state.state_checkpoint_manifest import StateCheckpointManifest
from ray.klein.state.state_partition import StatePartition


def test_manifest_round_trip_contains_no_object_ref():
    partition = StatePartition("job", "operator", 3)
    manifest = StateCheckpointManifest(
        job_id="job",
        checkpoint_id=42,
        epoch=7,
        entries=(
            StateCheckpointEntry(
                partition=partition,
                version=9,
                input_sequence=123,
                uri="s3://checkpoints/job/42/operator/3.bin",
                checksum="sha256:abc",
                size_bytes=1024,
            ),
        ),
        source_positions=(("source-1", "offset-123"),),
        sink_transactions=(("sink-1", "transaction-42"),),
    )

    encoded = manifest.to_dict()

    assert "object_ref" not in repr(encoded)
    assert StateCheckpointManifest.from_dict(encoded) == manifest


def test_manifest_reader_rejects_entry_without_scope():
    partition = StatePartition("job", "operator", 3)
    manifest = StateCheckpointManifest(
        job_id="job",
        checkpoint_id=1,
        epoch=0,
        entries=(
            StateCheckpointEntry(
                partition=partition,
                version=1,
                input_sequence=0,
                uri="file:///checkpoint.bin",
                checksum="sha256:abc",
                size_bytes=1,
            ),
        ),
    )
    encoded = manifest.to_dict()
    del encoded["entries"][0]["scope"]

    with pytest.raises(KeyError, match="scope"):
        StateCheckpointManifest.from_dict(encoded)
