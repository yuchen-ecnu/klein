# SPDX-License-Identifier: Apache-2.0
from pathlib import Path

import pytest

from ray.klein.runtime.coordinator import checkpoint_io


def test_checkpoint_persists_and_verifies_operator_state(tmp_path: Path):
    checkpoint = checkpoint_io.write_checkpoint(
        [],
        3,
        tmp_path.as_uri(),
        barrier_high_water=17,
        job_id="job-a",
        operator_states={
            "2:0": b"key-groups-0-63",
            "2:1": b"key-groups-64-127",
        },
    )

    assert checkpoint_io.restore_checkpoint(checkpoint) == (3, [], 17)
    entries = checkpoint_io.restore_operator_state_entries(checkpoint)
    assert set(entries) == {"2:0", "2:1"}
    assert checkpoint_io.read_operator_state(checkpoint, entries["2:0"]) == b"key-groups-0-63"
    assert checkpoint_io.read_operator_state(checkpoint, entries["2:1"]) == b"key-groups-64-127"


def test_operator_state_checksum_detects_corruption(tmp_path: Path):
    checkpoint = checkpoint_io.write_checkpoint(
        [],
        1,
        tmp_path.as_uri(),
        job_id="job-a",
        operator_states={"2:0": b"valid"},
    )
    entry = checkpoint_io.restore_operator_state_entries(checkpoint)["2:0"]
    Path(entry.uri.removeprefix("file://")).write_bytes(b"broken")

    with pytest.raises(ValueError, match="size mismatch"):
        checkpoint_io.read_operator_state(checkpoint, entry)
