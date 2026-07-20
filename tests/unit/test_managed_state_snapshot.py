# SPDX-License-Identifier: Apache-2.0
import pickle

import pytest

from ray.klein.state.key_group_range import KeyGroupRange, assign_key_group_range, key_group_owner
from ray.klein.state.managed_state_snapshot import repartition_managed_state_snapshots


def _snapshot(
    key_groups: dict[int, bytes],
    *,
    max_parallelism: int = 8,
    watermark: int = -1,
) -> bytes:
    return pickle.dumps(
        {
            "format_version": 2,
            "max_parallelism": max_parallelism,
            "key_group_range": KeyGroupRange(0, max_parallelism - 1),
            "key_groups": key_groups,
            "watermark": watermark,
        },
        protocol=pickle.HIGHEST_PROTOCOL,
    )


def test_repartition_decodes_old_fragments_once_and_assigns_each_group_to_one_target() -> None:
    shards = repartition_managed_state_snapshots(
        (
            _snapshot({0: b"zero", 1: b"one", 2: b"two", 3: b"three"}, watermark=40),
            _snapshot({4: b"four", 5: b"five", 6: b"six", 7: b"seven"}, watermark=35),
        ),
        target_parallelism=3,
    )

    assert len(shards) == 3
    restored_groups = {}
    for index, shard in enumerate(shards):
        payload = pickle.loads(shard)
        assert payload["key_group_range"] == assign_key_group_range(8, 3, index)
        assert payload["watermark"] == 35
        assert all(key_group_owner(group, 8, 3) == index for group in payload["key_groups"])
        restored_groups.update(payload["key_groups"])
    assert restored_groups == {
        0: b"zero",
        1: b"one",
        2: b"two",
        3: b"three",
        4: b"four",
        5: b"five",
        6: b"six",
        7: b"seven",
    }


def test_repartition_rejects_inconsistent_or_conflicting_fragments() -> None:
    with pytest.raises(ValueError, match="inconsistent max_parallelism"):
        repartition_managed_state_snapshots(
            (_snapshot({0: b"left"}), _snapshot({1: b"right"}, max_parallelism=16)),
            2,
        )

    with pytest.raises(ValueError, match="conflicting key group 0"):
        repartition_managed_state_snapshots(
            (_snapshot({0: b"left"}), _snapshot({0: b"right"})),
            2,
        )


@pytest.mark.parametrize("target_parallelism", [0, 9])
def test_repartition_rejects_invalid_target_parallelism(target_parallelism: int) -> None:
    with pytest.raises(ValueError):
        repartition_managed_state_snapshots((_snapshot({}),), target_parallelism)
