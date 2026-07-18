# SPDX-License-Identifier: Apache-2.0
import pytest

from ray.klein.state.key_group_range import (
    assign_key_group_range,
    key_group_for_key,
    key_group_owner,
)


@pytest.mark.parametrize("parallelism", [1, 2, 3, 7, 16])
def test_key_group_ranges_are_contiguous_complete_and_match_owner(parallelism):
    max_parallelism = 16
    ranges = [assign_key_group_range(max_parallelism, parallelism, index) for index in range(parallelism)]

    assert [group for owned in ranges for group in owned] == list(range(max_parallelism))
    for index, owned in enumerate(ranges):
        assert all(key_group_owner(group, max_parallelism, parallelism) == index for group in owned)


def test_key_group_hash_is_stable_and_max_parallelism_is_validated():
    assert key_group_for_key("customer-1", 128) == key_group_for_key("customer-1", 128)
    with pytest.raises(ValueError, match="must not exceed"):
        assign_key_group_range(2, 3, 0)
