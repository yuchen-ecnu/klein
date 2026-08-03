# SPDX-License-Identifier: Apache-2.0
import os
import pickle
import subprocess
import sys
from array import array
from decimal import Decimal
from fractions import Fraction

import pytest

from ray.klein.state.key_encoding import encode_key
from ray.klein.state.key_group_range import (
    assign_key_group_range,
    key_group_for_key,
    key_group_owner,
)


class _DistinctInt(int):
    def __eq__(self, other):
        return type(other) is type(self) and int(self) == int(other)

    def __ne__(self, other):
        return not self == other

    __hash__ = int.__hash__


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


@pytest.mark.parametrize("key", [None, "customer-1", b"tenant", 7, 0.5, ("tenant", 7)])
def test_canonical_representatives_keep_the_legacy_routing_encoding(key):
    assert encode_key(key, protocol=4) == pickle.dumps(key, protocol=4)


@pytest.mark.parametrize(
    "equal_keys",
    [
        (1, 1.0, True, Decimal("1"), Fraction(1, 1), 1 + 0j),
        (0.5, Decimal("0.5"), Fraction(1, 2)),
        (
            {"tenant": 7, "window": [1, 2]},
            {"window": [1.0, 2.0], "tenant": Decimal("7")},
        ),
        (frozenset({"tenant", 7}), frozenset({7.0, "tenant"})),
        (range(0, 3, 2), range(0, 4, 2)),
        (range(0, 10**100, 2), range(0, 10**100 - 1, 2)),
    ],
)
def test_equal_python_keys_share_a_key_group(equal_keys):
    assert all(candidate == equal_keys[0] for candidate in equal_keys[1:])
    assert len({key_group_for_key(candidate, 32768) for candidate in equal_keys}) == 1


def test_unordered_key_hash_is_stable_across_python_hash_seeds():
    script = (
        "from ray.klein.state.key_group_range import key_group_for_key;"
        "print(key_group_for_key(frozenset({'alpha', 'beta', 'gamma'}), 32768))"
    )
    values = []
    for seed in ("1", "2", "random"):
        environment = os.environ.copy()
        environment["PYTHONHASHSEED"] = seed
        values.append(subprocess.check_output([sys.executable, "-c", script], env=environment, text=True).strip())

    assert len(set(values)) == 1


def test_byte_equivalent_memoryview_uses_the_bytes_equality_class():
    view = memoryview(b"tenant")

    assert view == b"tenant"
    assert encode_key(view) == encode_key(b"tenant")


def test_format_sensitive_memoryview_is_rejected_instead_of_aliasing_bytes():
    view = memoryview(array("I", [1]))
    payload = bytes(view)

    assert view != payload
    with pytest.raises(TypeError, match="memoryview"):
        encode_key(view)


def test_custom_numeric_subclass_falls_back_to_opaque_pickle_identity():
    key = _DistinctInt(1)

    assert key != 1
    assert encode_key(key, protocol=4) == pickle.dumps(key, protocol=4)
    assert encode_key(key, protocol=4) != encode_key(1, protocol=4)


@pytest.mark.parametrize(
    "key",
    [
        float("nan"),
        Decimal("NaN"),
        complex(float("nan"), 0),
        complex(0, float("nan")),
        ("nested", float("nan")),
        {"value": Decimal("sNaN")},
        frozenset({float("nan")}),
    ],
)
def test_nan_keys_are_rejected_recursively(key):
    with pytest.raises(TypeError, match="NaN"):
        encode_key(key)


def test_complex_signed_zero_components_have_one_equality_encoding():
    negative_real_zero = complex(-0.0, 1.0)
    positive_real_zero = complex(0.0, 1.0)
    negative_imaginary_zero = complex(1.0, -0.0)

    assert negative_real_zero == positive_real_zero
    assert encode_key(negative_real_zero) == encode_key(positive_real_zero)
    assert negative_imaginary_zero == 1
    assert encode_key(negative_imaginary_zero) == encode_key(1)
