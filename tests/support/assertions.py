# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from typing import Any


def assert_rows_equal(
    actual: Sequence[Mapping[str, Any]],
    expected: Sequence[Mapping[str, Any]],
    *,
    order_sensitive: bool = True,
) -> None:
    """Compare row collections with useful pytest assertion diffs."""

    if order_sensitive:
        assert list(actual) == list(expected)
        return
    assert Counter(_freeze(row) for row in actual) == Counter(_freeze(row) for row in expected)


def _freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return tuple(sorted((key, _freeze(item)) for key, item in value.items()))
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(item) for item in value)
    if isinstance(value, set):
        return frozenset(_freeze(item) for item in value)
    return value
