# SPDX-License-Identifier: Apache-2.0
"""Low-overhead estimates for memory retained by data-plane objects."""

from __future__ import annotations

import sys
from collections import deque
from collections.abc import Mapping
from typing import Any


def estimate_retained_size(value: Any, seen: set[int] | None = None) -> int:
    """Estimate bytes kept alive by ``value`` without serializing it.

    Array-like objects expose their payload through ``nbytes``. Containers are
    traversed recursively while shared objects are counted once. The estimate
    is intentionally cheap and conservative enough for admission control; it is
    not a replacement for a heap profiler.
    """
    if seen is None:
        seen = set()
    identity = id(value)
    if identity in seen:
        return 0
    seen.add(identity)

    nbytes = getattr(value, "nbytes", None)
    if isinstance(nbytes, int):
        return max(0, nbytes)
    block = getattr(value, "block", None)
    if block is not None:
        return sys.getsizeof(value) + estimate_retained_size(block, seen)
    if isinstance(value, Mapping):
        return sys.getsizeof(value) + sum(
            estimate_retained_size(key, seen) + estimate_retained_size(item, seen) for key, item in value.items()
        )
    if isinstance(value, (tuple, list, set, frozenset, deque)):
        return sys.getsizeof(value) + sum(estimate_retained_size(item, seen) for item in value)
    return sys.getsizeof(value)
