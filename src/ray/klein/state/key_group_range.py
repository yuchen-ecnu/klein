# SPDX-License-Identifier: Apache-2.0
"""Stable key-group assignment used by keyed routing and checkpoint rescaling."""

from __future__ import annotations

import hashlib
import pickle
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, order=True, slots=True)
class KeyGroupRange:
    """Inclusive contiguous key-group range owned by one subtask."""

    start: int
    end: int

    def __post_init__(self) -> None:
        if self.start < 0:
            raise ValueError("key-group range start must be non-negative")
        if self.end < self.start:
            raise ValueError("key-group range end must be >= start")

    def __contains__(self, key_group: int) -> bool:
        return self.start <= key_group <= self.end

    def __iter__(self) -> Iterator[int]:
        return iter(range(self.start, self.end + 1))

    def __len__(self) -> int:
        return self.end - self.start + 1


def assign_key_group_range(
    max_parallelism: int,
    parallelism: int,
    subtask_index: int,
) -> KeyGroupRange:
    """Return the Flink-compatible contiguous range for one subtask."""

    _validate_parallelism(max_parallelism, parallelism)
    if isinstance(subtask_index, bool) or not isinstance(subtask_index, int):
        raise TypeError("subtask_index must be an integer")
    if not 0 <= subtask_index < parallelism:
        raise ValueError("subtask_index must be within parallelism")
    start = (subtask_index * max_parallelism + parallelism - 1) // parallelism
    end = ((subtask_index + 1) * max_parallelism + parallelism - 1) // parallelism - 1
    return KeyGroupRange(start, end)


def key_group_owner(key_group: int, max_parallelism: int, parallelism: int) -> int:
    """Map a key group to its owning subtask for the current parallelism."""

    _validate_parallelism(max_parallelism, parallelism)
    if isinstance(key_group, bool) or not isinstance(key_group, int):
        raise TypeError("key_group must be an integer")
    if not 0 <= key_group < max_parallelism:
        raise ValueError("key_group must be within max_parallelism")
    return (key_group * parallelism) // max_parallelism


def key_group_for_key(key: Any, max_parallelism: int) -> int:
    """Hash an arbitrary Python key into a process-independent key group.

    Python's built-in ``hash`` is salted per process and therefore cannot be
    used for distributed state ownership. A fixed pickle protocol plus BLAKE2b
    gives Klein deterministic routing across Ray workers and restarts.
    """

    if isinstance(max_parallelism, bool) or not isinstance(max_parallelism, int):
        raise TypeError("max_parallelism must be an integer")
    if max_parallelism < 1:
        raise ValueError("max_parallelism must be at least 1")
    try:
        encoded = pickle.dumps(key, protocol=4)
    except (pickle.PickleError, TypeError, AttributeError) as exc:
        raise TypeError("keyed stream keys must be pickle-serializable") from exc
    digest = hashlib.blake2b(encoded, digest_size=8, person=b"ray.klein").digest()
    return int.from_bytes(digest, byteorder="big", signed=False) % max_parallelism


def _validate_parallelism(max_parallelism: int, parallelism: int) -> None:
    for name, value in (("max_parallelism", max_parallelism), ("parallelism", parallelism)):
        if isinstance(value, bool) or not isinstance(value, int):
            raise TypeError(f"{name} must be an integer")
        if value < 1:
            raise ValueError(f"{name} must be at least 1")
    if parallelism > max_parallelism:
        raise ValueError("parallelism must not exceed max_parallelism")
