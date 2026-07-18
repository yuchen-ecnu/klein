# SPDX-License-Identifier: Apache-2.0
"""Small picklable immutable mapping used by frozen value objects."""

from collections.abc import Iterable, Iterator, Mapping
from typing import Generic, TypeVar

K = TypeVar("K")
V = TypeVar("V")


class FrozenMapping(Mapping[K, V], Generic[K, V]):
    """A defensive, read-only copy of mapping data.

    ``MappingProxyType`` is not picklable, while these values cross Ray process
    boundaries. This class keeps the same read-only contract and has an
    explicit pickle reconstruction path.
    """

    __slots__ = ("_data",)

    def __init__(self, values: Mapping[K, V] | Iterable[tuple[K, V]] = ()) -> None:
        self._data = dict(values)

    def __getitem__(self, key: K) -> V:
        return self._data[key]

    def __iter__(self) -> Iterator[K]:
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self._data!r})"

    def __reduce__(self) -> tuple[type["FrozenMapping[K, V]"], tuple[dict[K, V]]]:
        return type(self), (self._data,)
