# SPDX-License-Identifier: Apache-2.0
from collections.abc import Iterator, MutableMapping
from typing import Generic, TypeVar

from ray.klein.state.managed_state_backend import ManagedStateBackend
from ray.klein.state.map_state_descriptor import MapStateDescriptor

K = TypeVar("K")
V = TypeVar("V")


class MapState(MutableMapping[K, V], Generic[K, V]):
    def __init__(
        self,
        backend: ManagedStateBackend,
        descriptor: MapStateDescriptor[K, V],
        namespace=None,
    ) -> None:
        self._backend = backend
        self._descriptor = descriptor
        self._namespace = namespace

    def _values(self) -> dict[K, V]:
        return dict(self._backend.get(self._descriptor, self._namespace) or {})

    def __getitem__(self, key: K) -> V:
        return self._values()[key]

    def __setitem__(self, key: K, value: V) -> None:
        values = self._values()
        values[key] = value
        self._backend.put(self._descriptor, values, self._namespace)

    def __delitem__(self, key: K) -> None:
        values = self._values()
        del values[key]
        if values:
            self._backend.put(self._descriptor, values, self._namespace)
        else:
            self.clear()

    def __iter__(self) -> Iterator[K]:
        return iter(self._values())

    def __len__(self) -> int:
        return len(self._values())

    def clear(self) -> None:
        self._backend.delete(self._descriptor, self._namespace)
