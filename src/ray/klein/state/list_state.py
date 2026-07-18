# SPDX-License-Identifier: Apache-2.0
from collections.abc import Iterable, MutableSequence
from typing import Generic, TypeVar, overload

from ray.klein.state.list_state_descriptor import ListStateDescriptor
from ray.klein.state.managed_state_backend import ManagedStateBackend

T = TypeVar("T")


class ListState(MutableSequence[T], Generic[T]):
    def __init__(
        self,
        backend: ManagedStateBackend,
        descriptor: ListStateDescriptor[T],
        namespace=None,
    ) -> None:
        self._backend = backend
        self._descriptor = descriptor
        self._namespace = namespace

    def _values(self) -> list[T]:
        return list(self._backend.get(self._descriptor, self._namespace) or ())

    @overload
    def __getitem__(self, index: int) -> T: ...

    @overload
    def __getitem__(self, index: slice) -> list[T]: ...

    def __getitem__(self, index: int | slice) -> T | list[T]:
        return self._values()[index]

    def __setitem__(self, index: int | slice, value: T | Iterable[T]) -> None:
        values = self._values()
        values[index] = value
        self._backend.put(self._descriptor, values, self._namespace)

    def __delitem__(self, index: int | slice) -> None:
        values = self._values()
        del values[index]
        if values:
            self._backend.put(self._descriptor, values, self._namespace)
        else:
            self.clear()

    def __len__(self) -> int:
        return len(self._values())

    def insert(self, index: int, value: T) -> None:
        values = self._values()
        values.insert(index, value)
        self._backend.put(self._descriptor, values, self._namespace)

    def extend(self, values: Iterable[T]) -> None:
        current = self._values()
        current.extend(values)
        self._backend.put(self._descriptor, current, self._namespace)

    def clear(self) -> None:
        self._backend.delete(self._descriptor, self._namespace)
