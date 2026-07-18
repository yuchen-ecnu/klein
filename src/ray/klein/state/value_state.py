# SPDX-License-Identifier: Apache-2.0
from typing import Generic, TypeVar

from ray.klein.state.managed_state_backend import ManagedStateBackend
from ray.klein.state.value_state_descriptor import ValueStateDescriptor

T = TypeVar("T")


class ValueState(Generic[T]):
    def __init__(
        self,
        backend: ManagedStateBackend,
        descriptor: ValueStateDescriptor[T],
        namespace=None,
    ) -> None:
        self._backend = backend
        self._descriptor = descriptor
        self._namespace = namespace

    @property
    def value(self) -> T | None:
        return self._backend.get(self._descriptor, self._namespace)

    @value.setter
    def value(self, value: T) -> None:
        self._backend.put(self._descriptor, value, self._namespace)

    def clear(self) -> None:
        self._backend.delete(self._descriptor, self._namespace)
