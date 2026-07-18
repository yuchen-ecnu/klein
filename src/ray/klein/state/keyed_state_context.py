# SPDX-License-Identifier: Apache-2.0
from typing import Any

from ray.klein.state.list_state import ListState
from ray.klein.state.list_state_descriptor import ListStateDescriptor
from ray.klein.state.managed_state_backend import ManagedStateBackend
from ray.klein.state.map_state import MapState
from ray.klein.state.map_state_descriptor import MapStateDescriptor
from ray.klein.state.state_descriptor import StateDescriptor
from ray.klein.state.timer_service import TimerService
from ray.klein.state.value_state import ValueState
from ray.klein.state.value_state_descriptor import ValueStateDescriptor


class KeyedStateContext:
    def __init__(self, backend: ManagedStateBackend, timer_service: TimerService) -> None:
        self._backend = backend
        self._timer_service = timer_service
        self._timestamp: int | None = None

    @property
    def current_key(self) -> Any:
        return self._backend.current_key

    @property
    def timestamp(self) -> int | None:
        return self._timestamp

    @property
    def timer_service(self) -> TimerService:
        return self._timer_service

    def bind(self, key: Any, timestamp: int | None) -> "KeyedStateContext":
        self._backend.current_key = key
        self._timestamp = timestamp
        return self

    def state(self, descriptor: StateDescriptor, namespace: Any = None) -> ValueState | ListState | MapState:
        if isinstance(descriptor, ValueStateDescriptor):
            return ValueState(self._backend, descriptor, namespace)
        if isinstance(descriptor, ListStateDescriptor):
            return ListState(self._backend, descriptor, namespace)
        if isinstance(descriptor, MapStateDescriptor):
            return MapState(self._backend, descriptor, namespace)
        raise TypeError(f"unsupported state descriptor: {type(descriptor).__name__}")
