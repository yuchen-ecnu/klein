# SPDX-License-Identifier: Apache-2.0
import pickle
from typing import Generic, TypeVar

from ray.klein.state.state_serializer import StateSerializer

T = TypeVar("T")


class PickleStateSerializer(StateSerializer[T], Generic[T]):
    def dumps(self, value: T) -> bytes:
        return pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)

    def loads(self, value: bytes) -> T:
        return pickle.loads(value)
