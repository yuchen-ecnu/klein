# SPDX-License-Identifier: Apache-2.0
from abc import ABC, abstractmethod
from typing import Generic, TypeVar

T = TypeVar("T")


class StateSerializer(ABC, Generic[T]):
    @abstractmethod
    def dumps(self, value: T) -> bytes:
        """Serialize a state value."""

    @abstractmethod
    def loads(self, value: bytes) -> T:
        """Deserialize a state value."""
