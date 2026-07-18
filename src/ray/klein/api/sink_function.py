# SPDX-License-Identifier: Apache-2.0
from abc import ABC, abstractmethod
from typing import Any

from ray.klein.api.function import Function


class SinkFunction(Function, ABC):
    """Interface for implementing user defined sink functionality."""

    @abstractmethod
    def write(self, value: dict[str, Any]) -> None:
        """Writes the given value to the sink. This function is called for every
        record. The function is mutually exclusive with function 'flush'"""

    def flush(self) -> None:
        """Flush buffered records to the external system, if applicable."""
