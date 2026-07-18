# SPDX-License-Identifier: Apache-2.0
from typing import Generic, TypeVar

from ray.klein.state.state_descriptor import StateDescriptor

T = TypeVar("T")


class ListStateDescriptor(StateDescriptor[list[T]], Generic[T]):
    """Descriptor for a list per key and namespace."""
