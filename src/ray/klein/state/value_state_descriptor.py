# SPDX-License-Identifier: Apache-2.0
from typing import Generic, TypeVar

from ray.klein.state.state_descriptor import StateDescriptor

T = TypeVar("T")


class ValueStateDescriptor(StateDescriptor[T], Generic[T]):
    """Descriptor for one value per key and namespace."""
