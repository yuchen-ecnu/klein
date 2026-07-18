# SPDX-License-Identifier: Apache-2.0
from typing import Generic, TypeVar

from ray.klein.state.state_descriptor import StateDescriptor

K = TypeVar("K")
V = TypeVar("V")


class MapStateDescriptor(StateDescriptor[dict[K, V]], Generic[K, V]):
    """Descriptor for a map per key and namespace."""
