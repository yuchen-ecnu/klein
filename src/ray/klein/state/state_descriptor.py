# SPDX-License-Identifier: Apache-2.0
from dataclasses import dataclass, field
from typing import Generic, TypeVar

from ray.klein.state.pickle_state_serializer import PickleStateSerializer
from ray.klein.state.state_serializer import StateSerializer
from ray.klein.state.state_ttl_config import StateTTLConfig

T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class StateDescriptor(Generic[T]):
    name: str
    serializer: StateSerializer[T] = field(default_factory=PickleStateSerializer)
    ttl_config: StateTTLConfig | None = None

    def __post_init__(self) -> None:
        if not self.name or not self.name.strip():
            raise ValueError("state descriptor name must not be empty")
