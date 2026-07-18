# SPDX-License-Identifier: Apache-2.0
from dataclasses import dataclass

from ray.klein.state.state_handle import StateHandle


@dataclass(frozen=True, slots=True)
class StateSnapshot:
    """Hot checkpoint whose handles are pinned by a registry actor."""

    checkpoint_id: int
    epoch: int
    handles: tuple[StateHandle, ...]

    def __post_init__(self) -> None:
        if self.checkpoint_id < 0:
            raise ValueError("checkpoint_id must be non-negative")
        if self.epoch < 0:
            raise ValueError("epoch must be non-negative")
