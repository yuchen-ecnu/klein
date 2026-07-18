# SPDX-License-Identifier: Apache-2.0
from dataclasses import dataclass
from datetime import timedelta

from ray.klein.state.state_ttl_update_type import StateTTLUpdateType
from ray.klein.state.state_visibility import StateVisibility


@dataclass(frozen=True, slots=True)
class StateTTLConfig:
    ttl: timedelta
    update_type: StateTTLUpdateType = StateTTLUpdateType.ON_CREATE_AND_WRITE
    visibility: StateVisibility = StateVisibility.NEVER_RETURN_EXPIRED

    def __post_init__(self) -> None:
        if self.ttl.total_seconds() <= 0:
            raise ValueError("state TTL must be positive")

    @property
    def ttl_milliseconds(self) -> int:
        return max(1, int(self.ttl.total_seconds() * 1000))
