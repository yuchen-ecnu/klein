# SPDX-License-Identifier: Apache-2.0
from dataclasses import dataclass
from typing import Any

from ray.klein.state.timer_domain import TimerDomain


@dataclass(frozen=True, slots=True)
class TimerEvent:
    timestamp: int
    key: Any
    namespace: Any
    domain: TimerDomain
