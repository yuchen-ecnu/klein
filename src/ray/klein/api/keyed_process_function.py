# SPDX-License-Identifier: Apache-2.0
from abc import ABC, abstractmethod
from typing import Any

from ray.klein.state.keyed_state_context import KeyedStateContext
from ray.klein.state.timer_event import TimerEvent


class KeyedProcessFunction(ABC):
    """User function with keyed state and event/processing-time timers."""

    @abstractmethod
    def process(self, value: dict, context: KeyedStateContext) -> Any:
        """Process one keyed record."""

    def on_timer(
        self,
        event: TimerEvent,
        context: KeyedStateContext,
    ) -> Any:
        return None
