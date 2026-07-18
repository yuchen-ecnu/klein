# SPDX-License-Identifier: Apache-2.0
from collections.abc import Callable, Mapping
from typing import Any

from ray.klein.api.keyed_process_function import KeyedProcessFunction
from ray.klein.runtime.message import Record
from ray.klein.runtime.operator.managed_state_operator import ManagedStateOperator
from ray.klein.state.keyed_state_context import KeyedStateContext
from ray.klein.state.timer_event import TimerEvent


class KeyedProcessOperator(ManagedStateOperator):
    """Runs a user keyed process function against managed state."""

    def __init__(
        self,
        logical_function=None,
        *,
        key_selector: Callable[[Mapping[str, Any]], Any],
        process_function: KeyedProcessFunction,
        timestamp_selector: Callable[[Mapping[str, Any]], int] | None = None,
    ) -> None:
        super().__init__(
            logical_function,
            key_selector=key_selector,
            timestamp_selector=timestamp_selector,
        )
        self._process_function = process_function

    def process_managed_element(
        self,
        record: Record,
        context: KeyedStateContext,
    ) -> None:
        self.emit_result(self._process_function.process(record.block, context))

    def on_timer(self, event: TimerEvent, context: KeyedStateContext) -> None:
        self.emit_result(self._process_function.on_timer(event, context))

    def _spec_parameters(self) -> dict[str, Any]:
        return {
            "key_selector": self._key_selector,
            "process_function": self._process_function,
            "timestamp_selector": self._timestamp_selector,
        }
