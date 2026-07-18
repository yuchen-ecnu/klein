# SPDX-License-Identifier: Apache-2.0
from collections.abc import Callable, Mapping
from datetime import timedelta
from typing import Any

from ray.klein.runtime.message import Record
from ray.klein.runtime.operator.managed_state_operator import ManagedStateOperator
from ray.klein.state.keyed_state_context import KeyedStateContext
from ray.klein.state.list_state_descriptor import ListStateDescriptor
from ray.klein.state.state_ttl_config import StateTTLConfig
from ray.klein.state.timer_domain import TimerDomain
from ray.klein.state.timer_event import TimerEvent


class IntervalJoinOperator(ManagedStateOperator):
    """Keyed two-stream event-time interval join with bounded managed state."""

    def __init__(
        self,
        logical_function=None,
        *,
        left_key: Callable[[Mapping[str, Any]], Any],
        right_key: Callable[[Mapping[str, Any]], Any],
        left_timestamp: Callable[[Mapping[str, Any]], int],
        right_timestamp: Callable[[Mapping[str, Any]], int],
        lower_bound: timedelta,
        upper_bound: timedelta,
        join_function: Callable[[Mapping[str, Any], Mapping[str, Any]], Any],
        allowed_lateness: timedelta = timedelta(0),
        state_ttl: timedelta | None = None,
    ) -> None:
        self._left_key = left_key
        self._right_key = right_key
        self._left_timestamp = left_timestamp
        self._right_timestamp = right_timestamp
        self._lower_ms = int(lower_bound.total_seconds() * 1000)
        self._upper_ms = int(upper_bound.total_seconds() * 1000)
        self._allowed_lateness_ms = int(allowed_lateness.total_seconds() * 1000)
        if self._lower_ms > self._upper_ms:
            raise ValueError("lower_bound must not exceed upper_bound")
        if self._allowed_lateness_ms < 0:
            raise ValueError("allowed_lateness must not be negative")
        self._join_function = join_function
        ttl = StateTTLConfig(state_ttl) if state_ttl is not None else None
        self._left_state = ListStateDescriptor("interval-join-left", ttl_config=ttl)
        self._right_state = ListStateDescriptor("interval-join-right", ttl_config=ttl)
        super().__init__(
            logical_function,
            key_selector=left_key,
            timestamp_selector=left_timestamp,
        )

    def process_managed_element(
        self,
        record: Record,
        context: KeyedStateContext,
    ) -> None:
        if record.input_tag not in {0, 1}:
            raise ValueError("interval join record is missing its left/right input tag")
        timestamp = context.timestamp
        if timestamp is None or timestamp < 0:
            raise ValueError("join timestamp selectors must return non-negative integers")
        if record.input_tag == 0:
            self._join_left(record.block, timestamp, context)
        else:
            self._join_right(record.block, timestamp, context)

    def _join_left(
        self,
        left: Mapping[str, Any],
        timestamp: int,
        context: KeyedStateContext,
    ) -> None:
        cleanup_timestamp = timestamp + self._upper_ms + self._allowed_lateness_ms + 1
        if cleanup_timestamp <= self.timer_service.current_watermark:
            self._record_late_drop()
            return
        for right_timestamp in self._backend.namespaces(self._right_state):
            delta = right_timestamp - timestamp
            if not self._lower_ms <= delta <= self._upper_ms:
                continue
            for right in context.state(self._right_state, right_timestamp):
                self.emit_result(self._join_function(left, right))
        context.state(self._left_state, timestamp).append(dict(left))
        context.timer_service.register_event_time_timer(
            cleanup_timestamp,
            ("left", timestamp),
        )

    def _join_right(
        self,
        right: Mapping[str, Any],
        timestamp: int,
        context: KeyedStateContext,
    ) -> None:
        cleanup_timestamp = max(0, timestamp - self._lower_ms + self._allowed_lateness_ms + 1)
        if cleanup_timestamp <= self.timer_service.current_watermark:
            self._record_late_drop()
            return
        for left_timestamp in self._backend.namespaces(self._left_state):
            delta = timestamp - left_timestamp
            if not self._lower_ms <= delta <= self._upper_ms:
                continue
            for left in context.state(self._left_state, left_timestamp):
                self.emit_result(self._join_function(left, right))
        context.state(self._right_state, timestamp).append(dict(right))
        context.timer_service.register_event_time_timer(
            cleanup_timestamp,
            ("right", timestamp),
        )

    def on_timer(self, event: TimerEvent, context: KeyedStateContext) -> None:
        if event.domain != TimerDomain.EVENT_TIME:
            return
        side, timestamp = event.namespace
        descriptor = self._left_state if side == "left" else self._right_state
        context.state(descriptor, timestamp).clear()

    def _key_and_timestamp(self, record: Record) -> tuple[Any, int]:
        if record.input_tag == 0:
            return self._left_key(record.block), self._left_timestamp(record.block)
        if record.input_tag == 1:
            return self._right_key(record.block), self._right_timestamp(record.block)
        raise ValueError("interval join record is missing its left/right input tag")

    def _spec_parameters(self) -> dict[str, Any]:
        ttl = self._left_state.ttl_config
        return {
            "left_key": self._left_key,
            "right_key": self._right_key,
            "left_timestamp": self._left_timestamp,
            "right_timestamp": self._right_timestamp,
            "lower_bound": timedelta(milliseconds=self._lower_ms),
            "upper_bound": timedelta(milliseconds=self._upper_ms),
            "join_function": self._join_function,
            "allowed_lateness": timedelta(milliseconds=self._allowed_lateness_ms),
            "state_ttl": None if ttl is None else ttl.ttl,
        }
