# SPDX-License-Identifier: Apache-2.0
from collections.abc import Callable, Mapping
from datetime import timedelta
from typing import Any

from ray.klein.api.time_window import TimeWindow
from ray.klein.api.window_assigner import WindowAssigner
from ray.klein.runtime.message import Record
from ray.klein.runtime.operator.managed_state_operator import ManagedStateOperator
from ray.klein.state.keyed_state_context import KeyedStateContext
from ray.klein.state.state_ttl_config import StateTTLConfig
from ray.klein.state.timer_domain import TimerDomain
from ray.klein.state.timer_event import TimerEvent
from ray.klein.state.value_state_descriptor import ValueStateDescriptor


class WindowOperator(ManagedStateOperator):
    """Managed event-time tumbling, sliding, or session window aggregation."""

    def __init__(
        self,
        logical_function=None,
        *,
        key_selector: Callable[[Mapping[str, Any]], Any],
        timestamp_selector: Callable[[Mapping[str, Any]], int],
        assigner: WindowAssigner,
        reduce_function: Callable[[Any, Any], Any],
        allowed_lateness: timedelta = timedelta(0),
        state_ttl: timedelta | None = None,
    ) -> None:
        super().__init__(
            logical_function,
            key_selector=key_selector,
            timestamp_selector=timestamp_selector,
        )
        self._assigner = assigner
        self._reduce_function = reduce_function
        if allowed_lateness < timedelta(0):
            raise ValueError("allowed_lateness must not be negative")
        self._allowed_lateness_ms = int(allowed_lateness.total_seconds() * 1000)
        ttl_config = StateTTLConfig(state_ttl) if state_ttl is not None else None
        self._window_state = ValueStateDescriptor(
            "window-aggregate",
            ttl_config=ttl_config,
        )

    def process_managed_element(
        self,
        record: Record,
        context: KeyedStateContext,
    ) -> None:
        timestamp = context.timestamp
        if timestamp is None or timestamp < 0:
            raise ValueError("window timestamp_selector must return a non-negative integer")
        dropped_from_window = False
        for window in self._assigner.assign_windows(timestamp):
            target, overlapping = self._session_merge_plan(window)
            cleanup_timestamp = target.end - 1 + self._allowed_lateness_ms
            if cleanup_timestamp <= self.timer_service.current_watermark:
                dropped_from_window = True
                continue
            self._merge_session_windows(target, overlapping, context)
            state = context.state(self._window_state, target)
            current = state.value
            state.value = dict(record.block) if current is None else self._reduce_function(current, record.block)
            context.timer_service.register_event_time_timer(
                target.end - 1 + self._allowed_lateness_ms,
                target,
            )
        if dropped_from_window:
            self._record_late_drop()

    def on_timer(self, event: TimerEvent, context: KeyedStateContext) -> None:
        if event.domain != TimerDomain.EVENT_TIME:
            return
        state = context.state(self._window_state, event.namespace)
        result = state.value
        if result is None:
            return
        state.clear()
        self.emit_result(result)

    def finish(self) -> None:
        for event in self._backend.pop_due_timers(
            (1 << 63) - 1,
            TimerDomain.EVENT_TIME,
        ):
            self._fire_timer(event)

    def _session_merge_plan(
        self,
        candidate: TimeWindow,
    ) -> tuple[TimeWindow, tuple[TimeWindow, ...]]:
        if not self._assigner.is_merging:
            return candidate, ()
        overlapping = tuple(
            window for window in self._backend.namespaces(self._window_state) if window.overlaps(candidate)
        )
        if not overlapping:
            return candidate, ()
        merged = candidate
        for window in overlapping:
            merged = merged.merge(window)
        return merged, overlapping

    def _merge_session_windows(
        self,
        target: TimeWindow,
        overlapping: tuple[TimeWindow, ...],
        context: KeyedStateContext,
    ) -> None:
        accumulated = None
        for window in overlapping:
            state = context.state(self._window_state, window)
            value = state.value
            if value is not None:
                accumulated = value if accumulated is None else self._reduce_function(accumulated, value)
            state.clear()
            context.timer_service.delete_event_time_timer(
                window.end - 1 + self._allowed_lateness_ms,
                window,
            )
        if accumulated is not None:
            context.state(self._window_state, target).value = accumulated

    def _spec_parameters(self) -> dict[str, Any]:
        ttl = self._window_state.ttl_config
        return {
            "key_selector": self._key_selector,
            "timestamp_selector": self._timestamp_selector,
            "assigner": self._assigner,
            "reduce_function": self._reduce_function,
            "allowed_lateness": timedelta(milliseconds=self._allowed_lateness_ms),
            "state_ttl": None if ttl is None else ttl.ttl,
        }
