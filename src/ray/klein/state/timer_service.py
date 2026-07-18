# SPDX-License-Identifier: Apache-2.0
import time

from ray.klein.state.managed_state_backend import ManagedStateBackend
from ray.klein.state.timer_domain import TimerDomain
from ray.klein.state.timer_event import TimerEvent


class TimerService:
    def __init__(self, backend: ManagedStateBackend) -> None:
        self._backend = backend
        self._watermark = -1

    @property
    def current_processing_time(self) -> int:
        return int(time.time() * 1000)

    @property
    def current_watermark(self) -> int:
        return self._watermark

    def advance_watermark(self, timestamp: int) -> tuple[TimerEvent, ...]:
        if timestamp <= self._watermark:
            return ()
        self._watermark = timestamp
        return self._backend.pop_due_timers(timestamp, TimerDomain.EVENT_TIME)

    def restore_watermark(self, timestamp: int) -> None:
        if timestamp < -1:
            raise ValueError("watermark must be -1 or non-negative")
        self._watermark = timestamp

    def register_event_time_timer(self, timestamp: int, namespace=None) -> None:
        self._backend.register_timer(timestamp, namespace, TimerDomain.EVENT_TIME)

    def delete_event_time_timer(self, timestamp: int, namespace=None) -> None:
        self._backend.delete_timer(timestamp, namespace, TimerDomain.EVENT_TIME)

    def register_processing_time_timer(self, timestamp: int, namespace=None) -> None:
        self._backend.register_timer(timestamp, namespace, TimerDomain.PROCESSING_TIME)

    def delete_processing_time_timer(self, timestamp: int, namespace=None) -> None:
        self._backend.delete_timer(timestamp, namespace, TimerDomain.PROCESSING_TIME)

    def pop_due_processing_time_timers(self, timestamp: int | None = None) -> tuple[TimerEvent, ...]:
        timestamp = self.current_processing_time if timestamp is None else timestamp
        return self._backend.pop_due_timers(timestamp, TimerDomain.PROCESSING_TIME)
