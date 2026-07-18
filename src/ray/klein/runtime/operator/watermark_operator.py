# SPDX-License-Identifier: Apache-2.0
"""Operator that turns a WatermarkStrategy into ordered control messages."""

from __future__ import annotations

import time
from typing import Any

from ray.klein.api.watermark_strategy import WatermarkStrategy
from ray.klein.runtime.message import InputActive, InputIdle, Record, Watermark
from ray.klein.runtime.operator.operator import OneInputOperator, StreamOperator


class WatermarkOperator(StreamOperator, OneInputOperator):
    def __init__(self, logical_function=None, *, strategy: WatermarkStrategy) -> None:
        super().__init__(logical_function)
        self._strategy = strategy
        self._max_timestamp = -1
        self._last_emitted_watermark = -1
        self._last_record_monotonic = time.monotonic()
        self._idle = False

    def process_element(self, record: Record) -> None:
        timestamp = self._strategy.timestamp_assigner(record.block)
        if isinstance(timestamp, bool) or not isinstance(timestamp, int) or timestamp < 0:
            raise ValueError("watermark timestamp assigners must return a non-negative integer")
        if self._idle:
            resume = None if self._last_emitted_watermark < 0 else self._last_emitted_watermark
            self.collect(InputActive(resume))
            self._idle = False
        self._last_record_monotonic = time.monotonic()
        record.timestamp = timestamp
        self.collect(record)
        self._max_timestamp = max(self._max_timestamp, timestamp)
        candidate = self._max_timestamp - self._strategy.max_out_of_orderness_ms
        if candidate >= 0 and candidate > self._last_emitted_watermark:
            self.collect(Watermark(candidate))
            self._last_emitted_watermark = candidate

    def on_idle(self) -> None:
        timeout = self._strategy.idle_timeout_ms
        if timeout is None or self._idle:
            return
        if (time.monotonic() - self._last_record_monotonic) * 1000 >= timeout:
            self.collect(InputIdle())
            self._idle = True

    def on_input_idle(self) -> None:
        self._idle = True

    def on_input_active(self) -> None:
        self._idle = False
        self._last_record_monotonic = time.monotonic()

    def _spec_parameters(self) -> dict[str, Any]:
        return {"strategy": self._strategy}
