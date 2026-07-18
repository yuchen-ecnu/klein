# SPDX-License-Identifier: Apache-2.0
"""Flink-style minimum-watermark and idle-input aggregation."""

from __future__ import annotations

from collections.abc import Hashable, Iterable
from dataclasses import dataclass

from ray.klein.runtime.message import InputActive, InputIdle, StreamControl, Watermark


@dataclass(slots=True)
class _InputState:
    watermark: int | None = None
    idle: bool = False


class InputWatermarkTracker:
    """Combine physical-input progress into one monotonic output protocol."""

    def __init__(self, input_ids: Iterable[Hashable] = ()) -> None:
        self._inputs = {input_id: _InputState() for input_id in input_ids}
        self._output_watermark = -1
        self._output_idle = False

    @property
    def current_watermark(self) -> int:
        return self._output_watermark

    @property
    def is_idle(self) -> bool:
        return self._output_idle

    @property
    def idle_input_count(self) -> int:
        return sum(1 for state in self._inputs.values() if state.idle)

    def on_control(
        self,
        sender: Hashable,
        control: StreamControl,
    ) -> tuple[StreamControl, ...]:
        state = self._inputs.setdefault(sender, _InputState())
        if isinstance(control, Watermark):
            return self._on_watermark(sender, state, control)
        if isinstance(control, InputIdle):
            return self._on_idle(state)
        if isinstance(control, InputActive):
            return self._on_active(state, control)
        raise TypeError(f"unsupported stream control: {type(control).__name__}")

    def _on_watermark(
        self,
        sender: Hashable,
        state: _InputState,
        watermark: Watermark,
    ) -> tuple[StreamControl, ...]:
        was_idle = state.idle
        state.idle = False
        is_newer = state.watermark is None or watermark.timestamp > state.watermark
        if is_newer:
            state.watermark = watermark.timestamp
        output = self._reactivate_output(self._output_idle or (was_idle and self._all_inputs_were_idle_except(sender)))
        if is_newer:
            output.extend(self._advance())
        return tuple(output)

    def _on_idle(self, state: _InputState) -> tuple[StreamControl, ...]:
        if state.idle:
            return ()
        state.idle = True
        if all(item.idle for item in self._inputs.values()):
            if self._output_idle:
                return ()
            self._output_idle = True
            return (InputIdle(),)
        return tuple(self._advance())

    def _on_active(
        self,
        state: _InputState,
        control: InputActive,
    ) -> tuple[StreamControl, ...]:
        state.idle = False
        if control.resume_watermark is not None:
            previous = -1 if state.watermark is None else state.watermark
            state.watermark = max(previous, control.resume_watermark)
        output = self._reactivate_output(self._output_idle)
        output.extend(self._advance())
        return tuple(output)

    def _reactivate_output(self, should_reactivate: bool) -> list[StreamControl]:
        if not should_reactivate:
            return []
        self._output_idle = False
        return [InputActive(self._resume_watermark())]

    def _advance(self) -> list[Watermark]:
        active = [state for state in self._inputs.values() if not state.idle]
        if not active or any(state.watermark is None for state in active):
            return []
        candidate = min(state.watermark for state in active if state.watermark is not None)
        if candidate <= self._output_watermark:
            return []
        self._output_watermark = candidate
        return [Watermark(candidate)]

    def _resume_watermark(self) -> int | None:
        return None if self._output_watermark < 0 else self._output_watermark

    def _all_inputs_were_idle_except(self, sender: Hashable) -> bool:
        return all(input_id == sender or state.idle for input_id, state in self._inputs.items())
