# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from collections.abc import Callable
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from ray.klein.api.keyed_process_function import KeyedProcessFunction
from ray.klein.api.node_type import NodeType
from ray.klein.runtime.operator.keyed_process_operator import KeyedProcessOperator
from ray.klein.runtime.resources import Resources
from ray.klein.state.keyed_state_context import KeyedStateContext

if TYPE_CHECKING:
    from ray.klein.api.data_stream import DataStream
    from ray.klein.api.window_assigner import WindowAssigner
    from ray.klein.api.windowed_stream import WindowedStream


class KeyedStream:
    """A hash-partitioned stream ready for managed-state transformations."""

    def __init__(
        self,
        stream: DataStream,
        key_selector: Callable[[dict[str, Any]], Any],
    ) -> None:
        self._stream = stream
        self._key_selector = key_selector

    @property
    def stream(self) -> DataStream:
        return self._stream

    def process(
        self,
        function: KeyedProcessFunction | Callable[[dict[str, Any], KeyedStateContext], Any],
        *,
        timestamp_selector: Callable[[dict[str, Any]], int] | None = None,
        num_cpus: float | None = None,
        num_gpus: float | None = None,
        concurrency: int | None = None,
        name: str = "KeyedProcess",
    ) -> DataStream:
        from ray.klein.api.data_stream import DataStream

        if not isinstance(function, KeyedProcessFunction):
            if not callable(function):
                raise TypeError("function must be a KeyedProcessFunction or callable")
            function = _CallableKeyedProcessFunction(function)
        resources = Resources(num_cpus, num_gpus, concurrency)
        return DataStream(
            self._stream,
            KeyedProcessOperator(
                key_selector=self._key_selector,
                process_function=function,
                timestamp_selector=timestamp_selector,
            ),
            name,
            NodeType.TRANSFORM,
            resources=resources,
        )

    def window(
        self,
        assigner: WindowAssigner,
        *,
        timestamp_selector: Callable[[dict[str, Any]], int],
        allowed_lateness: timedelta = timedelta(0),
        state_ttl: timedelta | None = None,
    ) -> WindowedStream:
        from ray.klein.api.windowed_stream import WindowedStream

        return WindowedStream(
            self._stream,
            self._key_selector,
            timestamp_selector,
            assigner,
            allowed_lateness,
            state_ttl,
        )


class _CallableKeyedProcessFunction(KeyedProcessFunction):
    """Give a plain process callable the same explicit runtime contract."""

    def __init__(self, function: Callable[[dict[str, Any], KeyedStateContext], Any]) -> None:
        self._function = function

    def process(self, value: dict[str, Any], context: KeyedStateContext) -> Any:
        return self._function(value, context)
