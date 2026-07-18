# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from collections.abc import Callable
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from ray.klein.api.node_type import NodeType
from ray.klein.api.window_assigner import WindowAssigner
from ray.klein.runtime.operator.window_operator import WindowOperator
from ray.klein.runtime.resources import Resources

if TYPE_CHECKING:
    from ray.klein.api.data_stream import DataStream


class WindowedStream:
    """Builder for a keyed event-time window aggregation."""

    def __init__(
        self,
        stream: DataStream,
        key_selector: Callable[[dict[str, Any]], Any],
        timestamp_selector: Callable[[dict[str, Any]], int],
        assigner: WindowAssigner,
        allowed_lateness: timedelta,
        state_ttl: timedelta | None,
    ) -> None:
        self._stream = stream
        self._key_selector = key_selector
        self._timestamp_selector = timestamp_selector
        self._assigner = assigner
        self._allowed_lateness = allowed_lateness
        self._state_ttl = state_ttl

    def reduce(
        self,
        function: Callable[[dict[str, Any], dict[str, Any]], dict[str, Any]],
        *,
        num_cpus: float | None = None,
        num_gpus: float | None = None,
        concurrency: int | None = None,
        name: str = "WindowReduce",
    ) -> DataStream:
        from ray.klein.api.data_stream import DataStream

        resources = Resources(num_cpus, num_gpus, concurrency)
        return DataStream(
            self._stream,
            WindowOperator(
                key_selector=self._key_selector,
                timestamp_selector=self._timestamp_selector,
                assigner=self._assigner,
                reduce_function=function,
                allowed_lateness=self._allowed_lateness,
                state_ttl=self._state_ttl,
            ),
            name,
            NodeType.TRANSFORM,
            resources=resources,
        )
