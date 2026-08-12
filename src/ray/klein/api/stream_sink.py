# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

from ray.klein.api.functions.logical_function import LogicalFunction
from ray.klein.api.node_type import NodeType
from ray.klein.api.stream import Stream
from ray.klein.runtime.operator.sink import CollectOperator, SinkOperator
from ray.klein.runtime.resources import Resources

if TYPE_CHECKING:
    from ray.klein.api.job_handle import JobHandle


class StreamSink(Stream):
    """A lazy terminal operation registered with its owning pipeline."""

    def __init__(
        self,
        input_stream: Stream | list[Stream],
        fn: LogicalFunction,
        *,
        resources: Resources | None = None,
        node_type: NodeType | None = None,
        name: str | None = None,
    ) -> None:
        super().__init__(
            input_stream if isinstance(input_stream, list) else [input_stream],
            CollectOperator(fn) if node_type == NodeType.TAKE else SinkOperator(fn),
            (name or ("StreamTake" if node_type == NodeType.TAKE else "StreamSink")),
            NodeType.SINK if node_type is None else node_type,
            resources=resources,
        )
        self._statement_set_owner_token: str | None = None
        self.context.add_sink(self)

    def run(self, job_name: str | None = None) -> JobHandle:
        """Execute only this terminal and return its job handle."""

        return self.context.execute(job_name, sinks=(self,))

    def wait(self, job_name: str | None = None) -> None:
        """Execute this legacy lazy terminal and wait for completion."""

        self.run(job_name).wait()

    def result(self, job_name: str | None = None) -> Any:
        """Execute this legacy lazy terminal and return its collected result."""

        return self.run(job_name).result()

    def explain(self, job_name: str | None = None) -> str:
        """Compile only this terminal and return its resource plan."""

        return cast(str, self.context.explain(job_name, sinks=(self,)))
