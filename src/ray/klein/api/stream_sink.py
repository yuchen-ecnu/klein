# SPDX-License-Identifier: Apache-2.0
from ray.klein.api.functions.logical_function import LogicalFunction
from ray.klein.api.node_type import NodeType
from ray.klein.api.stream import Stream
from ray.klein.runtime.operator.sink import CollectOperator, SinkOperator
from ray.klein.runtime.resources import Resources


class StreamSink(Stream):
    """Represents a sink of the DataStream."""

    def __init__(
        self,
        input_stream: "Stream | list[Stream]",
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
        self.context.add_sink(self)
