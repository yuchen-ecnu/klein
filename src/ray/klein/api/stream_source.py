# SPDX-License-Identifier: Apache-2.0

from typing import TYPE_CHECKING

from ray.klein.api.data_stream import DataStream
from ray.klein.api.functions.logical_function import LogicalFunction
from ray.klein.api.node_type import NodeType
from ray.klein.runtime.operator.source import SourceFunctionOperator
from ray.klein.runtime.resources import Resources

if TYPE_CHECKING:
    from ray.klein.api.klein_context import KleinContext


class StreamSource(DataStream):
    """Represents a source of the DataStream."""

    def __init__(
        self,
        context: "KleinContext",
        fn: LogicalFunction,
        *,
        resources: Resources | None = None,
        name: str | None = None,
        bounded: bool = False,
    ) -> None:
        super().__init__(
            [],
            SourceFunctionOperator(fn, bounded),
            name or "StreamSource",
            NodeType.SOURCE,
            resources=resources,
            context=context,
        )
