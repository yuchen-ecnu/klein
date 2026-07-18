# SPDX-License-Identifier: Apache-2.0

from ray.klein.api.data_stream import DataStream
from ray.klein.api.node_type import NodeType
from ray.klein.api.stream import Stream
from ray.klein.runtime.operator.union_operator import UnionOperator


class UnionStream(DataStream):
    """Represents a union stream."""

    def __init__(
        self,
        input_streams: list["Stream"],
        operator: UnionOperator,
    ) -> None:
        super().__init__(
            input_streams,
            operator,
            "Union",
            NodeType.UNION,
        )
