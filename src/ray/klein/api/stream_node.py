# SPDX-License-Identifier: Apache-2.0

from ray.klein.api.node_type import NodeType
from ray.klein.api.resource_node import ResourceNode
from ray.klein.api.stream import Stream
from ray.klein.runtime.operator.chained_operator import ChainedOperator
from ray.klein.runtime.operator.operator import StreamOperator
from ray.klein.runtime.resources import Resources


class StreamNode:
    def __init__(
        self,
        node_id: int,
        name: str,
        operator: StreamOperator,
        resources: Resources,
        node_type: NodeType,
        ray_serve_enabled: bool = False,
    ) -> None:
        self.id = node_id
        self.base_name = name
        self.name = f"{name}[{self.id}]"
        self.operator = operator
        self.resources = resources
        self.node_type = node_type
        self.ray_serve_enabled = ray_serve_enabled

    @staticmethod
    def load(stream: Stream) -> "StreamNode":
        return StreamNode(
            stream.id,
            stream.name,
            stream.stream_operator,
            stream.resources,
            stream.node_type,
            stream.ray_serve_enabled,
        )

    def chain(self, nodes: list["StreamNode"]) -> "StreamNode":
        self.operator = ChainedOperator.compose(self.operator, [node.operator for node in nodes])
        self.name = f"{self.name} -> {', '.join(node.name for node in nodes)}"
        return self

    @property
    def resource_plan_node(self) -> ResourceNode:
        """Serializable tuning record for this node (the ResourcePlan unit)."""
        return ResourceNode(
            self.id,
            self.base_name,
            self.resources.num_cpus,
            self.resources.num_gpus,
            self.resources.concurrency,
            self.operator.runtime_info.batch_size,
            self.operator.runtime_info.async_buffer_size,
        )

    def __str__(self) -> str:
        return f"{self.name} {self.resource_plan_node}"

    __repr__ = __str__

    @property
    def concurrency(self) -> int | tuple[int, int]:
        return self.resources.effective_concurrency
