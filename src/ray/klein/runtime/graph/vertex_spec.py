# SPDX-License-Identifier: Apache-2.0
from dataclasses import dataclass

from ray.klein.api.node_type import NodeType
from ray.klein.runtime.graph.vertex_id import VertexId
from ray.klein.runtime.operator.operator_spec import OperatorSpec
from ray.klein.runtime.resources import Resources


@dataclass(frozen=True, slots=True)
class VertexSpec:
    """The one vertex attribute bag carried through every layer.

    Batch fields are NOT stored here: the operator's ``RuntimeInfo`` (reachable
    via ``operator.runtime_info``) is the single source of truth, exposed below
    as terse helpers so call sites stay readable.
    """

    id: VertexId
    name: str
    operator: OperatorSpec
    node_type: NodeType
    resources: Resources
    ray_serve_enabled: bool = False

    @property
    def concurrency(self) -> int:
        """Resolved scalar parallelism (the lower bound of an autoscaling range)."""
        return self.resources.scalar_concurrency

    @property
    def batch_size(self) -> int | None:
        return self.operator.runtime_info.batch_size

    @property
    def batch_timeout(self) -> int | None:
        return self.operator.runtime_info.batch_timeout

    @property
    def batch_format(self) -> str | None:
        return self.operator.runtime_info.batch_format

    @property
    def async_buffer_size(self) -> int | None:
        return self.operator.runtime_info.async_buffer_size

    @property
    def is_source(self) -> bool:
        return self.operator.source
