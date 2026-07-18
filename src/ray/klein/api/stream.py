# SPDX-License-Identifier: Apache-2.0
from typing import TYPE_CHECKING

from ray.klein.api.node_type import NodeType
from ray.klein.runtime.operator.operator import StreamOperator
from ray.klein.runtime.resources import Resources

if TYPE_CHECKING:
    from ray.klein.api.klein_context import KleinContext
    from ray.klein.runtime.partitioning.partitioner import Partitioner


class Stream:
    """One node in a lazily assembled Klein pipeline."""

    def __init__(
        self,
        input_streams: list["Stream"],
        stream_operator: StreamOperator,
        name: str,
        node_type: NodeType,
        *,
        resources: Resources | None = None,
        context: "KleinContext | None" = None,
        partitioner: "Partitioner | None" = None,
        ray_serve_enabled: bool = False,
    ) -> None:
        self.input_streams: list[Stream] = input_streams
        self.stream_operator = stream_operator
        self.name = name
        self.node_type = node_type
        # The single resource representation for this stream, shared by reference
        # with the operator's LogicalFunction so the batch lowering derives its
        # ray.data kwargs from the same object the runtime schedules on.
        self.resources = resources if resources is not None else Resources()
        self.partitioner = partitioner
        self.ray_serve_enabled = ray_serve_enabled

        # ------------------------------------------------------------------
        #  Core: construction helpers
        # ------------------------------------------------------------------
        if node_type == NodeType.SOURCE and context is None:
            raise ValueError("A source stream requires a Klein context")
        if context is None:
            if not input_streams:
                raise ValueError("A non-source stream requires at least one input stream")
            if not all(stream.context is input_streams[0].context for stream in input_streams):
                raise ValueError("a stream graph cannot contain multiple Klein contexts")
            self.context = input_streams[0].context
        else:
            self.context = context
        self.id = self.context._allocate_stream_id()
        stream_operator.id = self.id
        stream_operator.name = self.name

    @property
    def concurrency(self) -> int | tuple[int, int]:
        return self.resources.effective_concurrency
