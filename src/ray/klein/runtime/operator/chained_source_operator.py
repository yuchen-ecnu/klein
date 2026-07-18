# SPDX-License-Identifier: Apache-2.0
from collections.abc import Callable
from typing import Any

from ray.klein.api.collector import Collector
from ray.klein.runtime.context.runtime_context import TaskRuntimeContext
from ray.klein.runtime.operator.chained_operator import ChainedOperator
from ray.klein.runtime.operator.operator import OperatorType, StreamOperator
from ray.klein.runtime.operator.source import SourceOperator


class ChainedSourceOperator(ChainedOperator, SourceOperator):
    """A source chain that delegates source lifecycle to its single root."""

    def __init__(self, root_op: StreamOperator, operators: list[StreamOperator]) -> None:
        if not isinstance(root_op, SourceOperator):
            raise TypeError("ChainedSourceOperator root must be a source operator")
        super().__init__(root_op, operators)
        self._bounded = root_op.bounded

    def open(self, collector: Collector, runtime_context: TaskRuntimeContext) -> None:
        super().open(collector, runtime_context)

    @property
    def operator_type(self) -> OperatorType:
        return OperatorType.SOURCE

    def run(self) -> None:
        self._root_operator.run()

    def interrupt(self) -> None:
        self._root_operator.interrupt()

    def restore_state(self, state: Any) -> None:
        self._root_operator.restore_state(state)

    def snapshot_state(self, checkpoint_id: int) -> Any:
        return self._root_operator.snapshot_state(checkpoint_id)

    def notify_checkpoint_complete(self, checkpoint_id: int) -> None:
        self._root_operator.notify_checkpoint_complete(checkpoint_id)

    def bind_record_emitter(self, on_record_emitted: Callable) -> None:
        self._root_operator.bind_record_emitter(on_record_emitted)
