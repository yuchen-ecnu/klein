# SPDX-License-Identifier: Apache-2.0
import pickle
from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any

from ray.klein.api.collector import Collector
from ray.klein.api.functions.logical_function import LogicalFunction
from ray.klein.api.source_function import SourceFunction
from ray.klein.runtime.context.runtime_context import TaskRuntimeContext
from ray.klein.runtime.context.source_context import RuntimeSourceContext
from ray.klein.runtime.operator.operator import OperatorType, StreamOperator


class SourceOperator(StreamOperator, ABC):
    def __init__(self, logical_function: LogicalFunction | None = None, bounded: bool = False) -> None:
        super().__init__(logical_function)
        self._bounded = bounded

    @abstractmethod
    def run(self) -> None:
        """Run the source loop until completion or interruption."""

    @abstractmethod
    def interrupt(self) -> None:
        """Ask the running source loop to stop cooperatively."""

    @property
    def bounded(self) -> bool:
        return self._bounded

    def _spec_parameters(self) -> dict[str, Any]:
        return {"bounded": self._bounded}

    @property
    def operator_type(self) -> OperatorType:
        return OperatorType.SOURCE

    @abstractmethod
    def restore_state(self, state: Any) -> None:
        """Restore this source subtask's opaque checkpoint state."""

    @abstractmethod
    def snapshot_state(self, checkpoint_id: int) -> Any:
        """Capture source state before emitting a checkpoint barrier."""

    def notify_checkpoint_complete(self, checkpoint_id: int) -> None:
        """Notify the source after the captured state becomes durable."""

    @abstractmethod
    def bind_record_emitter(self, on_record_emitted: Callable) -> None:
        """
        Setting listener for records emitted.
        """


class SourceFunctionOperator(SourceOperator):
    """
    Operator to run a :class:`function.SourceFunction`
    """

    def __init__(self, fn: LogicalFunction, bounded: bool = False) -> None:
        super().__init__(fn, bounded)
        self.source_context: RuntimeSourceContext | None = None

    def open(self, collector: Collector, runtime_context: TaskRuntimeContext) -> None:
        super().open(collector, runtime_context)
        self.source_context = RuntimeSourceContext(collector)

    def run(self) -> None:
        if self.source_context is None:
            raise RuntimeError("run called before open(); source_context is not initialized")
        self.source_function.run(self.source_context)

    @property
    def source_function(self) -> SourceFunction:
        if not isinstance(self._function, SourceFunction):
            raise RuntimeError("source function is unavailable before open()")
        return self._function

    def interrupt(self) -> None:
        """Ask the still-running source loop to stop cooperatively."""
        if self._function is not None:
            self.source_function.cancel()

    def restore_state(self, state: Any) -> None:
        self.source_function.restore_state(state)

    def snapshot_state(self, checkpoint_id: int) -> Any:
        if self.source_context is None:
            raise RuntimeError("snapshot_state called before open(); source_context is not initialized.")
        state = self.source_function.snapshot_state(checkpoint_id)
        try:
            payload = pickle.dumps(state, protocol=pickle.HIGHEST_PROTOCOL)
            return pickle.loads(payload)
        except (pickle.PickleError, TypeError, AttributeError) as exc:
            raise TypeError("Source checkpoint state must be pickleable") from exc

    def notify_checkpoint_complete(self, checkpoint_id: int) -> None:
        self.source_function.notify_checkpoint_complete(checkpoint_id)

    def bind_record_emitter(self, on_record_emitted: Callable) -> None:
        if self.source_context is None:
            raise RuntimeError("bind_record_emitter called before open(); source_context is not initialized.")
        self.source_context.bind_record_emitter(on_record_emitted)


__all__ = ["SourceFunctionOperator", "SourceOperator"]
