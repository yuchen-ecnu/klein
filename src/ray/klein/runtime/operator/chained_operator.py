# SPDX-License-Identifier: Apache-2.0
from abc import ABC

from ray.klein._internal.logging import get_logger
from ray.klein.api.collector import Collector
from ray.klein.api.sink_committable import SinkCommittable
from ray.klein.runtime.context.runtime_context import TaskRuntimeContext
from ray.klein.runtime.operator.composite_sink_committable import CompositeSinkCommittable
from ray.klein.runtime.operator.forward_collector import ForwardCollector
from ray.klein.runtime.operator.operator import StreamOperator
from ray.klein.runtime.operator.operator_type import OperatorType

logger = get_logger(__name__)


class ChainedOperator(StreamOperator, ABC):
    def __init__(self, root_operator: StreamOperator, operators: list[StreamOperator]) -> None:
        super().__init__(root_operator.logical_function)
        self.id = root_operator.id
        self.name = root_operator.name
        self.operators = operators
        self._root_operator = root_operator

    def open(self, collector: Collector, runtime_context: TaskRuntimeContext) -> None:
        if len(self.operators) > 1 and collector is not None:
            raise ValueError("ChainedOperator with multi downstream node should not have collector.")

        opened: list[StreamOperator] = []
        try:
            for operator in self.operators:
                opened.append(operator)
                operator.open(collector, runtime_context)
            opened.append(self._root_operator)
            self._root_operator.open(ForwardCollector(self.operators), runtime_context)
        except BaseException:
            for operator in reversed(opened):
                try:
                    operator.close()
                except Exception:
                    logger.exception("Failed to roll back operator %s after chain open failed", operator)
            raise
        self._collector = ForwardCollector([self._root_operator])
        self._collector.open(runtime_context)
        # The chain wires its own collectors, so it adopts the root operator's
        # exception policy instead of running StreamOperator.open itself.
        self._handle_udf_exception_func = self._root_operator._handle_udf_exception_func

    @property
    def operator_type(self) -> OperatorType:
        return self.operators[0].operator_type

    @property
    def records_in(self) -> int:
        return self._root_operator.records_in

    @property
    def records_out(self) -> int:
        return sum(operator.records_out for operator in self.operators)

    @property
    def bytes_in(self) -> int:
        return self._root_operator.bytes_in

    @property
    def bytes_out(self) -> int:
        return sum(operator.bytes_out for operator in self.operators)

    @property
    def processing_duration_ns(self) -> int:
        return self._root_operator.processing_duration_ns + sum(
            operator.processing_duration_ns for operator in self.operators
        )

    @property
    def end_of_stream(self) -> bool:
        return any(operator.end_of_stream for operator in self.operators)

    def close(self) -> None:
        first_error: Exception | None = None
        for operator in (self._root_operator, *self.operators):
            try:
                operator.close()
            except Exception as error:
                if first_error is None:
                    first_error = error
                else:
                    logger.exception("Failed to close chained operator %s", operator)
        if self._collector is not None:
            self._collector.close()
        if first_error is not None:
            raise first_error.with_traceback(first_error.__traceback__)

    def finish(self) -> None:
        self._root_operator.finish()
        for operator in self.operators:
            operator.finish()

    def on_idle(self) -> None:
        self._root_operator.on_idle()
        for operator in self.operators:
            operator.on_idle()

    def on_event_time_watermark(self, timestamp: int) -> None:
        self._root_operator.on_event_time_watermark(timestamp)
        for operator in self.operators:
            operator.on_event_time_watermark(timestamp)

    def on_input_idle(self) -> None:
        self._root_operator.on_input_idle()
        for operator in self.operators:
            operator.on_input_idle()

    def on_input_active(self) -> None:
        self._root_operator.on_input_active()
        for operator in self.operators:
            operator.on_input_active()

    def flush(self) -> None:
        # Lifecycle methods are part of the StreamOperator contract. Calling
        # every child follows sequential nested chains and fan-out leaves;
        # ordinary transforms inherit the no-op implementation.
        for operator in (self._root_operator, *self.operators):
            operator.flush()

    def prepare_checkpoint(self, checkpoint_id: int) -> SinkCommittable | None:
        committables = [
            committable
            for operator in (self._root_operator, *self.operators)
            if (committable := operator.prepare_checkpoint(checkpoint_id)) is not None
        ]
        if not committables:
            return None
        if len(committables) == 1:
            return committables[0]
        return CompositeSinkCommittable(tuple(committables))

    @staticmethod
    def compose(
        root: StreamOperator,
        succeeding: list[StreamOperator],
    ) -> "ChainedOperator":
        from ray.klein.runtime.operator.chained_one_input_operator import ChainedOneInputOperator
        from ray.klein.runtime.operator.chained_source_operator import ChainedSourceOperator

        if isinstance(root, ChainedOperator):
            raise TypeError("A chained operator cannot be used as the root of another chain")

        operator_type = root.operator_type
        logger.debug("Building ChainedOperator from operators (%s -> %s).", root, succeeding)
        if operator_type == OperatorType.SOURCE:
            return ChainedSourceOperator(root, succeeding)
        if operator_type == OperatorType.ONE_INPUT:
            return ChainedOneInputOperator(root, succeeding)
        raise ValueError(f"Current operator type `{operator_type}` is not supported")
