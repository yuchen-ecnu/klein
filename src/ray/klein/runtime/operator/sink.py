# SPDX-License-Identifier: Apache-2.0

from ray.util.queue import Queue

from ray.klein.api.collect_function import CollectFunction
from ray.klein.api.functions.logical_function import LogicalFunction
from ray.klein.api.sink_committable import SinkCommittable
from ray.klein.api.sink_function import SinkFunction
from ray.klein.api.two_phase_commit_sink_function import TwoPhaseCommitSinkFunction
from ray.klein.runtime.context.runtime_context import (
    OperatorRuntimeContext,
)
from ray.klein.runtime.message import Record
from ray.klein.runtime.operator.operator import (
    OneInputOperator,
    OperatorType,
    StreamOperator,
)


class SinkOperator(StreamOperator, OneInputOperator):
    """
    Operator to run a :class:`function.SinkFunction`
    """

    @property
    def sink_function(self) -> SinkFunction:
        if not isinstance(self._function, SinkFunction):
            raise RuntimeError("sink function is unavailable before open()")
        return self._function

    def process_element(self, record: Record) -> None:
        self.sink_function.write(record.block)

    def flush(self) -> None:
        self.sink_function.flush()

    def prepare_checkpoint(self, checkpoint_id: int) -> SinkCommittable | None:
        sink = self.sink_function
        if not isinstance(sink, TwoPhaseCommitSinkFunction):
            return None
        return sink.prepare_commit(checkpoint_id)

    @property
    def operator_type(self) -> OperatorType:
        return OperatorType.SINK


class CollectOperator(SinkOperator):
    """
    Operator to run a :class:`function.TakeFunction`
    """

    def __init__(self, logical_function: LogicalFunction | None = None) -> None:
        super().__init__(logical_function)
        self.output_queue: Queue | None = None
        self._limit: int | None = None
        self._processed_records: int = 0

    def assign_output_queue(self, output_queue: Queue | None) -> None:
        self.output_queue = output_queue

    def _materialize_function(self, runtime_context: OperatorRuntimeContext) -> CollectFunction:
        if self._logical_function is None:
            raise RuntimeError("collect operator has no logical function to materialize")
        function = self._logical_function.to_stream(runtime_context, self.output_queue)
        if not isinstance(function, CollectFunction):
            raise TypeError("collect operator requires a CollectFunction")
        self._limit = function.limit
        return function

    def process_element(self, record: Record) -> None:
        if self._limit is None or self._processed_records < self._limit:
            self.sink_function.write(record.block)
            self._processed_records += 1

    @property
    def end_of_stream(self) -> bool:
        return self._limit is not None and self._processed_records >= self._limit

    @property
    def operator_type(self) -> OperatorType:
        return OperatorType.COLLECT
