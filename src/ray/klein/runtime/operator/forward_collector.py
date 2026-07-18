# SPDX-License-Identifier: Apache-2.0
from ray.klein.api.collector import Collector
from ray.klein.runtime.message import Barrier, Record, StreamControl
from ray.klein.runtime.operator.operator import StreamOperator


class ForwardCollector(Collector):
    """Forward records through one or more operators in a chain."""

    def __init__(self, succeeding_ops: list[StreamOperator]) -> None:
        super().__init__()
        self.succeeding_ops: list[StreamOperator] = succeeding_ops

    def collect(self, record: Record) -> None:
        if isinstance(record, Barrier | StreamControl):
            for op in self.succeeding_ops:
                op.collect(record)
            return
        for index, op in enumerate(self.succeeding_ops):
            if index == 0:
                self._process(op, record)
            else:
                self._process(op, record.fork())

    @staticmethod
    def _process(op: StreamOperator, record: Record) -> None:
        try:
            op.invoke_process(record)
        except Exception as exc:
            if not op.should_ignore_exception(exc):
                raise
