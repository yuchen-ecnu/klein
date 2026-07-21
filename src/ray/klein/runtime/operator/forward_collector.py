# SPDX-License-Identifier: Apache-2.0
from ray.klein.api.collector import Collector
from ray.klein.runtime.message import Barrier, Record, StreamControl
from ray.klein.runtime.operator.operator import StreamOperator


class ForwardCollector(Collector):
    """Forward records through one or more operators in a chain."""

    def __init__(self, succeeding_ops: list[StreamOperator]) -> None:
        super().__init__()
        self._succeeding_operators = tuple(succeeding_ops)

    def collect(self, record: Record) -> None:
        self._ensure_open()
        if isinstance(record, Barrier | StreamControl):
            for op in self._succeeding_operators:
                op.collect(record)
            return
        branch_records = [record, *(record.fork() for _ in self._succeeding_operators[1:])]
        for op, branch_record in zip(self._succeeding_operators, branch_records, strict=True):
            self._process(op, branch_record)

    def flush(self, force: bool = False) -> None:
        """Flush every chained branch through its final output collector."""
        self._ensure_open()
        flushed: set[int] = set()
        for operator in self._succeeding_operators:
            operator.flush()
            collector = operator._collector
            if collector is not None and id(collector) not in flushed:
                collector.flush(force=force)
                flushed.add(id(collector))

    def _on_close(self) -> None:
        """Operator lifecycle owns the succeeding operators."""

    @staticmethod
    def _process(op: StreamOperator, record: Record) -> None:
        try:
            op.invoke_process(record)
        except Exception as exc:
            if not op.should_ignore_exception(exc):
                raise
