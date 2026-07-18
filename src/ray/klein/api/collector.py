# SPDX-License-Identifier: Apache-2.0
from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import TYPE_CHECKING

from ray.klein.runtime.context.runtime_context import OperatorRuntimeContext
from ray.klein.runtime.message import Record

if TYPE_CHECKING:
    from ray.klein.observability.metrics.metrics import Counter, Histogram


class Collector(ABC):
    """
    The collector that collects data from an upstream operator,
     and emits data to downstream operators.
    """

    def __init__(self) -> None:
        self._subtask_index = None
        self._parallelism = None
        self._task_name = None
        self._num_records_out = None
        # Readable mirror of the (write-only Prometheus) num_records_out metric;
        # the CLI progress view reads this via RPC. Counted here, the single
        # chokepoint every emitted record crosses — including source emissions
        # that go straight to the collector and bypass operator.collect().
        self._records_out_count: int = 0

    def open(self, op_runtime_context: OperatorRuntimeContext, register_metric: bool = True) -> None:
        """
        Initialize and Start the Collector.

        ``register_metric`` is False for the children of a fan-out
        ``CollectionCollector``: ``num_records_out`` is a single operator-level
        metric, so only the parent registers it on the shared OperatorMetricGroup.
        Each child still counts into its readable ``_records_out_count`` (the
        parent aggregates those), it just doesn't re-register the Prometheus
        counter — which would collide on the shared group and be dropped anyway.
        """
        from ray.klein.observability.metrics.metric_catalog import KleinMetrics

        self._subtask_index = op_runtime_context.task_index
        self._parallelism = op_runtime_context.parallelism
        self._task_name = op_runtime_context.task_name
        metric_group = op_runtime_context.metric_group
        if metric_group is not None and register_metric:
            self._num_records_out = metric_group.builtin_counter(KleinMetrics.RECORDS_OUT)

    def _count_out(self, n: int = 1) -> None:
        """Record that ``n`` data records were emitted (metric + readable int)."""
        if self._num_records_out is not None:
            self._num_records_out.inc(n)
        self._records_out_count += n

    @staticmethod
    def _record_rows(record: Record) -> int:
        rows = 1 if record.num_rows is None else record.num_rows
        if rows < 0:
            raise ValueError("record.num_rows must be non-negative")
        return rows

    @property
    def records_out(self) -> int:
        return self._records_out_count

    def close(self) -> None:
        """Stop the collector."""

        self._release_metric_handles()

    def _release_metric_handles(self) -> None:
        self._num_records_out = None

    @abstractmethod
    def collect(self, record: Record) -> None:
        """
        Collect data to downstream operators.
        """

    def flush(self, force: bool = False) -> None:
        """Flush any buffered output downstream (time-based / idle flush).

        ``force`` empties the buffer unconditionally (used by the
        replay-watermark flush, which must guarantee all processed output has
        physically left the task before the upstream may drop those records)."""
        del force

    def configure_pipelining(self, pipelined: bool) -> None:
        """Enable/disable emit pipelining (buffer in collect, drain on the loop)."""
        del pipelined

    def attach_runtime_metrics(
        self,
        replay_size_observer: Callable[[int], None],
        backpressure_events: "Counter",
        backpressure_duration_ms: "Histogram",
    ) -> None:
        """Attach task metrics after the collector and task state are built."""
        del replay_size_observer, backpressure_events, backpressure_duration_ms

    @property
    def replay_buffered_records(self) -> int:
        return 0

    @property
    def backpressure_events(self) -> int:
        return 0

    @property
    def backpressure_duration_ns(self) -> int:
        return 0

    def detach_pending(self) -> list:
        """Atomically take buffered emit-ops for the loop-side emitter."""
        return []

    async def aemit(self, pending: list) -> None:
        """Drain detached emit-ops on the actor loop, in FIFO order."""
        del pending

    @property
    def healthy(self) -> bool:
        """Whether this collector can currently emit records."""
        return True
