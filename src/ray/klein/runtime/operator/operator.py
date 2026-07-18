# SPDX-License-Identifier: Apache-2.0
import collections
import functools
import time
from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from ray.klein._internal.block import block_num_rows
from ray.klein._internal.values import is_valid_column_values, truncated_repr
from ray.klein.api.collector import Collector
from ray.klein.api.function import Function
from ray.klein.api.functions.logical_function import LogicalFunction
from ray.klein.api.runtime_info import RuntimeInfo
from ray.klein.api.sink_committable import SinkCommittable
from ray.klein.config.udf_options import UDFOptions
from ray.klein.observability.metrics.metric_catalog import KleinMetrics
from ray.klein.observability.metrics.metric_group import MetricGroup
from ray.klein.observability.metrics.metrics import Counter, Histogram
from ray.klein.runtime.context.runtime_context import (
    OperatorRuntimeContext,
    TaskRuntimeContext,
)
from ray.klein.runtime.message import Barrier, Record, StreamControl
from ray.klein.runtime.operator.error_handling import handle_udf_exception
from ray.klein.runtime.operator.operator_type import OperatorType

if TYPE_CHECKING:
    from ray.klein.runtime.operator.operator_spec import OperatorSpec


class Operator(ABC):
    """Runtime operator lifecycle contract."""

    @abstractmethod
    def open(self, collector: Collector, runtime_context: TaskRuntimeContext) -> None:
        """Initialize the operator for one task."""

    @abstractmethod
    def close(self) -> None:
        """Release task-local resources."""

    @property
    @abstractmethod
    def operator_type(self) -> OperatorType:
        """Operator input/output shape."""


class OneInputOperator(Operator, ABC):
    """Interface for stream operators with one input."""

    @abstractmethod
    def process_element(self, record: Record) -> None:
        """Process one input record."""

    async def process_async_element(self, record: Record) -> list[Record]:
        """Async element processing: compute only, return records to emit.

        Mirrors Flink's ``AsyncFunction``: the async path does NOT side-effect
        ``collect()`` (that would race the shared emit buffer when multiple
        requests are in flight). It computes and *returns* the records it would
        have collected; the framework (AsyncOrderedRunner's consumer) calls
        ``collect()`` on each, serially and in input order, so emission stays
        ordered and race-free while the compute (await) runs concurrently.
        Return an empty list to emit nothing (e.g. a filtered-out record).
        """
        raise NotImplementedError("async process_element is not implemented!")

    @property
    def operator_type(self) -> OperatorType:
        return OperatorType.ONE_INPUT


class TwoInputOperator(Operator, ABC):
    """Stream operator with two inputs."""

    @abstractmethod
    def process_element(self, _record1: Record, _record2: Record) -> None:
        """Process one record from each input."""

    @property
    def operator_type(self) -> OperatorType:
        return OperatorType.TWO_INPUT


class StreamOperator(Operator, ABC):
    """
    Basic interface for stream operators. Implementers would implement one of
    :class:`OneInputOperator` or :class:`TwoInputOperator` to create
    operators that process elements.
    """

    def __init__(self, logical_function: LogicalFunction | None = None) -> None:
        self.id: int | None = None
        self.name: str | None = None
        self._function: Any = None
        self._collector: Collector | None = None
        self._logical_function = logical_function
        self._handle_udf_exception_func: Callable | None = None
        # Readable mirror of the (write-only Prometheus) num_records_in metric.
        # The collector mirrors records_out the same way, but a terminal operator
        # (a sink, or a serve proxy at the end of the pipeline) has no downstream
        # collector, so its records_out would read 0; we fall back to this count
        # so the CLI progress view still shows throughput for terminal nodes.
        self._records_in_count: int = 0
        # Readable monotonic processing time used by the dashboard's interval
        # busy percentage. Ray histograms are write-only from the task actor.
        self._processing_duration_ns: int = 0
        # Columnar passthrough (opt-in): when True, a batched operator emits its
        # column-oriented output as a single Record(num_rows=N) instead of
        # exploding it into N per-row records. Set in open() from config.
        self._columnar_passthrough: bool = False
        self._metric_group: MetricGroup | None = None
        self._records_in_metric: Counter | None = None
        self._processing_duration_metric: Histogram | None = None

    def open(self, collector: Collector, runtime_context: TaskRuntimeContext) -> None:
        self._collector = collector
        op_runtime_context = OperatorRuntimeContext(runtime_context, self.id, self.name)
        metrics_group = op_runtime_context.metric_group
        self._metric_group = metrics_group
        if isinstance(self, OneInputOperator):
            self._records_in_metric = metrics_group.builtin_counter(KleinMetrics.RECORDS_IN)
            self._processing_duration_metric = metrics_group.builtin_histogram(KleinMetrics.PROCESSING_DURATION_MS)

        from ray.klein.config.pipeline_options import PipelineOptions

        self._columnar_passthrough = runtime_context.config.get(PipelineOptions.COLUMNAR_PASSTHROUGH_ENABLED)
        if self._logical_function is not None:
            self._function = self._materialize_function(op_runtime_context)
        if self._collector is not None:
            self._collector.open(op_runtime_context)
        udf_error_count = metrics_group.builtin_counter(KleinMetrics.UDF_EXCEPTIONS)
        self._handle_udf_exception_func = functools.partial(
            handle_udf_exception,
            udf_error_count,
            runtime_context.config.get(UDFOptions.IGNORE_EXCEPTIONS),
        )

    def _materialize_function(self, runtime_context: OperatorRuntimeContext) -> Any:
        if self._logical_function is None:
            raise RuntimeError("operator has no logical function to materialize")
        return self._logical_function.to_stream(runtime_context)

    def close(self) -> None:
        if isinstance(self._function, Function):
            self._function.close()
        if self._collector is not None:
            self._collector.close()

    @property
    def callable_function(self) -> Callable[..., Any]:
        """Return the materialized UDF after enforcing the operator contract."""

        if not callable(self._function):
            raise RuntimeError("callable function is unavailable before open()")
        return self._function

    def should_ignore_exception(self, error: Exception) -> bool:
        """Apply this operator's configured UDF exception policy."""
        handler = self._handle_udf_exception_func
        return handler is not None and handler(error, self.name)

    def flush(self) -> None:
        """Flush buffered side effects at an aligned barrier."""

    def prepare_checkpoint(self, checkpoint_id: int) -> SinkCommittable | None:
        """Prepare optional external side effects for a global checkpoint."""

        del checkpoint_id
        return None

    def collect(self, record: Record) -> None:
        if isinstance(record, StreamControl):
            if self._collector is not None:
                self._collector.collect(record)
            return
        self._validate_record(record)
        if self._collector is None:
            return
        if not self.runtime_info.batch_enabled or isinstance(record, Barrier):
            self._collector.collect(record)
        elif self._columnar_passthrough:
            self._collect_columnar(record)
        else:
            for item in self._expand(record.block):
                self._collector.collect(Record(item))

    def _validate_record(self, record: Record) -> None:
        if isinstance(record, Barrier) or isinstance(record.block, collections.abc.Mapping):
            return
        function_name = self._function.__class__.__name__ if self._function is not None else "the operator"
        raise ValueError(
            f"Error validating {truncated_repr(record)}: "
            "Standalone Python objects are not allowed. "
            f"To return Python objects from {function_name}(), wrap them in a dict, e.g., "
            "return `{'item': item}` instead of just `item`."
        )

    def _collect_columnar(self, record: Record) -> None:
        self._validate_columns(record.block)
        num_rows = block_num_rows(record.block)
        if num_rows > 0 and self._collector is not None:
            self._collector.collect(Record(record.block, num_rows=num_rows))

    @property
    def records_in(self) -> int:
        """Records this operator has consumed so far (drives the CLI progress view).

        Read from the ``num_records_in`` mirror incremented in the metric
        wrappers. A source operator has no upstream and never increments it, so
        this reads 0 for sources — correct, since "input rows" is meaningless
        for a producer."""
        return self._records_in_count

    @property
    def metric_group(self) -> MetricGroup:
        """The operator-scoped metric group created during ``open``."""

        if self._metric_group is None:
            raise RuntimeError("operator metric_group is unavailable before open()")
        return self._metric_group

    @property
    def records_out(self) -> int:
        """Records this operator has put through so far (drives the CLI progress view).

        Normally delegated to the output collector — the single chokepoint every
        emitted record crosses, including source emissions that bypass
        ``collect()``. A terminal operator (a sink, or a serve proxy at the end
        of the pipeline) has no downstream collector and would otherwise report
        0, so we fall back to the readable input count: for a terminal node,
        "records processed" is the meaningful throughput number to surface."""
        if self._collector is not None:
            return self._collector.records_out
        return self._records_in_count

    @property
    def processing_duration_ns(self) -> int:
        return self._processing_duration_ns

    @property
    def runtime_info(self) -> RuntimeInfo:
        return RuntimeInfo() if self._logical_function is None else self._logical_function.runtime_info

    @property
    def logical_function(self) -> LogicalFunction | None:
        return self._logical_function

    def _spec_parameters(self) -> dict[str, Any]:
        """Non-fn constructor args this operator needs to be rebuilt.

        Overridden by subclasses with extra ctor params (key_selector,
        missing_data_strategy, bounded). The base operator takes only the
        logical function, so no extra keyword arguments.
        """
        return {}

    def to_spec(self) -> "OperatorSpec":
        """Lift this (graph-time) operator into an immutable, picklable recipe.

        Called once when the API-level graph is lifted into the LogicalGraph IR;
        from there the spec is carried and each subtask calls ``build()`` for its
        own runtime instance.
        """
        from ray.klein.runtime.operator.operator_spec import OperatorSpec

        return OperatorSpec(
            operator_class=type(self),
            logical_function=self._logical_function,
            id=self.id,
            name=self.name,
            operator_type=self.operator_type,
            parameters=self._spec_parameters(),
            owns_state=self.stateful,
        )

    @property
    def end_of_stream(self) -> bool:
        return False

    @property
    def stateful(self) -> bool:
        """Whether this operator owns checkpointed managed state."""

        return False

    def finish(self) -> None:
        """Flush bounded state before the terminal EndOfData barrier."""

    def on_idle(self) -> None:
        """Run lightweight maintenance while the task inbox is idle."""

    def on_event_time_watermark(self, timestamp: int) -> None:
        """Observe an aggregate, monotonic event-time watermark."""

    def on_input_idle(self) -> None:
        """Observe that all physical inputs are idle."""

    def on_input_active(self) -> None:
        """Observe that at least one physical input resumed."""

    def _validate_columns(self, batch: collections.abc.Mapping) -> None:
        """Assert every column value is a sequence (list/np.ndarray/pa.array).

        A batched UDF must return column-oriented values; a scalar means the UDF
        returned a single row where a batch was expected. Shared by the
        explode path (_expand) and the columnar-passthrough path so both reject
        the same malformed output identically.
        """
        for key, value in list(batch.items()):
            if not is_valid_column_values(value):
                raise ValueError(
                    f"Error validating {truncated_repr(batch)}: "
                    f"The function passed to `{self._function.__class__}` returned a "
                    f"`dict`. `{self._function.__class__}` expects all `dict` values "
                    f"to be `list` or `np.ndarray` type (since the batch_size={self.runtime_info.batch_size}), "
                    f"but the value corresponding to key {key!r} is of type "
                    f"{type(value)}. To fix this issue, convert "
                    f"the {type(value)} to a `np.ndarray`."
                )

    def _expand(self, batch: collections.abc.Mapping) -> list[dict[str, Any]]:
        self._validate_columns(batch)
        keys = tuple(batch.keys())
        values = zip(*batch.values(), strict=True)
        return [dict(zip(keys, value, strict=True)) for value in values]

    def invoke_process(self, record: Record) -> None:
        """Framework entrypoint for a synchronous operator invocation.

        Keeping instrumentation in this explicit facade avoids mutating bound
        methods during ``open`` and preserves one row-counting rule for direct,
        chained and columnar execution.
        """

        self._record_input(record)
        started_at = time.monotonic()
        try:
            if not isinstance(self, OneInputOperator):
                raise TypeError("invoke_process requires a OneInputOperator")
            self.process_element(record)
        finally:
            self._processing_duration_ns += max(0, int((time.monotonic() - started_at) * 1_000_000_000))
            if self._processing_duration_metric is not None:
                self._processing_duration_metric.observe_elapsed(started_at)

    async def invoke_process_async(self, record: Record) -> list[Record]:
        self._record_input(record)
        started_at = time.monotonic()
        try:
            if not isinstance(self, OneInputOperator):
                raise TypeError("invoke_process_async requires a OneInputOperator")
            return await self.process_async_element(record)
        finally:
            self._processing_duration_ns += max(0, int((time.monotonic() - started_at) * 1_000_000_000))
            if self._processing_duration_metric is not None:
                self._processing_duration_metric.observe_elapsed(started_at)

    def _record_input(self, record: Record) -> None:
        rows = 1 if record.num_rows is None else record.num_rows
        if rows < 0:
            raise ValueError("record.num_rows must be non-negative")
        if self._records_in_metric is not None:
            self._records_in_metric.inc(rows)
        self._records_in_count += rows

    def __str__(self) -> str:
        return f"{self.__class__.__name__} ({self.runtime_info})"
