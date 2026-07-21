# SPDX-License-Identifier: Apache-2.0
from types import SimpleNamespace
from typing import ClassVar

import pytest

from ray.klein.api.collect_function import CollectFunction
from ray.klein.api.collector import Collector
from ray.klein.api.functions.logical_function import LogicalFunction
from ray.klein.api.runtime_info import RuntimeInfo
from ray.klein.api.sink_committable import SinkCommittable
from ray.klein.config.configuration import Configuration
from ray.klein.observability.metrics.metric_group import JobMetricGroup
from ray.klein.runtime.context.runtime_context import TaskRuntimeContext
from ray.klein.runtime.message import Barrier, Record, Watermark
from ray.klein.runtime.operator.chained_one_input_operator import ChainedOneInputOperator
from ray.klein.runtime.operator.composite_sink_committable import CompositeSinkCommittable
from ray.klein.runtime.operator.error_handling import handle_udf_exception
from ray.klein.runtime.operator.filter_operator import FilterOperator
from ray.klein.runtime.operator.flat_map_operator import FlatMapOperator
from ray.klein.runtime.operator.forward_collector import ForwardCollector
from ray.klein.runtime.operator.map_operator import MapOperator
from ray.klein.runtime.operator.operator import OneInputOperator, OperatorType, StreamOperator
from ray.klein.runtime.operator.sink import CollectOperator


class _CountingCollect(CollectFunction):
    instances: ClassVar[list["_CountingCollect"]] = []

    def __init__(self, output_queue) -> None:
        super().__init__(output_queue)
        self.open_count = 0
        self.close_count = 0
        self.instances.append(self)

    def open(self, runtime_context) -> None:
        self.open_count += 1

    def close(self) -> None:
        self.close_count += 1


def test_collect_operator_materializes_one_lifecycle_instance() -> None:
    _CountingCollect.instances.clear()
    operator = CollectOperator(LogicalFunction(_CountingCollect))
    operator.id, operator.name = 1, "collect"
    operator.assign_output_queue(SimpleNamespace())
    context = TaskRuntimeContext(
        "collect",
        0,
        1,
        Configuration(),
        JobMetricGroup("test").add_task_group("1:0", "collect", 0),
        SimpleNamespace(),
        RuntimeInfo(),
        "test",
    )

    operator.open(None, context)
    operator.close()

    assert len(_CountingCollect.instances) == 1
    assert (_CountingCollect.instances[0].open_count, _CountingCollect.instances[0].close_count) == (1, 1)


class _LifecycleOperator(StreamOperator, OneInputOperator):
    def __init__(self, *, fail_open: bool = False) -> None:
        super().__init__()
        self.fail_open = fail_open
        self.open_count = 0
        self.close_count = 0

    def open(self, collector, runtime_context) -> None:
        self.open_count += 1
        if self.fail_open:
            raise RuntimeError("open failed")

    def close(self) -> None:
        self.close_count += 1

    def process_element(self, record: Record) -> None:
        return None

    @property
    def operator_type(self) -> OperatorType:
        return OperatorType.ONE_INPUT


def test_chained_operator_rolls_back_every_opened_operator() -> None:
    root = _LifecycleOperator()
    first = _LifecycleOperator()
    failing = _LifecycleOperator(fail_open=True)
    chain = ChainedOneInputOperator(root, [first, failing])

    try:
        chain.open(None, SimpleNamespace())
    except RuntimeError as error:
        assert str(error) == "open failed"
    else:
        raise AssertionError("chain.open() must propagate the operator failure")

    assert root.open_count == root.close_count == 0
    assert (first.open_count, first.close_count) == (1, 1)
    assert (failing.open_count, failing.close_count) == (1, 1)


class _TestCommittable(SinkCommittable):
    def __init__(self, transaction_id: str = "transaction-1") -> None:
        self._transaction_id = transaction_id

    @property
    def transaction_id(self) -> str:
        return self._transaction_id

    def commit(self) -> None:
        return None

    def abort(self) -> None:
        return None


class _TransactionalLifecycleOperator(_LifecycleOperator):
    def __init__(self, transaction_id: str = "transaction-1") -> None:
        super().__init__()
        self.transaction_id = transaction_id
        self.flush_count = 0
        self.prepare_count = 0

    def flush(self) -> None:
        self.flush_count += 1

    def prepare_checkpoint(self, checkpoint_id: int) -> SinkCommittable:
        assert checkpoint_id == 7
        self.prepare_count += 1
        return _TestCommittable(self.transaction_id)


def test_nested_chained_operator_forwards_checkpoint_lifecycle() -> None:
    transactional = _TransactionalLifecycleOperator()
    inner = ChainedOneInputOperator(_LifecycleOperator(), [transactional])
    outer = ChainedOneInputOperator(_LifecycleOperator(), [inner])

    outer.flush()
    committable = outer.prepare_checkpoint(7)

    assert transactional.flush_count == 1
    assert transactional.prepare_count == 1
    assert committable is not None and committable.transaction_id == "transaction-1"


def test_chained_fan_out_composes_multiple_sink_transactions() -> None:
    first = _TransactionalLifecycleOperator("first")
    second = _TransactionalLifecycleOperator("second")
    chain = ChainedOneInputOperator(_LifecycleOperator(), [first, second])

    committable = chain.prepare_checkpoint(7)

    assert isinstance(committable, CompositeSinkCommittable)
    assert [item.transaction_id for item in committable.committables] == ["first", "second"]


class _RecordingCollector(Collector):
    def __init__(self) -> None:
        super().__init__()
        self.records = []
        self.flushes = []

    def collect(self, record: Record) -> None:
        self.records.append(record)

    def flush(self, force: bool = False) -> None:
        self.flushes.append(force)


class _CountingMetric:
    def __init__(self) -> None:
        self.value = 0

    def inc(self, value=1) -> None:
        self.value += value


@pytest.mark.asyncio
async def test_map_operator_emits_sync_and_async_results() -> None:
    sync_operator = MapOperator()
    sync_operator._function = lambda row: {"value": row["value"] + 1}
    collector = _RecordingCollector()
    sync_operator._collector = collector

    sync_operator.process_element(Record({"value": 1}))

    async def async_map(row):
        return {"value": row["value"] * 2}

    async_operator = MapOperator()
    async_operator._function = async_map

    assert collector.records == [Record({"value": 2})]
    assert await async_operator.process_async_element(Record({"value": 3})) == [Record({"value": 6})]


def test_flat_map_operator_emits_rows_before_generator_failure() -> None:
    failure = RuntimeError("flat-map failed")

    def generate(_row):
        yield {"value": 1}
        raise failure

    operator = FlatMapOperator()
    operator._function = generate
    collector = _RecordingCollector()
    operator._collector = collector

    with pytest.raises(RuntimeError) as captured:
        operator.process_element(Record({"value": 0}))

    assert captured.value is failure
    assert collector.records == [Record({"value": 1})]


@pytest.mark.asyncio
async def test_flat_map_operator_returns_all_async_rows() -> None:
    async def generate(row):
        return ({"value": row["value"]}, {"value": row["value"] + 1})

    operator = FlatMapOperator()
    operator._function = generate

    assert await operator.process_async_element(Record({"value": 4})) == [
        Record({"value": 4}),
        Record({"value": 5}),
    ]


def test_filter_operator_emits_kept_rows_and_records_stream_metrics() -> None:
    operator = FilterOperator(LogicalFunction(lambda row: row["keep"]))
    operator.id, operator.name = 1, "filter"
    collector = _RecordingCollector()
    context = TaskRuntimeContext(
        "filter",
        0,
        1,
        Configuration(include_environment=False),
        JobMetricGroup("test").add_task_group("1:0", "filter", 0),
        SimpleNamespace(),
        RuntimeInfo(),
    )
    operator.open(collector, context)

    operator.process_element(Record({"id": 1, "keep": True}))
    operator.process_element(Record({"id": 2, "keep": False}))

    assert collector.records == [Record({"id": 1, "keep": True})]
    assert operator._filter_total_cnt_metric is not None
    assert operator._filter_out_cnt_metric is not None
    assert (operator._filter_total_cnt_metric.value, operator._filter_out_cnt_metric.value) == (2, 1)
    operator.close()


@pytest.mark.asyncio
async def test_filter_operator_returns_or_drops_async_stream_rows() -> None:
    async def keep(row):
        return row["keep"]

    operator = FilterOperator()
    operator._function = keep

    assert await operator.process_async_element(Record({"id": 1, "keep": True})) == [Record({"id": 1, "keep": True})]
    assert await operator.process_async_element(Record({"id": 2, "keep": False})) == []


def test_filter_operator_filters_columnar_batches_and_validates_decision_count() -> None:
    logical_function = LogicalFunction(
        lambda value: value,
        batch_size=3,
        batch_timeout=1,
        batch_format="numpy",
    )
    operator = FilterOperator(logical_function)
    operator._function = lambda _batch: [True, False, True]
    collector = _RecordingCollector()
    operator._collector = collector
    total = _CountingMetric()
    dropped = _CountingMetric()
    operator._filter_total_cnt_metric = total
    operator._filter_out_cnt_metric = dropped

    operator.process_element(Record({"id": [1, 2, 3], "label": ["a", "b", "c"]}))

    assert [record.block["id"] for record in collector.records] == [1, 3]
    assert [record.block["label"] for record in collector.records] == ["a", "c"]
    assert (total.value, dropped.value) == (3, 1)
    with pytest.raises(ValueError, match="zip"):
        operator._filter_batch({"id": [1, 2]}, [True])


@pytest.mark.asyncio
async def test_filter_operator_returns_columnar_async_result_without_collecting() -> None:
    logical_function = LogicalFunction(
        lambda value: value,
        batch_size=3,
        batch_timeout=1,
        batch_format="numpy",
    )

    async def decisions(_batch):
        return [False, True, True]

    operator = FilterOperator(logical_function)
    operator._function = decisions
    returned = await operator.process_async_element(Record({"id": [1, 2, 3]}))

    assert len(returned) == 1
    assert returned[0].block["id"].tolist() == [2, 3]


class _BranchOperator:
    def __init__(
        self,
        *,
        collector=None,
        mutation=None,
        failure: Exception | None = None,
        ignore_failure: bool = False,
    ) -> None:
        self._collector = collector
        self._mutation = mutation
        self._failure = failure
        self._ignore_failure = ignore_failure
        self.records = []
        self.controls = []
        self.seen_errors = []
        self.flush_count = 0

    def invoke_process(self, record: Record) -> None:
        self.records.append(record)
        if self._mutation is not None:
            self._mutation(record)
        if self._failure is not None:
            raise self._failure

    def should_ignore_exception(self, error: Exception) -> bool:
        self.seen_errors.append(error)
        return self._ignore_failure

    def collect(self, control) -> None:
        self.controls.append(control)

    def flush(self) -> None:
        self.flush_count += 1


def test_forward_collector_isolates_branch_records_before_processing() -> None:
    def mutate(record: Record) -> None:
        assert record.block is not None
        record.block["branch"] = "first"

    first = _BranchOperator(mutation=mutate)
    second = _BranchOperator()
    collector = ForwardCollector([first, second])
    collector.open(SimpleNamespace())
    record = Record({"value": 1})
    record.timestamp = 10

    collector.collect(record)

    assert first.records[0] is record
    assert first.records[0].block == {"value": 1, "branch": "first"}
    assert second.records[0] is not record
    assert second.records[0].block == {"value": 1}
    assert second.records[0].timestamp == 10


def test_forward_collector_broadcasts_control_messages_without_processing() -> None:
    first = _BranchOperator()
    second = _BranchOperator()
    collector = ForwardCollector([first, second])
    collector.open(SimpleNamespace())
    controls = [Barrier(7), Watermark(11)]

    for control in controls:
        collector.collect(control)

    assert first.controls == controls
    assert second.controls == controls
    assert first.records == second.records == []


def test_forward_collector_applies_each_branch_error_policy() -> None:
    ignored_failure = ValueError("ignored")
    ignored = _BranchOperator(failure=ignored_failure, ignore_failure=True)
    succeeding = _BranchOperator()
    collector = ForwardCollector([ignored, succeeding])
    collector.open(SimpleNamespace())

    collector.collect(Record({"id": 1}))

    assert ignored.seen_errors == [ignored_failure]
    assert succeeding.records[0].block == {"id": 1}

    propagated_failure = RuntimeError("propagated")
    propagated = _BranchOperator(failure=propagated_failure)
    failing_collector = ForwardCollector([propagated])
    failing_collector.open(SimpleNamespace())
    with pytest.raises(RuntimeError) as captured:
        failing_collector.collect(Record({"id": 2}))
    assert captured.value is propagated_failure
    assert propagated.seen_errors == [propagated_failure]


def test_forward_collector_flushes_shared_outputs_once_and_enforces_lifecycle() -> None:
    shared_output = _RecordingCollector()
    first = _BranchOperator(collector=shared_output)
    second = _BranchOperator(collector=shared_output)
    terminal = _BranchOperator()
    collector = ForwardCollector([first, second, terminal])
    collector.open(SimpleNamespace())

    collector.flush(force=True)

    assert (first.flush_count, second.flush_count, terminal.flush_count) == (1, 1, 1)
    assert shared_output.flushes == [True]
    collector.close()
    with pytest.raises(RuntimeError, match="is not open"):
        collector.collect(Record({"id": 1}))
    with pytest.raises(RuntimeError, match="is not open"):
        collector.flush()


def test_handle_udf_exception_counts_and_logs_only_ignored_errors(monkeypatch) -> None:
    counter = _CountingMetric()
    error = ValueError("boom")
    logged = []

    def record_warning(message, *args) -> None:
        logged.append(message % args)

    monkeypatch.setattr("ray.klein.runtime.operator.error_handling.logger.warning", record_warning)

    assert handle_udf_exception(counter, True, error, "map") is True

    assert counter.value == 1
    assert logged == ["Ignoring UDF caused exception boom in map"]
    logged.clear()
    assert handle_udf_exception(counter, False, error, "map") is False
    assert counter.value == 1
    assert logged == []


class _TrackedCommittable(SinkCommittable):
    def __init__(
        self,
        transaction_id: str,
        events: list[str],
        *,
        commit_error: Exception | None = None,
        abort_error: Exception | None = None,
    ) -> None:
        self._transaction_id = transaction_id
        self.events = events
        self.commit_error = commit_error
        self.abort_error = abort_error

    @property
    def transaction_id(self) -> str:
        return self._transaction_id

    def commit(self) -> None:
        self.events.append(f"commit:{self.transaction_id}")
        if self.commit_error is not None:
            raise self.commit_error

    def abort(self) -> None:
        self.events.append(f"abort:{self.transaction_id}")
        if self.abort_error is not None:
            raise self.abort_error


def test_composite_committable_validates_children_and_has_unambiguous_identity() -> None:
    first = _TrackedCommittable("ab", [])
    second = _TrackedCommittable("c", [])
    composite = CompositeSinkCommittable([first, second])

    assert composite.committables == (first, second)
    assert composite.transaction_id == CompositeSinkCommittable((first, second)).transaction_id
    assert (
        composite.transaction_id
        != CompositeSinkCommittable((_TrackedCommittable("a", []), _TrackedCommittable("bc", []))).transaction_id
    )
    with pytest.raises(ValueError, match="at least one child"):
        CompositeSinkCommittable(())
    with pytest.raises(TypeError, match="must be SinkCommittable"):
        CompositeSinkCommittable((first, object()))


def test_composite_commit_stops_on_failure_and_retry_starts_from_first_child() -> None:
    events = []
    failure = RuntimeError("commit failed")
    first = _TrackedCommittable("first", events)
    second = _TrackedCommittable("second", events, commit_error=failure)
    third = _TrackedCommittable("third", events)
    composite = CompositeSinkCommittable((first, second, third))

    with pytest.raises(RuntimeError) as captured:
        composite.commit()

    assert captured.value is failure
    assert events == ["commit:first", "commit:second"]
    second.commit_error = None
    composite.commit()
    assert events == ["commit:first", "commit:second", "commit:first", "commit:second", "commit:third"]


def test_composite_abort_attempts_every_child_and_raises_first_error() -> None:
    events = []
    first_error = RuntimeError("first abort failed")
    later_error = ValueError("later abort failed")
    composite = CompositeSinkCommittable(
        (
            _TrackedCommittable("first", events, abort_error=first_error),
            _TrackedCommittable("second", events, abort_error=later_error),
            _TrackedCommittable("third", events),
        )
    )

    with pytest.raises(RuntimeError) as captured:
        composite.abort()

    assert captured.value is first_error
    assert events == ["abort:first", "abort:second", "abort:third"]


def test_composite_abort_succeeds_after_aborting_every_child() -> None:
    events = []
    composite = CompositeSinkCommittable((_TrackedCommittable("first", events), _TrackedCommittable("second", events)))

    composite.abort()

    assert events == ["abort:first", "abort:second"]
