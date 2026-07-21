# SPDX-License-Identifier: Apache-2.0
from datetime import timedelta

import pytest

from ray.klein.api.collector import Collector
from ray.klein.api.keyed_process_function import KeyedProcessFunction
from ray.klein.api.runtime_info import RuntimeInfo
from ray.klein.api.session_window import SessionWindow
from ray.klein.api.sliding_window import SlidingWindow
from ray.klein.api.time_window import TimeWindow
from ray.klein.api.tumbling_window import TumblingWindow
from ray.klein.config.configuration import Configuration
from ray.klein.config.state_options import StateOptions
from ray.klein.observability.metrics.metric_group import JobMetricGroup
from ray.klein.runtime.context.runtime_context import TaskRuntimeContext
from ray.klein.runtime.message import Record
from ray.klein.runtime.operator.interval_join_operator import IntervalJoinOperator
from ray.klein.runtime.operator.keyed_process_operator import KeyedProcessOperator
from ray.klein.runtime.operator.window_operator import WindowOperator
from ray.klein.state.key_group_range import key_group_for_key, key_group_owner
from ray.klein.state.keyed_state_context import KeyedStateContext
from ray.klein.state.list_state_descriptor import ListStateDescriptor
from ray.klein.state.map_state_descriptor import MapStateDescriptor
from ray.klein.state.memory_state_backend import MemoryStateBackend
from ray.klein.state.timer_domain import TimerDomain
from ray.klein.state.timer_event import TimerEvent
from ray.klein.state.timer_service import TimerService
from ray.klein.state.value_state_descriptor import ValueStateDescriptor


class RecordingCollector(Collector):
    def __init__(self):
        super().__init__()
        self.records = []

    def collect(self, record: Record) -> None:
        self.records.append(record)


class RunningTotal(KeyedProcessFunction):
    descriptor = ValueStateDescriptor("running-total")

    def process(self, value, context):
        state = context.state(self.descriptor)
        total = (state.value or 0) + value["value"]
        state.value = total
        return {"key": value["key"], "total": total}


class ImmediateTimer(KeyedProcessFunction):
    def process(self, value, context):
        context.timer_service.register_processing_time_timer(context.timer_service.current_processing_time)

    def on_timer(self, event, context):
        return {"key": context.current_key, "fired_at": event.timestamp}


def test_state_handles_use_standard_python_protocols():
    backend = MemoryStateBackend()
    context = KeyedStateContext(backend, TimerService(backend)).bind("key", None)

    value = context.state(ValueStateDescriptor("value"))
    value.value = 3
    assert value.value == 3

    values = context.state(ListStateDescriptor("values"))
    values.append(1)
    values.extend([2, 3])
    values[1] = 4
    assert list(values) == [1, 4, 3]

    mapping = context.state(MapStateDescriptor("mapping"))
    mapping["a"] = 1
    mapping.update({"b": 2})
    del mapping["a"]
    assert dict(mapping) == {"b": 2}


def _open(operator, task_name="stateful-task"):
    config = Configuration()
    config.set(StateOptions.BACKEND, "memory")
    metrics = JobMetricGroup("stateful-test").add_task_group("1", task_name, 0)
    context = TaskRuntimeContext(
        task_name,
        0,
        1,
        config,
        metrics,
        None,
        RuntimeInfo(),
        "stateful-test",
    )
    collector = RecordingCollector()
    operator.id = 1
    operator.name = task_name
    operator.open(collector, context)
    return collector


def _sum_window_operator(assigner, *, allowed_lateness=timedelta(0)):
    return WindowOperator(
        key_selector=lambda row: row["key"],
        timestamp_selector=lambda row: row["ts"],
        assigner=assigner,
        reduce_function=lambda left, right: {
            "key": left["key"],
            "value": left["value"] + right["value"],
            "ts": max(left["ts"], right["ts"]),
        },
        allowed_lateness=allowed_lateness,
    )


def test_keyed_process_state_snapshot_round_trip():
    operator = KeyedProcessOperator(
        key_selector=lambda row: row["key"],
        process_function=RunningTotal(),
    )
    collector = _open(operator)
    operator.process_element(Record({"key": "a", "value": 1}))
    snapshot = operator.snapshot_state()
    operator.process_element(Record({"key": "a", "value": 10}))
    operator.close()

    restored = KeyedProcessOperator(
        key_selector=lambda row: row["key"],
        process_function=RunningTotal(),
    )
    restored_collector = _open(restored, "restored-task")
    restored.restore_state(snapshot)
    restored.process_element(Record({"key": "a", "value": 2}))

    assert [record.block for record in collector.records] == [
        {"key": "a", "total": 1},
        {"key": "a", "total": 11},
    ]
    assert [record.block for record in restored_collector.records] == [{"key": "a", "total": 3}]
    restored.close()


def test_keyed_state_checkpoint_rescales_by_stable_key_group():
    max_parallelism = 16

    def open_subtask(operator, index, parallelism, label):
        config = Configuration()
        config.set(StateOptions.BACKEND, "memory")
        config.set(StateOptions.MAX_PARALLELISM, max_parallelism)
        metrics = JobMetricGroup(label).add_task_group("1", label, index)
        context = TaskRuntimeContext(
            label,
            index,
            parallelism,
            config,
            metrics,
            None,
            RuntimeInfo(),
            label,
        )
        collector = RecordingCollector()
        operator.id = 1
        operator.name = label
        operator.open(collector, context)
        return collector

    keys = [f"key-{index}" for index in range(30)]
    old_operators = []
    old_snapshots = []
    for index in range(2):
        operator = KeyedProcessOperator(
            key_selector=lambda row: row["key"],
            process_function=RunningTotal(),
        )
        open_subtask(operator, index, 2, f"old-{index}")
        for key in keys:
            group = key_group_for_key(key, max_parallelism)
            if key_group_owner(group, max_parallelism, 2) == index:
                operator.process_element(Record({"key": key, "value": 1}))
        old_snapshots.append(operator.snapshot_state())
        old_operators.append(operator)

    observed = {}
    for index in range(3):
        operator = KeyedProcessOperator(
            key_selector=lambda row: row["key"],
            process_function=RunningTotal(),
        )
        collector = open_subtask(operator, index, 3, f"new-{index}")
        operator.restore_state_fragments(old_snapshots)
        for key in keys:
            group = key_group_for_key(key, max_parallelism)
            if key_group_owner(group, max_parallelism, 3) == index:
                operator.process_element(Record({"key": key, "value": 2}))
        observed.update({record.block["key"]: record.block["total"] for record in collector.records})
        operator.close()

    assert observed == dict.fromkeys(keys, 3)
    for operator in old_operators:
        operator.close()


def test_pending_state_restore_metrics_are_published_only_on_commit() -> None:
    source = KeyedProcessOperator(
        key_selector=lambda row: row["key"],
        process_function=RunningTotal(),
    )
    _open(source, "metric-source")
    source.process_element(Record({"key": "a", "value": 1}))
    snapshot = source.snapshot_state()

    pending = KeyedProcessOperator(
        key_selector=lambda row: row["key"],
        process_function=RunningTotal(),
    )
    _open(pending, "metric-pending")
    pending.restore_state_fragments((snapshot,), publish_metrics=False)

    assert pending._state_size_metric.value == 0
    assert pending._restore_duration_metric.count == 0
    pending.publish_deferred_restore_metrics()
    assert pending._state_size_metric.value == len(snapshot)
    assert pending._restore_duration_metric.count == 1

    source.close()
    pending.close()


def test_keyed_process_dispatches_processing_time_timer():
    operator = KeyedProcessOperator(
        key_selector=lambda row: row["key"],
        process_function=ImmediateTimer(),
    )
    collector = _open(operator)

    operator.process_element(Record({"key": "a"}))

    assert len(collector.records) == 1
    assert collector.records[0].block["key"] == "a"
    assert collector.records[0].block["fired_at"] >= 0
    operator.close()


def test_tumbling_window_flushes_managed_state_at_end_of_data():
    operator = WindowOperator(
        key_selector=lambda row: row["key"],
        timestamp_selector=lambda row: row["ts"],
        assigner=TumblingWindow(timedelta(seconds=1)),
        reduce_function=lambda left, right: {
            "key": left["key"],
            "value": left["value"] + right["value"],
            "ts": right["ts"],
        },
    )
    collector = _open(operator)
    operator.process_element(Record({"key": "a", "value": 1, "ts": 100}))
    operator.process_element(Record({"key": "a", "value": 2, "ts": 200}))

    assert collector.records == []
    operator.finish()
    assert [record.block for record in collector.records] == [{"key": "a", "value": 3, "ts": 200}]
    operator.close()


def test_tumbling_window_advances_only_on_explicit_watermark():
    operator = WindowOperator(
        key_selector=lambda row: row["key"],
        timestamp_selector=lambda row: row["ts"],
        assigner=TumblingWindow(timedelta(seconds=1)),
        reduce_function=lambda left, right: right,
    )
    collector = _open(operator)
    operator.process_element(Record({"key": "a", "value": 1, "ts": 100}))
    operator.process_element(Record({"key": "a", "value": 2, "ts": 1200}))

    assert collector.records == []
    operator.on_event_time_watermark(999)
    assert [record.block for record in collector.records] == [{"key": "a", "value": 1, "ts": 100}]
    operator.close()


def test_tumbling_window_drops_events_after_cleanup_watermark():
    operator = WindowOperator(
        key_selector=lambda row: row["key"],
        timestamp_selector=lambda row: row["ts"],
        assigner=TumblingWindow(timedelta(seconds=1)),
        reduce_function=lambda left, right: right,
    )
    collector = _open(operator)

    operator.on_event_time_watermark(999)
    operator.process_element(Record({"key": "a", "value": 1, "ts": 100}))
    operator.finish()

    assert collector.records == []
    operator.close()


def test_tumbling_window_accepts_late_data_until_exact_cleanup_boundary():
    operator = _sum_window_operator(
        TumblingWindow(timedelta(seconds=1)),
        allowed_lateness=timedelta(milliseconds=500),
    )
    collector = _open(operator)
    operator.process_element(Record({"key": "a", "value": 1, "ts": 100}))

    operator.on_event_time_watermark(999)
    operator.process_element(Record({"key": "a", "value": 2, "ts": 200}))
    operator.on_event_time_watermark(1_498)
    assert collector.records == []

    operator.on_event_time_watermark(1_499)
    assert [record.block for record in collector.records] == [{"key": "a", "value": 3, "ts": 200}]

    operator.process_element(Record({"key": "a", "value": 4, "ts": 300}))
    operator.finish()
    assert [record.block for record in collector.records] == [{"key": "a", "value": 3, "ts": 200}]
    assert operator._late_records_metric.value == 1
    operator.close()


def test_sliding_window_emits_each_overlapping_aggregate_in_timer_order():
    operator = _sum_window_operator(
        SlidingWindow(
            timedelta(seconds=1),
            timedelta(milliseconds=500),
        )
    )
    collector = _open(operator)

    operator.process_element(Record({"key": "a", "value": 1, "ts": 0}))
    operator.process_element(Record({"key": "a", "value": 2, "ts": 500}))
    operator.finish()

    assert [record.block for record in collector.records] == [
        {"key": "a", "value": 1, "ts": 0},
        {"key": "a", "value": 3, "ts": 500},
        {"key": "a", "value": 2, "ts": 500},
    ]
    operator.close()


def test_sliding_window_counts_one_late_record_across_overlapping_windows():
    operator = _sum_window_operator(
        SlidingWindow(
            timedelta(seconds=1),
            timedelta(milliseconds=500),
        )
    )
    collector = _open(operator)

    operator.on_event_time_watermark(999)
    operator.process_element(Record({"key": "a", "value": 1, "ts": 100}))
    operator.finish()

    assert collector.records == []
    assert operator._late_records_metric.value == 1
    operator.close()


def test_window_ignores_processing_time_timer_for_event_time_state():
    operator = _sum_window_operator(TumblingWindow(timedelta(seconds=1)))
    collector = _open(operator)
    operator.process_element(Record({"key": "a", "value": 1, "ts": 100}))

    namespace = TimeWindow(0, 1_000)
    operator.timer_service.register_processing_time_timer(0, namespace)
    operator.on_idle()
    assert collector.records == []

    operator.on_event_time_watermark(999)
    assert [record.block for record in collector.records] == [{"key": "a", "value": 1, "ts": 100}]
    assert operator._timers_fired_metric.value == 2
    operator.close()


def test_window_on_timer_ignores_foreign_timer_domain():
    operator = _sum_window_operator(TumblingWindow(timedelta(seconds=1)))
    collector = _open(operator)
    operator.process_element(Record({"key": "a", "value": 1, "ts": 100}))

    operator.on_timer(
        TimerEvent(999, "a", TimeWindow(0, 1_000), TimerDomain.PROCESSING_TIME),
        operator.state_context,
    )
    assert collector.records == []

    operator.on_event_time_watermark(999)
    assert [record.block for record in collector.records] == [{"key": "a", "value": 1, "ts": 100}]
    operator.close()


def test_session_window_merges_overlapping_namespaces():
    operator = WindowOperator(
        key_selector=lambda row: row["key"],
        timestamp_selector=lambda row: row["ts"],
        assigner=SessionWindow(timedelta(milliseconds=100)),
        reduce_function=lambda left, right: {
            "key": left["key"],
            "value": left["value"] + right["value"],
            "ts": right["ts"],
        },
    )
    collector = _open(operator)
    operator.process_element(Record({"key": "a", "value": 1, "ts": 100}))
    operator.process_element(Record({"key": "a", "value": 2, "ts": 150}))

    operator.finish()

    assert [record.block for record in collector.records] == [{"key": "a", "value": 3, "ts": 150}]
    operator.close()


def test_session_window_does_not_merge_events_at_exact_gap_boundary():
    operator = _sum_window_operator(SessionWindow(timedelta(milliseconds=100)))
    collector = _open(operator)
    operator.process_element(Record({"key": "a", "value": 1, "ts": 100}))
    operator.process_element(Record({"key": "a", "value": 2, "ts": 200}))

    assert operator._backend.namespaces(operator._window_state) == (
        TimeWindow(100, 200),
        TimeWindow(200, 300),
    )
    operator.finish()
    assert [record.block for record in collector.records] == [
        {"key": "a", "value": 1, "ts": 100},
        {"key": "a", "value": 2, "ts": 200},
    ]
    operator.close()


def test_session_bridge_event_merges_both_neighboring_sessions():
    operator = _sum_window_operator(SessionWindow(timedelta(milliseconds=100)))
    collector = _open(operator)
    operator.process_element(Record({"key": "a", "value": 1, "ts": 100}))
    operator.process_element(Record({"key": "a", "value": 2, "ts": 250}))
    operator.process_element(Record({"key": "a", "value": 4, "ts": 190}))

    assert operator._backend.namespaces(operator._window_state) == (TimeWindow(100, 350),)
    operator.finish()
    assert [record.block for record in collector.records] == [{"key": "a", "value": 7, "ts": 250}]
    operator.close()


def test_session_late_event_uses_merged_session_cleanup_boundary():
    operator = _sum_window_operator(SessionWindow(timedelta(milliseconds=100)))
    collector = _open(operator)
    operator.process_element(Record({"key": "a", "value": 1, "ts": 100}))
    operator.process_element(Record({"key": "a", "value": 2, "ts": 190}))

    operator.on_event_time_watermark(200)
    operator.process_element(Record({"key": "a", "value": 4, "ts": 50}))
    assert operator._late_records_metric.value == 0

    operator.on_event_time_watermark(289)
    assert [record.block for record in collector.records] == [{"key": "a", "value": 7, "ts": 190}]

    operator.process_element(Record({"key": "a", "value": 8, "ts": 50}))
    assert operator._late_records_metric.value == 1
    operator.close()


def test_window_rejects_sub_millisecond_negative_allowed_lateness():
    with pytest.raises(ValueError, match="allowed_lateness must not be negative"):
        _sum_window_operator(
            TumblingWindow(timedelta(seconds=1)),
            allowed_lateness=timedelta(microseconds=-1),
        )


def test_window_rejects_missing_event_timestamp():
    operator = WindowOperator(
        key_selector=lambda row: row["key"],
        timestamp_selector=lambda row: row.get("ts"),
        assigner=TumblingWindow(timedelta(seconds=1)),
        reduce_function=lambda left, right: right,
    )
    _open(operator)

    with pytest.raises(
        ValueError,
        match="window timestamp_selector must return a non-negative integer",
    ):
        operator.process_element(Record({"key": "a", "value": 1}))
    operator.close()


def test_window_ignores_stale_event_timer_without_state():
    operator = _sum_window_operator(TumblingWindow(timedelta(seconds=1)))
    collector = _open(operator)
    operator.state_context.bind("a", 999)

    operator.on_timer(
        TimerEvent(999, "a", TimeWindow(0, 1_000), TimerDomain.EVENT_TIME),
        operator.state_context,
    )

    assert collector.records == []
    operator.close()


def test_interval_join_routes_left_and_right_tags_and_honors_bounds():
    operator = IntervalJoinOperator(
        left_key=lambda row: row["key"],
        right_key=lambda row: row["key"],
        left_timestamp=lambda row: row["ts"],
        right_timestamp=lambda row: row["ts"],
        lower_bound=timedelta(milliseconds=-10),
        upper_bound=timedelta(milliseconds=10),
        join_function=lambda left, right: {
            "key": left["key"],
            "total": left["left"] + right["right"],
        },
    )
    collector = _open(operator)
    left = Record({"key": "a", "left": 1, "ts": 100})
    left.input_tag = 0
    matching_right = Record({"key": "a", "right": 10, "ts": 105})
    matching_right.input_tag = 1
    late_right = Record({"key": "a", "right": 100, "ts": 200})
    late_right.input_tag = 1

    operator.process_element(left)
    operator.process_element(matching_right)
    operator.process_element(late_right)

    assert [record.block for record in collector.records] == [{"key": "a", "total": 11}]
    operator.close()
