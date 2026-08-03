# SPDX-License-Identifier: Apache-2.0
from datetime import timedelta

import pyarrow as pa

from ray.klein.api.collector import Collector
from ray.klein.api.watermark_strategy import WatermarkStrategy
from ray.klein.runtime.context.source_context import RuntimeSourceContext
from ray.klein.runtime.event_time.input_watermark_tracker import InputWatermarkTracker
from ray.klein.runtime.message import InputActive, InputIdle, Record, Watermark
from ray.klein.runtime.operator.watermark_operator import WatermarkOperator
from ray.klein.runtime.partitioning import ForwardPartitioner
from tests.unit.task_output_utils import open_task_output


class RecordingCollector(Collector):
    def __init__(self):
        super().__init__()
        self.records = []

    def collect(self, record) -> None:
        self.records.append(record)


def test_multi_input_watermark_excludes_idle_inputs_and_reactivates():
    tracker = InputWatermarkTracker(("left", "right"))

    assert tracker.on_control("left", Watermark(100)) == ()
    assert tracker.on_control("right", Watermark(80)) == (Watermark(80),)
    assert tracker.on_control("right", InputIdle()) == (Watermark(100),)
    assert tracker.on_control("left", InputIdle()) == (InputIdle(),)
    assert tracker.is_idle

    assert tracker.on_control("right", InputActive(120)) == (
        InputActive(100),
        Watermark(120),
    )
    assert not tracker.is_idle
    assert tracker.current_watermark == 120


def test_active_input_with_no_resume_watermark_blocks_minimum():
    tracker = InputWatermarkTracker(("a", "b"))
    tracker.on_control("a", InputIdle())
    tracker.on_control("b", InputIdle())

    assert tracker.on_control("a", InputActive()) == (InputActive(),)
    assert tracker.on_control("a", Watermark(7)) == (Watermark(7),)


def test_watermark_implicitly_reactivates_an_idle_input_even_when_stale():
    tracker = InputWatermarkTracker(("input",))
    assert tracker.on_control("input", Watermark(10)) == (Watermark(10),)
    assert tracker.on_control("input", InputIdle()) == (InputIdle(),)

    assert tracker.on_control("input", Watermark(10)) == (InputActive(10),)


def test_reconfigure_inputs_advances_after_removing_the_slowest_input():
    tracker = InputWatermarkTracker(("fast", "slow"))
    assert tracker.on_control("fast", Watermark(100)) == ()
    assert tracker.on_control("slow", Watermark(10)) == (Watermark(10),)

    assert tracker.reconfigure_inputs(("fast",)) == (Watermark(100),)
    assert tracker.current_watermark == 100


def test_reconfigure_inputs_waits_for_a_new_input_before_advancing():
    tracker = InputWatermarkTracker(("existing",))
    assert tracker.on_control("existing", Watermark(100)) == (Watermark(100),)

    assert tracker.reconfigure_inputs(("existing", "new")) == ()
    assert tracker.on_control("existing", Watermark(200)) == ()
    assert tracker.on_control("new", Watermark(150)) == (Watermark(150),)


def test_reconfigure_inputs_can_restore_progress_after_topology_rollback():
    tracker = InputWatermarkTracker(("fast", "slow"))
    tracker.on_control("fast", Watermark(100))
    tracker.on_control("slow", Watermark(10))
    snapshot = tracker.snapshot_state()

    assert tracker.reconfigure_inputs(("fast",)) == (Watermark(100),)
    tracker.restore_state(snapshot)

    assert tracker.current_watermark == 10
    assert tracker.on_control("slow", Watermark(20)) == (Watermark(20),)


def test_reconfigure_inputs_updates_aggregate_idleness():
    tracker = InputWatermarkTracker(("active", "idle"))
    tracker.on_control("active", Watermark(100))
    tracker.on_control("idle", Watermark(10))
    assert tracker.on_control("idle", InputIdle()) == (Watermark(100),)

    assert tracker.reconfigure_inputs(("idle",)) == (InputIdle(),)
    assert tracker.is_idle

    assert tracker.reconfigure_inputs(("idle", "new")) == (InputActive(100),)
    assert not tracker.is_idle


def test_source_context_emits_idle_active_and_watermark_in_order():
    collector = RecordingCollector()
    context = RuntimeSourceContext(collector)

    context.emit_watermark(10)
    context.on_idle()
    context.collect({"id": 1})

    assert collector.records == [
        Watermark(10),
        InputIdle(),
        InputActive(10),
        Record({"id": 1}),
    ]


def test_source_context_collect_many_uses_one_columnar_transport_record():
    class Downstream:
        def __init__(self):
            self.received = []

        def put(self, records, **_kwargs):
            from ray.klein.runtime.message import PutAck

            self.received.append(records)
            return PutAck(True, 0)

    downstream = Downstream()
    output = open_task_output([downstream], ForwardPartitioner(), (0,), ["downstream"])
    context = RuntimeSourceContext(output)
    emissions = []
    context.bind_record_emitter(lambda emitted, count: emissions.append((emitted, count)))

    context.collect_many([{"id": 1, "value": "a"}, {"id": 2, "value": "b"}])

    assert len(downstream.received) == 1
    record = downstream.received[0][0]
    assert isinstance(record.block, pa.RecordBatch)
    assert record.block.to_pydict() == {"id": [1, 2], "value": ["a", "b"]}
    assert record.num_rows == 2
    assert emissions == [(True, 2)]


def test_source_context_collect_many_keeps_rows_for_a_chained_collector():
    collector = RecordingCollector()
    context = RuntimeSourceContext(collector)

    context.collect_many([{"id": 1}, {"id": 2}])

    assert collector.records == [Record({"id": 1}), Record({"id": 2})]


def test_watermark_strategy_generates_bounded_progress_and_idleness():
    collector = RecordingCollector()
    strategy = WatermarkStrategy.for_bounded_out_of_orderness(
        timedelta(milliseconds=5),
        lambda row: row["ts"],
    ).with_idleness(timedelta(milliseconds=10))
    operator = WatermarkOperator(strategy=strategy)
    operator._collector = collector

    operator.process_element(Record({"ts": 20}))
    operator.process_element(Record({"ts": 18}))
    operator._last_record_monotonic -= 1
    operator.on_idle()
    operator.process_element(Record({"ts": 30}))

    assert collector.records == [
        Record({"ts": 20}),
        Watermark(15),
        Record({"ts": 18}),
        InputIdle(),
        InputActive(15),
        Record({"ts": 30}),
        Watermark(25),
    ]


def test_bounded_strategy_does_not_clamp_a_negative_watermark_to_zero():
    collector = RecordingCollector()
    strategy = WatermarkStrategy.for_bounded_out_of_orderness(
        timedelta(milliseconds=5),
        lambda row: row["ts"],
    )
    operator = WatermarkOperator(strategy=strategy)
    operator._collector = collector

    operator.process_element(Record({"ts": 3}))

    assert collector.records == [Record({"ts": 3})]
