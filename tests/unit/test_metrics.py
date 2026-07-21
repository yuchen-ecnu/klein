# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from types import SimpleNamespace

import pytest

from ray.klein._internal.memory import estimate_retained_size
from ray.klein.api.runtime_info import RuntimeInfo
from ray.klein.config.configuration import Configuration
from ray.klein.observability.metrics.metric_catalog import KleinMetrics
from ray.klein.observability.metrics.metric_group import JobMetricGroup
from ray.klein.observability.metrics.metric_spec import MetricKind, MetricSpec
from ray.klein.observability.metrics.task_metrics import TaskMetrics
from ray.klein.runtime.collector.edge_output import DeliveryMode
from ray.klein.runtime.context.runtime_context import TaskRuntimeContext
from ray.klein.runtime.message import Record
from ray.klein.runtime.operator.operator import OneInputOperator, StreamOperator
from ray.klein.runtime.partitioning import ForwardPartitioner
from tests.unit.task_output_utils import open_task_output


class _FakeRayMetric:
    def __init__(self, name, description="", boundaries=None, tag_keys=()):
        self.name = name
        self.description = description
        self.boundaries = boundaries
        self.tag_keys = tag_keys
        self.default_tags = {}
        self.values = []

    def set_default_tags(self, tags):
        self.default_tags = dict(tags)
        return self

    @property
    def info(self):
        return {"name": self.name, "description": self.description}

    def inc(self, value, tags=None):
        self.values.append((value, tags))

    def set(self, value, tags=None):
        self.values.append((value, tags))

    def observe(self, value, tags=None):
        self.values.append((value, tags))


@pytest.fixture()
def fake_ray_metrics(monkeypatch):
    created = []

    def factory(*args, **kwargs):
        metric = _FakeRayMetric(*args, **kwargs)
        created.append(metric)
        return metric

    monkeypatch.setattr("ray.klein.observability.metrics.metrics.ray.util.metrics.Counter", factory)
    monkeypatch.setattr("ray.klein.observability.metrics.metrics.ray.util.metrics.Gauge", factory)
    monkeypatch.setattr("ray.klein.observability.metrics.metrics.ray.util.metrics.Histogram", factory)
    return created


def test_metric_spec_rejects_ambiguous_contracts() -> None:
    with pytest.raises(ValueError, match="invalid built-in metric name"):
        MetricSpec("Records-In", MetricKind.COUNTER, "rows")
    with pytest.raises(ValueError, match="must define boundaries"):
        MetricSpec("latency_ms", MetricKind.HISTOGRAM, "latency")
    with pytest.raises(ValueError, match="unique and increasing"):
        MetricSpec("latency_ms", MetricKind.HISTOGRAM, "latency", (10, 1, 10))
    with pytest.raises(ValueError, match="must have a description"):
        MetricSpec("records", MetricKind.COUNTER, " ")
    with pytest.raises(ValueError, match="only histograms"):
        MetricSpec("records", MetricKind.COUNTER, "rows", (1, 2))


def test_scoped_metric_uses_stable_name_description_and_labels(fake_ray_metrics) -> None:
    job = JobMetricGroup("orders", "job-7")
    task = job.add_task_group("map-1", "Map", 2)
    operator = task.add_operator_group("3", "Filter")

    metric = operator.metric(KleinMetrics.PROCESSING_DURATION_MS)
    raw = fake_ray_metrics[-1]

    assert raw.name == "ray_klein_operator_processing_duration_ms"
    assert raw.description == KleinMetrics.PROCESSING_DURATION_MS.description
    assert raw.boundaries == list(KleinMetrics.PROCESSING_DURATION_MS.boundaries)
    assert raw.default_tags == {
        "job_id": "job-7",
        "job_name": "orders",
        "operator_id": "3",
        "operator_name": "Filter",
        "subtask_index": "2",
        "task_id": "map-1",
        "task_name": "Map",
    }
    assert operator.metric(KleinMetrics.PROCESSING_DURATION_MS) is metric


def test_conflicting_duplicate_metric_registration_fails_fast(fake_ray_metrics) -> None:
    group = JobMetricGroup("orders")
    group.metric(KleinMetrics.RECORDS_IN)

    with pytest.raises(ValueError, match="registered with a different"):
        group.gauge(KleinMetrics.RECORDS_IN.name)


def test_builtin_metric_accessors_enforce_catalogue_kind(fake_ray_metrics) -> None:
    group = JobMetricGroup("orders")

    assert group.builtin_counter(KleinMetrics.RECORDS_IN) is group.metric(KleinMetrics.RECORDS_IN)
    with pytest.raises(ValueError, match="is not a counter"):
        group.builtin_counter(KleinMetrics.PROCESSING_DURATION_MS)
    with pytest.raises(ValueError, match="is not a gauge"):
        group.builtin_gauge(KleinMetrics.RECORDS_IN)
    with pytest.raises(ValueError, match="is not a histogram"):
        group.builtin_histogram(KleinMetrics.RECORDS_IN)


class _NoopOperator(StreamOperator, OneInputOperator):
    def process_element(self, record: Record) -> None:
        return None


def test_operator_facade_counts_columnar_rows_and_observes_duration(fake_ray_metrics) -> None:
    task_group = JobMetricGroup("orders").add_task_group("1", "Map", 0)
    context = TaskRuntimeContext(
        "Map",
        0,
        1,
        Configuration(),
        task_group,
        SimpleNamespace(),
        RuntimeInfo(),
    )
    operator = _NoopOperator()
    operator.id = 1
    operator.name = "Map"
    operator.open(None, context)

    record = Record({"value": [1, 2, 3]}, num_rows=3)
    expected_bytes = estimate_retained_size(record)
    operator.invoke_process(record)

    assert operator.records_in == 3
    assert operator.bytes_in == expected_bytes
    by_name = {metric.name: metric for metric in fake_ray_metrics}
    assert by_name["ray_klein_operator_records_in"].values == [(3, None)]
    assert by_name["ray_klein_operator_bytes_in"].values == [(expected_bytes, None)]
    assert len(by_name["ray_klein_operator_processing_duration_ms"].values) == 1
    assert by_name["ray_klein_operator_processing_duration_ms"].values[0][0] >= 0


def test_task_output_counts_columnar_rows() -> None:
    output = open_task_output(
        [object()],
        ForwardPartitioner(),
        (0,),
        ["downstream"],
        delivery_mode=DeliveryMode.PIPELINED,
    )
    record = Record({"value": [1, 2, 3, 4]}, num_rows=4)
    expected_bytes = estimate_retained_size(record)
    output.collect(record)
    assert output.records_out == 4
    assert output.bytes_out == expected_bytes


def test_metric_wrappers_validate_and_keep_local_aggregates(fake_ray_metrics, monkeypatch) -> None:
    group = JobMetricGroup("orders")
    counter = group.counter("accepted")
    gauge = group.gauge("queue_depth")
    histogram = group.histogram("latency", [1, 10, 100])

    assert histogram.mean == 0
    assert counter.info["name"].endswith("accepted")
    assert gauge.info["name"].endswith("queue_depth")
    assert histogram.info["name"].endswith("latency")

    counter.inc(0)
    counter.inc(2, {"partition": "1"})
    assert counter.value == 2
    assert fake_ray_metrics[0].values == [(2, {"partition": "1"})]
    with pytest.raises(ValueError, match="non-negative"):
        counter.inc(-1)

    gauge.set(3)
    gauge.mark_idle({"partition": "1"})
    assert gauge.value == 0
    assert fake_ray_metrics[1].values == [(3, None), (0, {"partition": "1"})]

    ticks = iter((2.025, 3.0, 3.010))
    monkeypatch.setattr("ray.klein.observability.metrics.metrics.time.monotonic", lambda: next(ticks))
    assert histogram.observe_elapsed(2.0) == pytest.approx(25.0)
    with histogram.time({"phase": "commit"}):
        pass
    histogram.observe(5)
    assert (histogram.count, histogram.last, histogram.maximum, histogram.mean) == pytest.approx((3, 5, 25, 40 / 3))
    with pytest.raises(ValueError, match="non-negative"):
        histogram.observe(-0.1)


def test_metric_group_rejects_invalid_names_collisions_and_reconfiguration(fake_ray_metrics) -> None:
    group = JobMetricGroup("orders")
    with pytest.raises(ValueError, match="cannot be blank"):
        group.counter(" ")
    with pytest.raises(ValueError, match="unique and increasing"):
        group.histogram("latency", [10, 1])
    with pytest.raises(ValueError, match="cannot be blank"):
        group.add_group("")

    child = group.add_group("reader", {"partition": 1})
    assert child is group.add_group("reader", {"partition": "1"})
    child_counter = child.counter("accepted", {"shard": 2})
    assert child_counter.info["name"] == "ray_klein_job_reader_accepted"
    assert fake_ray_metrics[-1].default_tags["shard"] == "2"
    with pytest.raises(ValueError, match="different labels"):
        group.add_group("reader", {"partition": 2})
    with pytest.raises(ValueError, match="already contains subgroup"):
        group.counter("reader")

    group.counter("accepted")
    with pytest.raises(ValueError, match="already contains metric"):
        group.add_group("accepted")
    group.close()
    group.close()
    with pytest.raises(RuntimeError, match="closed"):
        child.gauge("depth")
    with pytest.raises(RuntimeError, match="closed"):
        group.add_group("writer")


def test_job_and_operator_metric_groups_are_stable_and_unambiguous(fake_ray_metrics) -> None:
    job = JobMetricGroup("orders")
    first = job.add_task_group("map", "Map", 0)
    second = job.add_task_group("map", "Map", 1)
    assert job.add_task_group("map", "Map", 0) is first
    assert job.task_group("map", 1) is second
    reduce_group = job.add_task_group("reduce", "Reduce", 0)
    assert job.task_group("reduce") is reduce_group
    with pytest.raises(ValueError, match="different name"):
        job.add_task_group("map", "Other", 0)
    with pytest.raises(ValueError, match="specify subtask_index"):
        job.task_group("map")
    with pytest.raises(ValueError, match="maps to 0"):
        job.task_group("missing")

    operator = first.add_operator_group("1", "Filter")
    assert first.add_operator_group("1", "Filter") is operator
    assert first.operator_group("1") is operator
    with pytest.raises(ValueError, match="different name"):
        first.add_operator_group("1", "Map")


def test_task_metrics_initialize_and_saturate_buffer_utilization(fake_ray_metrics) -> None:
    task_group = JobMetricGroup("orders").add_task_group("1", "source", 0)
    metrics = TaskMetrics.create(task_group, 10, 100, 4)

    assert metrics.input_buffer_capacity_records.value == 10
    assert metrics.input_buffer_capacity_bytes.value == 100
    assert metrics.emit_queue_capacity_batches.value == 4
    assert metrics.input_buffer_records.value == 0
    assert metrics.emit_queue_batches.value == 0
    assert metrics.transport_inflight_requests.value == 0
    assert metrics.idle_inputs.value == 0
    assert metrics.replay_buffer_records.value == 0
    assert metrics.replay_buffer_bytes.value == 0

    metrics.update_input_buffer(15, 10, 5, 0)
    assert metrics.input_buffer_records.value == 15
    assert metrics.input_buffer_utilization.value == 1
    assert metrics.input_buffer_bytes.value == 5
    assert metrics.input_buffer_byte_utilization.value == 0

    metrics.update_input_buffer(1, 0, 250, 100)
    assert metrics.input_buffer_utilization.value == 0
    assert metrics.input_buffer_byte_utilization.value == 1


def test_task_metrics_barrier_and_watermark_time_are_clamped(fake_ray_metrics, monkeypatch) -> None:
    task_group = JobMetricGroup("orders").add_task_group("1", "source", 0)
    metrics = TaskMetrics.create(task_group, 0, 0, 0, initialize=False)
    monkeypatch.setattr("ray.klein.observability.metrics.task_metrics.time.time", lambda: 10.0)

    metrics.observe_barrier(None)
    assert metrics.checkpoint_barrier_latency_ms.count == 0
    metrics.observe_barrier(10_100)
    metrics.observe_barrier(9_500)
    assert metrics.checkpoint_barrier_latency_ms.count == 2
    assert metrics.checkpoint_barrier_latency_ms.last == 500

    metrics.update_watermarks(-1, -1, 2)
    assert metrics.current_input_watermark_ms.value == 0
    assert metrics.current_output_watermark_ms.value == 0
    assert metrics.idle_inputs.value == 2
    metrics.update_watermarks(8_000, 9_000, 0)
    assert metrics.current_input_watermark_ms.value == 8_000
    assert metrics.current_output_watermark_ms.value == 9_000
    assert metrics.watermark_lag_ms.value == 1_000
