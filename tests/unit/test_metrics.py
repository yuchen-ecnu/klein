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
