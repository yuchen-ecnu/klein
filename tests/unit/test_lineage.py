# SPDX-License-Identifier: Apache-2.0
"""Tests for Klein lineage: extractors + tracker"""

from unittest.mock import MagicMock, patch

from ray.klein.api.ray_data.call import RayDataCall
from ray.klein.observability.lineage.models import DatasetInfo


def _make_mock_node(fn_cls=object, args=(), kwargs=None, lowering=None):
    node = MagicMock()
    logical_function = MagicMock()
    logical_function.function = fn_cls
    logical_function.constructor_args = args
    logical_function.constructor_kwargs = kwargs or {}
    logical_function.batch_lowering = lowering
    operator = MagicMock()
    operator.logical_function = logical_function
    node.operator = operator
    return node


def _make_mock_graph(source_nodes=None, sink_nodes=None):
    graph = MagicMock()
    source_ids = list(range(len(source_nodes or [])))
    sink_ids = list(range(100, 100 + len(sink_nodes or [])))
    graph.source_nodes = set(source_ids)
    graph.sink_nodes = set(sink_ids)
    nodes = dict(enumerate(source_nodes or []))
    nodes.update(dict(enumerate(sink_nodes or [], start=100)))
    graph.nodes = nodes
    return graph


class TestKafkaExtraction:
    def test_ray_data_kafka_source(self):
        from ray.klein.observability.lineage.extractors import (
            extract_datasets_from_klein_graph,
        )

        call = RayDataCall.module_function(
            "read_kafka",
            (["topic-a", "topic-b"],),
            {"bootstrap_servers": ["host1:9092", "host2:9092"]},
        )
        node = _make_mock_node(lowering=call)
        graph = _make_mock_graph(source_nodes=[node])
        inputs, outputs = extract_datasets_from_klein_graph(graph)
        assert outputs == []
        assert inputs == [DatasetInfo("kafka", "topic-a,topic-b", "host1:9092,host2:9092")]

    def test_ray_data_kafka_sink(self):
        from ray.klein.observability.lineage.extractors import (
            extract_datasets_from_klein_graph,
        )

        call = RayDataCall.dataset_method(
            "write_kafka",
            ("output-topic", "host:9092"),
            {},
            expects_dataset=False,
        )
        node = _make_mock_node(lowering=call)
        graph = _make_mock_graph(sink_nodes=[node])
        inputs, outputs = extract_datasets_from_klein_graph(graph)

        assert inputs == []
        assert outputs == [DatasetInfo("kafka", "output-topic", "host:9092")]


class TestRedisExtraction:
    def test_redis_sink(self):
        from ray.klein.integrations.redis import RedisConnectionConfig, RedisSink
        from ray.klein.observability.lineage.extractors import (
            extract_datasets_from_klein_graph,
        )

        connection = RedisConnectionConfig("10.0.0.1", port=6380, database=2)
        node = _make_mock_node(RedisSink, args=(connection,))
        graph = _make_mock_graph(sink_nodes=[node])
        _, outputs = extract_datasets_from_klein_graph(graph)
        assert outputs == [DatasetInfo("redis", "redis://10.0.0.1:6380/2")]


class TestExtractEdgeCases:
    def test_empty_graph(self):
        from ray.klein.observability.lineage.extractors import (
            extract_datasets_from_klein_graph,
        )

        inputs, outputs = extract_datasets_from_klein_graph(_make_mock_graph())
        assert inputs == [] and outputs == []


class TestKleinLineageTracker:
    def _make_tracker(self, inputs=None, outputs=None, submission_id="raysubmit_test"):
        from ray.klein.observability.lineage.tracker import KleinLineageTracker

        mock_emitter = MagicMock()
        with patch.object(KleinLineageTracker, "_extract") as mock_extract:
            tracker = KleinLineageTracker(submission_id, emitter=mock_emitter)
            tracker._run_id = "test-run-id"
            mock_extract.return_value = (inputs or [], outputs or [])
            tracker.initialize(stream_graph=MagicMock())
        return tracker, mock_emitter

    def _make_disabled_tracker(self):
        from ray.klein.observability.lineage.tracker import KleinLineageTracker

        mock_emitter = MagicMock()
        tracker = KleinLineageTracker("test-job")
        return tracker, mock_emitter

    def test_has_lineage(self):
        t1, _ = self._make_tracker(inputs=[DatasetInfo("kafka", "topic")])
        assert t1.has_lineage is True

        t2, _ = self._make_tracker()
        assert t2.has_lineage is False

    def test_disabled_tracker_has_no_lineage(self):
        t, _ = self._make_disabled_tracker()
        assert t.has_lineage is False

    def test_report_start_complete_fail(self):
        tracker, emitter = self._make_tracker(
            inputs=[DatasetInfo("kafka", "in")],
            outputs=[DatasetInfo("kafka", "out")],
        )

        tracker.report_start()
        assert emitter.emit.call_args[0][0] == "START"

        emitter.emit.reset_mock()
        tracker.report_complete()
        assert emitter.emit.call_args[0][0] == "COMPLETE"

        emitter.emit.reset_mock()
        tracker.report_fail(RuntimeError("boom"))
        assert emitter.emit.call_args[0][0] == "FAIL"

        emitter.emit.reset_mock()
        tracker.report_cancel(RuntimeError("cancelled"))
        assert emitter.emit.call_args[0][0] == "CANCEL"

    def test_no_lineage_skips_all(self):
        tracker, emitter = self._make_tracker()
        tracker.report_start()
        tracker.report_complete()
        tracker.report_fail(RuntimeError("err"))
        tracker.report_cancel(RuntimeError("cancelled"))
        emitter.emit.assert_not_called()

    def test_disabled_tracker_skips_all(self):
        tracker, emitter = self._make_disabled_tracker()
        tracker.report_start()
        tracker.report_complete()
        tracker.report_fail(RuntimeError("err"))
        tracker.report_cancel(RuntimeError("cancelled"))
        emitter.emit.assert_not_called()
