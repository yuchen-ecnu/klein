# SPDX-License-Identifier: Apache-2.0
"""Unit tests for JobHealthReport, the failover supervisor's health snapshot.

The report aggregates per-task liveness (settling terminal vertices from local
state, probing the rest with a batched health RPC) plus the progress
coordinator's health. Tests drive it with mock execution vertices and patch the
two external calls — the batched ``klein.get`` and the coordinator health
static — so the aggregation/degradation logic is tested without Ray.
"""

import unittest
from unittest import mock

from ray.klein.runtime.execution_graph.execution_vertex_status import (
    ExecutionVertexStatus,
)
from ray.klein.runtime.job_manager import liveness_report as lr
from ray.klein.runtime.job_manager.liveness_report import JobHealthReport


class _FakeStreamTask:
    def __init__(self, health):
        # health is the (is_healthy, reason) tuple returned via klein.get.
        self._health = health

    def health_info(self):
        # Stands in for the remote ObjectRef; klein.get is patched to read it.
        return self._health


class _FakeVertex:
    def __init__(self, name, status, stream_task=None):
        self.name = name
        self._status = status
        self.stream_task = stream_task

    @property
    def status(self):
        return self._status


class _FakeGraph:
    def __init__(self, vertices, namespace="test"):
        self._vertices = vertices
        self.namespace = namespace

    @property
    def execution_vertices(self):
        return self._vertices


def _fake_klein_get(reqs, *, timeout=None):
    # The batched path passes a list of the ObjectRef stand-ins (the health
    # tuples themselves, since health_info returns them directly).
    return list(reqs)


class JobHealthReportTest(unittest.TestCase):
    def _build(self, vertices, coordinator_healthy=True, namespace="test"):
        graph = _FakeGraph(vertices, namespace=namespace)
        with (
            mock.patch.object(
                lr.CheckpointCoordinator,
                "coordinator_healthy",
                return_value=coordinator_healthy,
            ),
            mock.patch.object(lr.klein, "get", side_effect=_fake_klein_get),
        ):
            return JobHealthReport(graph)

    def test_all_running_is_healthy(self):
        vs = [
            _FakeVertex("a", ExecutionVertexStatus.RUNNING, _FakeStreamTask((True, ""))),
            _FakeVertex("b", ExecutionVertexStatus.RUNNING, _FakeStreamTask((True, ""))),
        ]
        report = self._build(vs)
        self.assertTrue(report.healthy)
        self.assertTrue(report.all_tasks_running)
        self.assertEqual(report.tasks_not_running, [])
        self.assertTrue(report.coordinator_healthy)

    def test_failed_vertex_settled_without_probe(self):
        vs = [
            _FakeVertex("ok", ExecutionVertexStatus.RUNNING, _FakeStreamTask((True, ""))),
            _FakeVertex("dead", ExecutionVertexStatus.FAILED, _FakeStreamTask((True, ""))),
        ]
        report = self._build(vs)
        self.assertFalse(report.healthy)
        self.assertEqual(report.tasks_not_running, ["dead"])

    def test_finished_vertex_is_healthy(self):
        vs = [_FakeVertex("done", ExecutionVertexStatus.FINISHED)]
        report = self._build(vs)
        self.assertTrue(report.all_tasks_running)

    def test_missing_stream_task_is_unhealthy(self):
        # A non-terminal vertex with no actor handle (mid-teardown) is unhealthy.
        vs = [_FakeVertex("half", ExecutionVertexStatus.DEPLOYED, stream_task=None)]
        report = self._build(vs)
        self.assertFalse(report.all_tasks_running)
        self.assertEqual(report.tasks_not_running, ["half"])

    def test_unhealthy_probe_result_marks_task_down(self):
        vs = [
            _FakeVertex(
                "sick",
                ExecutionVertexStatus.RUNNING,
                _FakeStreamTask((False, "inbox wedged")),
            )
        ]
        report = self._build(vs)
        self.assertFalse(report.all_tasks_running)
        self.assertEqual(report.tasks_not_running, ["sick"])

    def test_coordinator_down_fails_overall_health(self):
        vs = [_FakeVertex("a", ExecutionVertexStatus.RUNNING, _FakeStreamTask((True, "")))]
        report = self._build(vs, coordinator_healthy=False)
        self.assertTrue(report.all_tasks_running)
        self.assertFalse(report.coordinator_healthy)
        self.assertFalse(report.healthy)

    def test_batched_get_failure_degrades_to_per_request(self):
        # If the batched klein.get raises, the report must fall back to resolving
        # each request individually so one dead actor can't blind the others.
        good = _FakeStreamTask((True, ""))
        bad = _FakeStreamTask((True, ""))  # second per-request call will raise

        graph = _FakeGraph(
            [
                _FakeVertex("good", ExecutionVertexStatus.RUNNING, good),
                _FakeVertex("bad", ExecutionVertexStatus.RUNNING, bad),
            ]
        )

        call_count = {"n": 0}

        def flaky_get(reqs, *, timeout=None):
            # First call is the batch (list) -> raise. Subsequent are per-request.
            if isinstance(reqs, list):
                raise RuntimeError("batch resolution failed")
            call_count["n"] += 1
            if call_count["n"] == 2:
                raise RuntimeError("actor dead")
            return reqs

        with (
            mock.patch.object(
                lr.CheckpointCoordinator,
                "coordinator_healthy",
                return_value=True,
            ),
            mock.patch.object(lr.klein, "get", side_effect=flaky_get),
        ):
            report = JobHealthReport(graph)

        # First probe resolved healthy; second raised -> marked unhealthy.
        self.assertEqual(report.tasks_not_running, ["bad"])

    def test_summary_contains_status_lines(self):
        vs = [_FakeVertex("dead", ExecutionVertexStatus.FAILED)]
        report = self._build(vs)
        text = report.summary()
        self.assertIn("healthy=False", text)
        self.assertIn("tasks_healthy=False", text)
        self.assertIn("coordinator_healthy=True", text)
        self.assertIn("dead", text)
