# SPDX-License-Identifier: Apache-2.0
"""Unit tests for JobMaster failover — Tier 0/1/2 recovery.

Covers the complete fault model:
  - Task-level failures: source, sink, sync-op, async-op, UDF-exception
  - Coordinator failures: crash-without-checkpoint, crash-after-checkpoint, RPC timeout
  - JobManager failures: handle_exception, cancel-during-restart, lock contention
  - Ray node failures: ActorUnavailableError, ActorDiedError, NOT_EXIST

All tests use mock actors + fake execution graph — no Ray cluster required.
"""

import unittest
from types import SimpleNamespace
from unittest import mock

from ray.klein.api.stream_task_status import StreamTaskStatus
from ray.klein.config.configuration import Configuration
from ray.klein.config.job_manager_options import JobManagerOptions
from ray.klein.runtime.execution_graph.execution_graph import ExecutionGraph
from ray.klein.runtime.execution_graph.execution_vertex_id import ExecutionVertexId
from ray.klein.runtime.execution_graph.execution_vertex_status import (
    ExecutionVertexStatus,
)
from ray.klein.runtime.scheduler import job_master as js_mod
from ray.klein.runtime.scheduler.errors import DeploymentError
from ray.klein.runtime.scheduler.job_master import JobMaster
from ray.klein.runtime.scheduler.restart_result import RestartStatus

# ---------------------------------------------------------------------------
# Fake / mock infrastructure
# ---------------------------------------------------------------------------
_FAKE_NAMESPACE = "klein-test-abc12345"


class _FakeStreamTask:
    """Stand-in for a remote StreamTask actor handle.

    Controls outcomes: *running* (is_running), *setup_ok* (setup_and_run),
    *replay_ok* (replay_buffered_to). Set a bool to False to simulate
    the corresponding RPC failure.
    """

    def __init__(self, *, running=True, setup_ok=True, replay_ok=True):
        self.running_flag = running
        self.setup_flag = setup_ok
        self.replay_flag = replay_ok
        self.raise_on_is_running = None

    def is_running(self):
        if self.raise_on_is_running is not None:
            raise self.raise_on_is_running
        return self.running_flag

    def setup_and_run(self):
        if not self.setup_flag:
            raise RuntimeError("setup failed")
        return mock.MagicMock()

    def setup_and_run_with_descriptor(self, descriptor):
        del descriptor
        return self.setup_and_run()

    def replay_buffered_to(self, name):
        if not self.replay_flag:
            raise RuntimeError("replay failed")
        return mock.MagicMock()

    def stop(self):
        return mock.MagicMock()


class _FakeVertex:
    """Lightweight ExecutionVertex stand-in.

    Supports the two callers: ``status`` (read-only by scheduler) and
    ``transition_to()`` (by stop_workers).
    """

    def __init__(self, name, status, stream_task=None):
        self.name = name
        self._status = status
        self.stream_task = stream_task
        self.id = ExecutionVertexId(hash(name) % 1000, 0)
        self.index = 0
        self.concurrency = 1
        self.task_generation = f"generation-{name}"
        self.restore_operation_id = None
        self.operator_spec = mock.MagicMock()
        self.config = Configuration(include_environment=False)
        self.task_metric_group = mock.MagicMock()

    @property
    def status(self):
        return self._status

    def transition_to(self, status, error_message=None):
        self._status = status


class _ActorStatusMap:
    """Pin-able replacement for klein.get_actor_status."""

    def __init__(self, mapping=None):
        self._mapping = mapping or {}

    def __call__(self, name, namespace=None):
        return self._mapping.get(name, StreamTaskStatus.NOT_EXIST)


def _build_graph(vertices):
    """Minimal ExecutionGraph carrying only the vertex list + namespace."""
    g = mock.MagicMock(spec=ExecutionGraph)
    g.namespace = _FAKE_NAMESPACE
    g.execution_vertices = vertices
    g.input_job_edges.side_effect = lambda job_vertex_id: []
    g.output_job_edges.side_effect = lambda job_vertex_id: []
    job_vertices = {
        vertex.id.job_vertex_id: SimpleNamespace(
            id=vertex.id.job_vertex_id,
            config=vertex.config,
            concurrency=vertex.concurrency,
            operator_spec=vertex.operator_spec,
            output_queue=None,
        )
        for vertex in vertices
    }
    g.job_vertex.side_effect = job_vertices.__getitem__
    g.barrier_splits = {vertex.id: {} for vertex in vertices}
    g.source_job_vertices = []
    return g


def _config_with_timeouts(start=300, rpc=30):
    c = Configuration()
    c.set(JobManagerOptions.SCHEDULER_START_TIMEOUT, start)
    c.set(JobManagerOptions.COORDINATOR_RPC_TIMEOUT, rpc)
    return c


# ======================================================================
# try_recover_tasks  —  Tier-0 single-task recovery
# ======================================================================
class TryRecoverTasksTest(unittest.TestCase):
    """Tests every vertex-state → recovery-outcome edge of try_recover_tasks."""

    def _scheduler(self, vertices, actor_status_map=None):
        graph = _build_graph(vertices)
        s = JobMaster(graph, _config_with_timeouts())
        self._status_map = _ActorStatusMap(actor_status_map or {})
        return s

    # ----- success paths -------------------------------------------------

    def test_all_running_returns_true_no_recovery(self):
        tasks = [_FakeStreamTask()]
        vs = [_FakeVertex("a", ExecutionVertexStatus.RUNNING, tasks[0])]
        s = self._scheduler(vs, {"a": StreamTaskStatus.ALIVE})
        with (
            mock.patch.object(js_mod.klein, "get_actor_status", side_effect=self._status_map),
            mock.patch.object(js_mod.klein, "get", side_effect=lambda x, **kw: x),
        ):
            self.assertTrue(s.try_recover_tasks())

    def test_all_terminal_returns_true(self):
        vs = [
            _FakeVertex("a", ExecutionVertexStatus.FINISHED),
            _FakeVertex("b", ExecutionVertexStatus.CANCELLED),
        ]
        s = self._scheduler(vs, {"a": StreamTaskStatus.ALIVE, "b": StreamTaskStatus.ALIVE})
        with mock.patch.object(js_mod.klein, "get_actor_status", side_effect=self._status_map):
            self.assertTrue(s.try_recover_tasks())

    def test_dead_actor_returns_true_waiting_for_ray(self):
        tasks = [_FakeStreamTask()]
        vs = [_FakeVertex("a", ExecutionVertexStatus.RUNNING, tasks[0])]
        s = self._scheduler(vs, {"a": StreamTaskStatus.DEAD})
        with mock.patch.object(js_mod.klein, "get_actor_status", side_effect=self._status_map):
            self.assertTrue(s.try_recover_tasks())

    def test_alive_not_running_rebootstraps(self):
        tasks = [_FakeStreamTask(running=False, setup_ok=True)]
        vs = [_FakeVertex("a", ExecutionVertexStatus.DEPLOYED, tasks[0])]
        s = self._scheduler(vs, {"a": StreamTaskStatus.ALIVE})
        with (
            mock.patch.object(js_mod.klein, "get_actor_status", side_effect=self._status_map),
            mock.patch.object(js_mod.klein, "get", side_effect=lambda x, **kw: x),
        ):
            self.assertTrue(s.try_recover_tasks())

    # ----- mixed-state: dead + recoverable --------------------------------

    def test_mixed_dead_and_alive_not_running_recovers_alive_one(self):
        alive = _FakeStreamTask(running=False, setup_ok=True)
        dead = _FakeStreamTask(running=False)
        vs = [
            _FakeVertex("alive", ExecutionVertexStatus.DEPLOYED, alive),
            _FakeVertex("dead", ExecutionVertexStatus.DEPLOYED, dead),
        ]
        s = self._scheduler(vs, {"alive": StreamTaskStatus.ALIVE, "dead": StreamTaskStatus.DEAD})
        with (
            mock.patch.object(js_mod.klein, "get_actor_status", side_effect=self._status_map),
            mock.patch.object(js_mod.klein, "get", side_effect=lambda x, **kw: x),
        ):
            self.assertTrue(s.try_recover_tasks())

    def test_mixed_healthy_and_dead_returns_true(self):
        healthy = _FakeStreamTask()
        dead = _FakeStreamTask(running=False)
        vs = [
            _FakeVertex("healthy", ExecutionVertexStatus.RUNNING, healthy),
            _FakeVertex("dead", ExecutionVertexStatus.DEPLOYED, dead),
        ]
        s = self._scheduler(vs, {"healthy": StreamTaskStatus.ALIVE, "dead": StreamTaskStatus.DEAD})
        with (
            mock.patch.object(js_mod.klein, "get_actor_status", side_effect=self._status_map),
            mock.patch.object(js_mod.klein, "get", side_effect=lambda x, **kw: x),
        ):
            self.assertTrue(s.try_recover_tasks())

    # ----- escalation paths -------------------------------------------------

    def test_failed_vertex_escalates(self):
        vs = [_FakeVertex("a", ExecutionVertexStatus.FAILED)]
        s = self._scheduler(vs, {"a": StreamTaskStatus.ALIVE})
        with mock.patch.object(js_mod.klein, "get_actor_status", side_effect=self._status_map):
            self.assertFalse(s.try_recover_tasks())

    def test_not_exist_escalates(self):
        tasks = [_FakeStreamTask()]
        vs = [_FakeVertex("a", ExecutionVertexStatus.RUNNING, tasks[0])]
        s = self._scheduler(vs, {"a": StreamTaskStatus.NOT_EXIST})
        with mock.patch.object(js_mod.klein, "get_actor_status", side_effect=self._status_map):
            self.assertFalse(s.try_recover_tasks())

    def test_missing_stream_task_escalates(self):
        vs = [_FakeVertex("a", ExecutionVertexStatus.DEPLOYED, stream_task=None)]
        s = self._scheduler(vs, {"a": StreamTaskStatus.ALIVE})
        with mock.patch.object(js_mod.klein, "get_actor_status", side_effect=self._status_map):
            self.assertFalse(s.try_recover_tasks())

    def test_setup_and_run_failure_escalates(self):
        tasks = [_FakeStreamTask(running=False, setup_ok=False)]
        vs = [_FakeVertex("a", ExecutionVertexStatus.DEPLOYED, tasks[0])]
        s = self._scheduler(vs, {"a": StreamTaskStatus.ALIVE})
        with (
            mock.patch.object(js_mod.klein, "get_actor_status", side_effect=self._status_map),
            mock.patch.object(js_mod.klein, "get", side_effect=lambda x, **kw: x),
        ):
            self.assertFalse(s.try_recover_tasks())

    def test_mixed_failed_and_healthy_escalates(self):
        healthy = _FakeStreamTask()
        vs = [
            _FakeVertex("healthy", ExecutionVertexStatus.RUNNING, healthy),
            _FakeVertex("bad", ExecutionVertexStatus.FAILED),
        ]
        s = self._scheduler(vs, {"healthy": StreamTaskStatus.ALIVE, "bad": StreamTaskStatus.ALIVE})
        with (
            mock.patch.object(js_mod.klein, "get_actor_status", side_effect=self._status_map),
            mock.patch.object(js_mod.klein, "get", side_effect=lambda x, **kw: x),
        ):
            self.assertFalse(s.try_recover_tasks())

    # ----- Ray-node failure: ActorUnavailableError vs ActorDiedError ------

    def test_actor_unavailable_error_treated_as_rebuilding(self):
        import ray

        class _MockUnavailable(ray.exceptions.ActorUnavailableError):
            def __init__(self):
                pass

        task = _FakeStreamTask()
        task.raise_on_is_running = _MockUnavailable()
        vs = [_FakeVertex("a", ExecutionVertexStatus.RUNNING, task)]
        s = self._scheduler(vs, {"a": StreamTaskStatus.ALIVE})
        with (
            mock.patch.object(js_mod.klein, "get_actor_status", side_effect=self._status_map),
            mock.patch.object(js_mod.klein, "get", side_effect=lambda x, **kw: x),
        ):
            self.assertTrue(s.try_recover_tasks())

    def test_actor_died_error_escalates(self):
        task = _FakeStreamTask()
        task.raise_on_is_running = RuntimeError("actor died")
        vs = [_FakeVertex("a", ExecutionVertexStatus.RUNNING, task)]
        s = self._scheduler(vs, {"a": StreamTaskStatus.ALIVE})
        with (
            mock.patch.object(js_mod.klein, "get_actor_status", side_effect=self._status_map),
            mock.patch.object(js_mod.klein, "get", side_effect=lambda x, **kw: x),
        ):
            self.assertFalse(s.try_recover_tasks())

    def test_is_running_unexpected_error_escalates(self):
        task = _FakeStreamTask()
        task.raise_on_is_running = RuntimeError("unexpected crash")
        vs = [_FakeVertex("a", ExecutionVertexStatus.RUNNING, task)]
        s = self._scheduler(vs, {"a": StreamTaskStatus.ALIVE})
        with (
            mock.patch.object(js_mod.klein, "get_actor_status", side_effect=self._status_map),
            mock.patch.object(js_mod.klein, "get", side_effect=lambda x, **kw: x),
        ):
            self.assertFalse(s.try_recover_tasks())

    # ----- edge cases ------------------------------------------------------

    def test_empty_graph_returns_true(self):
        s = self._scheduler([])
        with mock.patch.object(js_mod.klein, "get_actor_status", side_effect=self._status_map):
            self.assertTrue(s.try_recover_tasks())

    def test_all_vertices_terminal_or_dead(self):
        vs = [
            _FakeVertex("a", ExecutionVertexStatus.FINISHED),
            _FakeVertex("b", ExecutionVertexStatus.CANCELLED),
            _FakeVertex("c", ExecutionVertexStatus.RUNNING, _FakeStreamTask()),
        ]
        s = self._scheduler(
            vs,
            {
                "a": StreamTaskStatus.ALIVE,
                "b": StreamTaskStatus.ALIVE,
                "c": StreamTaskStatus.DEAD,
            },
        )
        with mock.patch.object(js_mod.klein, "get_actor_status", side_effect=self._status_map):
            self.assertTrue(s.try_recover_tasks())


# ======================================================================
# recover_coordinator_if_needed  —  Tier-1 coordinator recovery
# ======================================================================
class RecoverCoordinatorTest(unittest.TestCase):
    """Coordinator crash/recovery scenarios.

    The coordinator is a Ray async actor with max_restarts=-1. When it crashes
    Ray rebuilds it (__init__ only, empty state); the JobManager health loop
    detects this via needs_recovery() and re-opens it from the last DFS
    checkpoint.
    """

    def _scheduler(self, vertex_names=None):
        graph = _build_graph([])
        base_vs = [_FakeVertex(n, ExecutionVertexStatus.RUNNING) for n in (vertex_names or [])]
        graph.execution_vertices = base_vs
        s = JobMaster(graph, _config_with_timeouts())
        self._coord = mock.MagicMock()
        self._coord.needs_recovery.return_value = False
        self._coord.latest_checkpoint_path.return_value = "/tmp/chk/42"
        self._coord.barrier_epoch_floor.return_value = 100
        s.coordinator = self._coord
        return s

    def _patch_alive_and_get(self, get_values):
        """Returns two started patches; callers must stop them explicitly."""
        p1 = mock.patch.object(js_mod.klein, "get_actor_status", return_value=StreamTaskStatus.ALIVE)
        p2 = mock.patch.object(js_mod.klein, "get", side_effect=get_values)
        p1.start()
        p2.start()
        self.addCleanup(p1.stop)
        self.addCleanup(p2.stop)

    # ----- coordinator not present / not alive ---------------------------

    def test_coordinator_none_returns_false(self):
        s = self._scheduler()
        s.coordinator = None
        self.assertFalse(s.recover_coordinator_if_needed())

    def test_coordinator_not_alive_returns_false(self):
        s = self._scheduler()
        with mock.patch.object(js_mod.klein, "get_actor_status", return_value=StreamTaskStatus.DEAD):
            self.assertFalse(s.recover_coordinator_if_needed())

    def test_no_recovery_needed_returns_false(self):
        s = self._scheduler()
        self._coord.needs_recovery.return_value = False
        self._patch_alive_and_get([True])
        self.assertFalse(s.recover_coordinator_if_needed())

    # ----- successful recovery path -------------------------------------

    def test_successful_recovery_returns_true(self):
        s = self._scheduler(vertex_names=["a", "b"])
        self._coord.needs_recovery.return_value = True

        vals = [True, "/tmp/chk/42", None, None, 100, None, None]
        self._patch_alive_and_get(vals)
        self.assertTrue(s.recover_coordinator_if_needed())
        self._coord.open.assert_called_once()
        self._coord.start.assert_called_once()

    # ----- coordinator crash before first checkpoint ----------------------

    def test_restore_path_none_before_first_checkpoint(self):
        s = self._scheduler()
        self._coord.needs_recovery.return_value = True
        self._coord.latest_checkpoint_path.return_value = None

        vals = [True, None, None, None, 0]
        self._patch_alive_and_get(vals)
        self.assertTrue(s.recover_coordinator_if_needed())
        self._coord.open.assert_called_once()
        self._coord.start.assert_called_once()

    # ----- coordinator recovered but barrier-floor read fails -----------

    def test_barrier_epoch_floor_fails_skips_reclaim(self):
        s = self._scheduler(vertex_names=["a"])
        self._coord.needs_recovery.return_value = True

        vals = [True, "/tmp/chk/42", None, None, RuntimeError("read failed")]
        self._patch_alive_and_get(vals)
        self.assertTrue(s.recover_coordinator_if_needed())

    # ----- individual RPC failures ---------------------------------------

    def test_needs_recovery_timeout_returns_false(self):
        s = self._scheduler()
        self._coord.needs_recovery.return_value = True
        vals = [TimeoutError("RPC timeout")]
        self._patch_alive_and_get(vals)
        self.assertFalse(s.recover_coordinator_if_needed())

    def test_open_failure_catches_and_returns_false(self):
        s = self._scheduler()
        self._coord.needs_recovery.return_value = True
        vals = [True, "/tmp/chk/42", RuntimeError("open failed")]
        self._patch_alive_and_get(vals)
        self.assertFalse(s.recover_coordinator_if_needed())


# ======================================================================
# stop_job  —  worker + coordinator teardown
# ======================================================================
class StopJobTest(unittest.TestCase):
    """Teardown during normal FINISHED / CANCELLED and during restart."""

    def _scheduler(self):
        graph = _build_graph([_FakeVertex("a", ExecutionVertexStatus.RUNNING)])
        s = JobMaster(graph, _config_with_timeouts())
        self._coord = mock.MagicMock()
        self._coord.persist_now.return_value = "/tmp/chk/latest"
        self._coord.stop.return_value = None
        s.coordinator = self._coord
        return s

    def _patch_stop_workers(self):
        return mock.patch.object(js_mod.task_terminator, "stop_workers")

    def test_normal_stop_persists_checkpoint_then_stops_coordinator(self):
        s = self._scheduler()
        with (
            self._patch_stop_workers(),
            mock.patch.object(js_mod.klein, "get_actor_status", return_value=StreamTaskStatus.ALIVE),
            mock.patch.object(js_mod.klein, "get", return_value=None),
        ):
            s.stop_job()
        self._coord.persist_now.assert_called_once()
        self._coord.stop.assert_called_once()

    def test_persistence_fails_but_continues_to_stop(self):
        s = self._scheduler()
        self._coord.persist_now.side_effect = RuntimeError("persistence failed")
        with (
            self._patch_stop_workers(),
            mock.patch.object(js_mod.klein, "get_actor_status", return_value=StreamTaskStatus.ALIVE),
            mock.patch.object(js_mod.klein, "get", return_value=None),
        ):
            s.stop_job()
        self._coord.stop.assert_called_once()

    def test_coordinator_already_dead_skips_persistence_and_stop(self):
        s = self._scheduler()
        with (
            self._patch_stop_workers(),
            mock.patch.object(
                js_mod.klein,
                "get_actor_status",
                return_value=StreamTaskStatus.NOT_EXIST,
            ),
        ):
            s.stop_job()
        self._coord.persist_now.assert_not_called()
        self._coord.stop.assert_not_called()

    def test_stop_job_timeout_does_not_hang(self):
        """Verify that persist_now and stop each carry a timeout parameter
        so a stuck coordinator cannot hang the asyncio.to_thread worker."""
        s = self._scheduler()
        with (
            self._patch_stop_workers(),
            mock.patch.object(js_mod.klein, "get_actor_status", return_value=StreamTaskStatus.ALIVE),
            mock.patch.object(js_mod.klein, "get", return_value=None) as m_get,
        ):
            s.stop_job()
        # Both calls should include a timeout kwarg.
        for call_args in m_get.call_args_list:
            self.assertIn("timeout", call_args[1])


# ======================================================================
# restart  —  Tier-2 global restart
# ======================================================================
class RestartTest(unittest.TestCase):
    """Global restart: stop_job → load checkpoint → schedule, with suppression."""

    def _scheduler(self):
        graph = _build_graph([])
        s = JobMaster(graph, _config_with_timeouts())
        self._coord = mock.MagicMock()
        self._coord.latest_checkpoint_path.return_value = "/tmp/chk/42"
        s.coordinator = self._coord
        return s

    def test_restart_success(self):
        s = self._scheduler()
        with (
            mock.patch.object(js_mod.task_terminator, "stop_workers"),
            mock.patch.object(s, "schedule", return_value=None),
            mock.patch.object(js_mod.klein, "get", return_value=None),
            mock.patch.object(
                js_mod.klein,
                "get_actor_status",
                return_value=StreamTaskStatus.NOT_EXIST,
            ),
        ):
            result = s.restart()
        self.assertEqual(result.status, RestartStatus.SUCCESS)

    def test_successful_restart_clears_a_forced_global_recovery_policy(self):
        s = self._scheduler()
        previous_recovery = s._recovery
        previous_recovery.require_global_recovery("uncertain local topology")
        with (
            mock.patch.object(js_mod.task_terminator, "stop_workers"),
            mock.patch.object(s, "schedule", return_value=None),
            mock.patch.object(js_mod.klein, "get", return_value=None),
            mock.patch.object(
                js_mod.klein,
                "get_actor_status",
                return_value=StreamTaskStatus.NOT_EXIST,
            ),
        ):
            result = s.restart()

        self.assertEqual(result.status, RestartStatus.SUCCESS)
        self.assertIsNot(s._recovery, previous_recovery)
        self.assertIsNone(s._recovery._force_global_recovery_reason)

    def test_restart_suppressed_after_limit(self):
        s = self._scheduler()
        with (
            mock.patch.object(js_mod.task_terminator, "stop_workers"),
            mock.patch.object(s, "schedule", return_value=None),
            mock.patch.object(js_mod.klein, "get", return_value=None),
            mock.patch.object(
                js_mod.klein,
                "get_actor_status",
                return_value=StreamTaskStatus.NOT_EXIST,
            ),
        ):
            for _ in range(5):
                s.restart()
            result = s.restart()
        self.assertEqual(result.status, RestartStatus.SUPPRESSED)

    def test_restart_failed_when_schedule_fails(self):
        s = self._scheduler()
        with (
            mock.patch.object(js_mod.task_terminator, "stop_workers"),
            # schedule() now raises DeploymentError on failure (no tuple return).
            mock.patch.object(s, "schedule", side_effect=DeploymentError("create workers", "no workers")),
            mock.patch.object(js_mod.klein, "get", return_value=None),
            mock.patch.object(
                js_mod.klein,
                "get_actor_status",
                return_value=StreamTaskStatus.NOT_EXIST,
            ),
        ):
            result = s.restart()
        self.assertEqual(result.status, RestartStatus.FAILED)

    def test_restart_stop_workers_raises_returns_failed(self):
        s = self._scheduler()
        with (
            mock.patch.object(
                js_mod.task_terminator,
                "stop_workers",
                side_effect=TimeoutError("stop"),
            ),
            mock.patch.object(js_mod.klein, "get", return_value=None),
            mock.patch.object(
                js_mod.klein,
                "get_actor_status",
                return_value=StreamTaskStatus.NOT_EXIST,
            ),
        ):
            result = s.restart()
        self.assertEqual(result.status, RestartStatus.FAILED)


# ======================================================================
# timeout configuration
# ======================================================================
class TimeoutConfigurationTest(unittest.TestCase):
    """Startup vs lightweight-RPC timeout layering."""

    def test_startup_uses_long_timeout(self):
        s = JobMaster(_build_graph([]), _config_with_timeouts(start=300, rpc=30))
        self.assertEqual(s._schedule_start_timeout, 300)
        self.assertEqual(s._coordinator_rpc_timeout, 30)

    def test_rpc_timeout_defaults_to_30(self):
        s = JobMaster(_build_graph([]), _config_with_timeouts(start=300, rpc=30))
        self.assertEqual(s._coordinator_rpc_timeout, 30)

    def test_rpc_timeout_configurable(self):
        s = JobMaster(_build_graph([]), _config_with_timeouts(start=300, rpc=60))
        self.assertEqual(s._coordinator_rpc_timeout, 60)
