# SPDX-License-Identifier: Apache-2.0
"""Pure tests for two-phase worker teardown and survivor handling."""

from __future__ import annotations

import asyncio
import threading
import time
from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch

import pytest

from ray.klein.api.stream_task_status import StreamTaskStatus
from ray.klein.runtime.actor import create_remote_actor
from ray.klein.runtime.execution_graph.execution_vertex import ExecutionVertex
from ray.klein.runtime.execution_graph.execution_vertex_status import ExecutionVertexStatus
from ray.klein.runtime.scheduler import task_terminator as terminator
from ray.klein.runtime.scheduler.errors import TeardownError


class _SlowStopActor:
    async def stop(self) -> None:
        await asyncio.sleep(0.2)


def _vertex(index: int, status: ExecutionVertexStatus, *, handle=...) -> ExecutionVertex:
    vertex = object.__new__(ExecutionVertex)
    vertex.name = f"operator ({index + 1}/3)"
    vertex.index = index
    vertex._status = status
    vertex._error_message = None
    vertex.stream_task = MagicMock(name=f"task-{index}") if handle is ... else handle
    return vertex


def _job_vertex(*vertices: ExecutionVertex):
    return SimpleNamespace(
        name="operator",
        execution_vertices={vertex.index: vertex for vertex in vertices},
    )


class _Graph:
    def __init__(self, jobs, downstream, sources=(1,), namespace="job-ns") -> None:
        self._jobs = jobs
        for vertex_id, job in jobs.items():
            job.id = vertex_id
        self._downstream = downstream
        self.source_job_vertices = sources
        self.namespace = namespace
        self.execution_vertices = [vertex for job in jobs.values() for vertex in job.execution_vertices.values()]

    def job_vertex(self, vertex_id):
        return self._jobs[vertex_id]

    def downstream_job_vertices(self, vertex_id):
        return self._downstream.get(vertex_id, ())


def test_stop_workers_always_runs_survivor_sweep_after_phase_one_failure() -> None:
    graph = MagicMock()
    deadline = MagicMock()
    deadline.step.return_value = 3
    deadline.remaining.return_value = 0.25
    with (
        patch.object(terminator, "Deadline", return_value=deadline) as deadline_factory,
        patch.object(terminator, "_request_graceful_stop", side_effect=RuntimeError("stop RPC failed")),
        patch.object(terminator, "force_kill_survivors") as force_kill,
    ):
        terminator.stop_workers(graph, timeout=3, force=False)

    deadline_factory.assert_called_once_with(3)
    force_kill.assert_called_once_with(graph, timeout=0.25)


def test_stop_workers_propagates_a_genuine_survivor_failure() -> None:
    graph = MagicMock()
    error = TeardownError("actor survived")
    with (
        patch.object(terminator, "_request_graceful_stop"),
        patch.object(terminator, "force_kill_survivors", side_effect=error),
        pytest.raises(TeardownError, match="actor survived"),
    ):
        terminator.stop_workers(graph, timeout=3, force=True)


def test_stop_workers_batches_multiple_survivors_under_one_remaining_budget() -> None:
    first = _vertex(0, ExecutionVertexStatus.RUNNING)
    second = _vertex(1, ExecutionVertexStatus.DEPLOYED)
    graph = _Graph({1: _job_vertex(first, second)}, {})
    deadline = MagicMock()
    deadline.step.return_value = 5
    deadline.remaining.return_value = 0.125

    with (
        patch.object(terminator, "Deadline", return_value=deadline),
        patch.object(terminator, "_request_graceful_stop"),
        patch.object(terminator, "_kill_actor_names", return_value=set()) as batch_kill,
        patch.object(terminator, "_kill_actor_with_retry") as per_actor_kill,
    ):
        terminator.stop_workers(graph, timeout=5, force=False)

    batch_kill.assert_called_once()
    names, namespace, kill_budget = batch_kill.call_args.args
    assert set(names) == {first.name, second.name}
    assert namespace == "job-ns"
    assert kill_budget == 0.125
    per_actor_kill.assert_not_called()
    assert first.status is ExecutionVertexStatus.CANCELLED
    assert second.status is ExecutionVertexStatus.CANCELLED
    assert first.stream_task is second.stream_task is None


def test_graceful_stop_visits_diamond_join_only_once() -> None:
    jobs = {vertex_id: _job_vertex() for vertex_id in (1, 2, 3)}
    graph = _Graph(jobs, {1: (3,), 2: (3,)}, sources=(1, 2))

    with (
        patch.object(terminator, "_stop_worker", return_value=[]) as stop_worker,
        patch.object(terminator.klein, "get") as get,
    ):
        terminator._request_graceful_stop(graph, timeout=4, force=False)

    assert [entry.args for entry in stop_worker.call_args_list] == [
        (jobs[1], False),
        (jobs[2], False),
        (jobs[3], False),
    ]
    assert all(0 <= entry.kwargs["timeout"] <= 4 for entry in stop_worker.call_args_list)
    assert 0 <= get.call_args.kwargs["timeout"] <= 4


def test_stop_workers_reaches_survivor_sweep_after_debug_proxy_timeout() -> None:
    vertex = _vertex(0, ExecutionVertexStatus.RUNNING)
    handle = create_remote_actor(_SlowStopActor, local_mode=True)
    vertex.stream_task = handle
    graph = _Graph({1: _job_vertex(vertex)}, {})
    started = time.monotonic()
    try:
        with patch.object(terminator, "force_kill_survivors") as force_kill:
            terminator.stop_workers(graph, timeout=0.01, force=False)

        assert time.monotonic() - started < 0.15
        force_kill.assert_called_once()
    finally:
        terminator.klein.kill(handle)


def test_graceful_stop_timeout_is_best_effort() -> None:
    jobs = {1: _job_vertex()}
    graph = _Graph(jobs, {})
    with (
        patch.object(terminator, "_stop_worker", return_value=["stop-ref"]),
        patch.object(terminator.klein, "get", side_effect=TimeoutError("late")),
    ):
        terminator._request_graceful_stop(graph, timeout=0.1, force=False)


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        (StreamTaskStatus.NOT_EXIST, False),
        (StreamTaskStatus.ALIVE, True),
        (StreamTaskStatus.DEAD, True),
    ],
)
def test_actor_status_controls_whether_named_kill_is_needed(status, expected: bool) -> None:
    with patch.object(terminator.klein, "get_actor_status", return_value=status):
        assert terminator._actor_may_exist("task", "job-ns") is expected


def test_actor_status_failure_is_treated_as_a_possible_survivor() -> None:
    with patch.object(terminator.klein, "get_actor_status", side_effect=RuntimeError("GCS unavailable")):
        assert terminator._actor_may_exist("task", "job-ns") is True


def test_batch_kill_status_probes_share_one_short_deadline() -> None:
    names = {f"task-{index}" for index in range(8)}
    calls = 0
    probe_timeouts: list[float] = []
    timer = threading.Event()

    def status(_name: str, *, namespace: str, timeout: float) -> StreamTaskStatus:
        nonlocal calls
        assert namespace == "job-ns"
        calls += 1
        probe_timeouts.append(timeout)
        # The existence batch is immediate. Every status in the first kill
        # round then consumes its assigned share of the remaining total budget.
        if calls > len(names):
            timer.wait(timeout)
        return StreamTaskStatus.ALIVE

    started = time.monotonic()
    with (
        patch.object(terminator.klein, "get_actor_status", side_effect=status),
        patch.object(terminator.klein, "kill_actor_by_name"),
    ):
        survivors = terminator._kill_actor_names(names, "job-ns", timeout=0.02)
    elapsed = time.monotonic() - started

    assert survivors == names
    assert elapsed < 0.15
    assert calls >= len(names)
    assert all(0 < timeout <= 0.02 for timeout in probe_timeouts)


def test_batch_kill_fires_once_when_shared_budget_is_already_exhausted() -> None:
    names = {"task-0", "task-1"}
    with (
        patch.object(terminator.klein, "get_actor_status") as status,
        patch.object(terminator.klein, "kill_actor_by_name") as kill,
    ):
        survivors = terminator._kill_actor_names(names, "job-ns", timeout=0)

    assert survivors == names
    assert {entry.args for entry in kill.call_args_list} == {("task-0",), ("task-1",)}
    assert all(entry.kwargs == {"namespace": "job-ns", "timeout": 0.0} for entry in kill.call_args_list)
    status.assert_not_called()


def test_force_sweep_reconciles_live_status_but_preserves_terminal_status() -> None:
    running = _vertex(0, ExecutionVertexStatus.RUNNING)
    finished = _vertex(1, ExecutionVertexStatus.FINISHED)
    created = _vertex(2, ExecutionVertexStatus.CREATED)
    graph = _Graph({1: _job_vertex(running, finished, created)}, {})

    with (
        patch.object(terminator, "_actor_may_exist", side_effect=[False, True, False]),
        patch.object(terminator, "_kill_actor_with_retry", return_value=True) as kill,
    ):
        terminator._force_kill_survivors(graph)

    kill.assert_called_once_with(finished.name, "job-ns")
    assert running.status is ExecutionVertexStatus.CANCELLED
    assert finished.status is ExecutionVertexStatus.FINISHED
    assert created.status is ExecutionVertexStatus.CREATED
    assert running.stream_task is finished.stream_task is created.stream_task is None


def test_force_sweep_reports_every_actor_that_survives_retries() -> None:
    first = _vertex(0, ExecutionVertexStatus.RUNNING)
    second = _vertex(1, ExecutionVertexStatus.DEPLOYED)
    graph = _Graph({1: _job_vertex(first, second)}, {})

    with (
        patch.object(terminator, "_actor_may_exist", return_value=True),
        patch.object(terminator, "_kill_actor_with_retry", return_value=False),
        pytest.raises(TeardownError, match=r"operator \(1/3\).*operator \(2/3\)"),
    ):
        terminator._force_kill_survivors(graph)

    assert first.status is ExecutionVertexStatus.RUNNING
    assert second.status is ExecutionVertexStatus.DEPLOYED
    assert first.stream_task is not None
    assert second.stream_task is not None


def test_named_kill_succeeds_when_status_disappears_despite_kill_error() -> None:
    with (
        patch.object(terminator.klein, "kill_actor_by_name", side_effect=RuntimeError("already gone")),
        patch.object(terminator.klein, "get_actor_status", return_value=StreamTaskStatus.NOT_EXIST),
        patch.object(terminator.time, "sleep") as sleep,
    ):
        assert terminator._kill_actor_with_retry("task", "job-ns") is True

    sleep.assert_not_called()


def test_named_kill_retries_status_errors_then_observes_removal() -> None:
    with (
        patch.object(terminator.klein, "kill_actor_by_name") as kill,
        patch.object(
            terminator.klein,
            "get_actor_status",
            side_effect=[RuntimeError("GCS unavailable"), StreamTaskStatus.NOT_EXIST],
        ),
        patch.object(terminator.time, "sleep") as sleep,
    ):
        assert terminator._kill_actor_with_retry("task", "job-ns") is True

    assert kill.call_count == 2
    sleep.assert_called_once_with(terminator._KILL_ACTOR_RETRY_DELAY)


def test_named_kill_exhausts_retries_for_a_live_actor() -> None:
    with (
        patch.object(terminator.klein, "kill_actor_by_name") as kill,
        patch.object(terminator.klein, "get_actor_status", return_value=StreamTaskStatus.ALIVE) as status,
        patch.object(terminator.time, "sleep") as sleep,
    ):
        assert terminator._kill_actor_with_retry("task", "job-ns") is False

    assert kill.call_count == terminator._KILL_ACTOR_MAX_RETRIES
    assert status.call_count == terminator._KILL_ACTOR_MAX_RETRIES
    assert sleep.call_count == terminator._KILL_ACTOR_MAX_RETRIES - 1


def test_graceful_stop_skips_inactive_and_missing_handles() -> None:
    running = _vertex(0, ExecutionVertexStatus.RUNNING)
    missing = _vertex(1, ExecutionVertexStatus.RUNNING, handle=None)
    finished = _vertex(2, ExecutionVertexStatus.FINISHED)
    reference = object()
    running.stream_task.stop.return_value = reference

    references = terminator._stop_worker(_job_vertex(running, missing, finished), force=False)

    assert references == [reference]
    running.stream_task.stop.assert_called_once_with()
    assert running.status is ExecutionVertexStatus.CANCELLING
    assert missing.status is ExecutionVertexStatus.RUNNING
    assert finished.status is ExecutionVertexStatus.FINISHED


def test_force_stop_continues_after_handle_kill_failure() -> None:
    first = _vertex(0, ExecutionVertexStatus.RUNNING)
    second = _vertex(1, ExecutionVertexStatus.RUNNING)
    with patch.object(terminator.klein, "kill", side_effect=[RuntimeError("lost handle"), None]) as kill:
        assert terminator._stop_worker(_job_vertex(first, second), force=True, timeout=0) == []

    assert kill.call_args_list == [
        call(first.stream_task, timeout=0.0),
        call(second.stream_task, timeout=0.0),
    ]


def test_stop_job_vertex_sweeps_by_name_after_stop_rpc_failure() -> None:
    vertex = _vertex(0, ExecutionVertexStatus.RUNNING)
    job_vertex = _job_vertex(vertex)
    with (
        patch.object(terminator, "_stop_worker", side_effect=RuntimeError("RPC construction failed")),
        patch.object(terminator, "_kill_actor_names", return_value=set()) as kill,
    ):
        terminator.stop_job_vertex(job_vertex, "job-ns", timeout=1)

    kill.assert_called_once()
    assert vertex.status is ExecutionVertexStatus.CANCELLED
    assert vertex.stream_task is None


def test_stop_job_vertex_waits_then_kills_named_survivor() -> None:
    vertex = _vertex(0, ExecutionVertexStatus.RUNNING)
    reference = object()
    vertex.stream_task.stop.return_value = reference
    job_vertex = _job_vertex(vertex)
    deadline = MagicMock()
    deadline.remaining.side_effect = [1.75, 1.25]
    deadline.step.return_value = 0.25
    with (
        patch.object(terminator, "Deadline", return_value=deadline) as deadline_factory,
        patch.object(terminator.klein, "get") as get,
        patch.object(terminator, "_kill_actor_names", return_value=set()) as kill,
    ):
        terminator.stop_job_vertex(job_vertex, "job-ns", timeout=2)

    assert deadline_factory.call_args_list == [call(2), call(1.75)]
    get.assert_called_once_with([reference], timeout=1.25)
    names, namespace, timeout = kill.call_args.args
    assert tuple(names) == (vertex.name,)
    assert namespace == "job-ns"
    assert timeout == 0.25
    assert vertex.status is ExecutionVertexStatus.CANCELLED
    assert vertex.stream_task is None


def test_vertex_subset_validation_rejects_wrong_foreign_and_duplicate_values() -> None:
    member = _vertex(0, ExecutionVertexStatus.CREATED)
    foreign = _vertex(0, ExecutionVertexStatus.CREATED)
    job_vertex = _job_vertex(member)

    with pytest.raises(TypeError, match="ExecutionVertex"):
        terminator._select_vertices(job_vertex, (object(),))
    with pytest.raises(ValueError, match="does not belong"):
        terminator._select_vertices(job_vertex, (foreign,))
    with pytest.raises(ValueError, match="duplicate"):
        terminator._select_vertices(job_vertex, (member, member))
    assert terminator._select_vertices(job_vertex, (member,)) == (member,)
