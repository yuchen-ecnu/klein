# SPDX-License-Identifier: Apache-2.0
import asyncio
import threading
from collections import deque
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, Mock, call, patch

import pytest

from ray.klein.runtime.execution_graph.execution_vertex_id import ExecutionVertexId
from ray.klein.runtime.execution_graph.execution_vertex_status import ExecutionVertexStatus
from ray.klein.runtime.message import MAX_WATERMARK, Barrier, RescaleBarrier, Watermark
from ray.klein.runtime.operator.source import SourceOperator
from ray.klein.runtime.worker.source_stream_task import SourceStreamTask
from ray.klein.runtime.worker.stream_task import StreamTask
from ray.klein.state.source_checkpoint_entry import SourceCheckpointEntry


def _source_operator() -> MagicMock:
    return MagicMock(spec=SourceOperator)


def _task(*, output=...) -> SourceStreamTask:
    task = object.__new__(SourceStreamTask)
    operator = _source_operator()
    checkpoint_strategy = MagicMock()
    checkpoint_strategy.should_trigger.return_value = False
    checkpoint_strategy.generate_next_barrier.return_value = None
    checkpoint_strategy.restore_source_state_async = AsyncMock(return_value=None)
    task._state = SimpleNamespace(
        operator=operator,
        output=MagicMock() if output is ... else output,
        executor=None,
        checkpoint_strategy=checkpoint_strategy,
        metrics=SimpleNamespace(barriers_out=MagicMock()),
    )
    operator.end_of_stream = False
    task._running = True
    task._eof_reached = False
    task._drain_requested = False
    task._inflight_source_states = {}
    task._source_rescale_requested = threading.Event()
    task._source_rescale_resume = threading.Event()
    task._source_rescale_loop = None
    task._source_rescale_barrier = None
    task._forced_checkpoint_requested = threading.Event()
    task._requested_checkpoint_ids = deque()
    task._checkpoint_request_lock = threading.Lock()
    task._resolved_checkpoint_floor = 0
    task._checkpoint_wait_stop = threading.Event()
    task._rescale_operation_id = None
    task._rescale_role = None
    task._rescale_edge_indices = ()
    task._rescale_seen_senders = set()
    task._rescale_snapshot = None
    task._rescale_ready_obj = None
    task._rescale_resume_obj = None
    task._rescale_tombstones = []
    task._runtime_rescale_preparing_operation_id = None
    task._runtime_rescale_transaction = None
    task._topology_operation_id = None
    task._topology_active = False
    task._vertex_id = ExecutionVertexId(3, 1)
    task._task_name = "source-1"
    task._task_generation = 4
    task._job_manager = MagicMock()
    return task


def test_constructor_initializes_source_coordination_state() -> None:
    descriptor = object()
    with patch.object(StreamTask, "__init__", autospec=True) as init:
        task = SourceStreamTask(descriptor)

    init.assert_called_once_with(task, descriptor)
    assert task._inflight_source_states == {}
    assert not task._source_rescale_requested.is_set()
    assert not task._source_rescale_resume.is_set()
    assert task._source_rescale_loop is None
    assert task._source_rescale_barrier is None
    assert not task._forced_checkpoint_requested.is_set()
    assert list(task._requested_checkpoint_ids) == []
    assert task._resolved_checkpoint_floor == 0
    assert not task._checkpoint_wait_stop.is_set()


def test_source_operator_property_rejects_non_source_operator() -> None:
    task = _task()
    assert task._source_operator is task._state.operator

    task._state.operator = object()
    with pytest.raises(TypeError, match="requires a SourceOperator"):
        _ = task._source_operator


@pytest.mark.asyncio
@pytest.mark.parametrize("restore", [None, SourceCheckpointEntry("source-1", 7, {"offset": 12})])
async def test_setup_binds_emitter_and_optionally_restores_source_state(restore) -> None:
    task = _task()
    task._checkpoint_wait_stop.set()
    task._state.checkpoint_strategy.restore_source_state_async.return_value = restore

    await task._on_setup_done(MagicMock())

    task._state.operator.bind_record_emitter.assert_called_once()
    assert task._state.operator.bind_record_emitter.call_args.args[0].__self__ is task
    assert task._source_rescale_loop is asyncio.get_running_loop()
    assert not task._checkpoint_wait_stop.is_set()
    if restore is None:
        task._state.operator.restore_state.assert_not_called()
    else:
        task._state.operator.restore_state.assert_called_once_with({"offset": 12})


@pytest.mark.asyncio
async def test_run_executes_blocking_source_then_stops() -> None:
    task = _task()
    task._run_source = Mock()
    task.stop = AsyncMock()

    await task._run()

    task._run_source.assert_called_once_with()
    task.stop.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_run_propagates_source_failure_without_calling_stop() -> None:
    task = _task()
    task._run_source = Mock(side_effect=RuntimeError("source failed"))
    task.stop = AsyncMock()

    with pytest.raises(RuntimeError, match="source failed"):
        await task._run()
    task.stop.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("has_state", [False, True])
async def test_stop_interrupts_initialized_source_and_releases_waiters(has_state: bool) -> None:
    task = _task()
    operator = task._state.operator
    if not has_state:
        task._state = None
    task._source_rescale_resume.clear()
    with patch.object(StreamTask, "stop", new=AsyncMock()) as parent_stop:
        await task.stop(2.5)

    if has_state:
        operator.interrupt.assert_called_once_with()
    else:
        operator.interrupt.assert_not_called()
    assert task._checkpoint_wait_stop.is_set()
    assert task._source_rescale_resume.is_set()
    parent_stop.assert_awaited_once_with(2.5)


@pytest.mark.asyncio
async def test_prepare_rescale_upstream_validates_before_mutating_task() -> None:
    task = _task()
    task._begin_rescale = Mock()

    with pytest.raises(ValueError, match="target output edge"):
        await task.prepare_rescale_upstream("resize-1", 8, (), 0.1)
    with pytest.raises(ValueError, match="operation_id"):
        await task.prepare_rescale_upstream("", 8, (0,), 0.1)

    task._begin_rescale.assert_not_called()
    assert task._source_rescale_barrier is None
    assert task._rescale_edge_indices == ()


@pytest.mark.asyncio
async def test_prepare_rescale_upstream_requests_fence_and_waits_until_ready() -> None:
    task = _task()
    task._begin_rescale = Mock()
    task._rescale_ready_obj = asyncio.Event()
    task._rescale_ready_obj.set()
    task._source_rescale_resume.set()

    assert await task.prepare_rescale_upstream("resize-1", 8, (2, 4), 0.1) is True

    task._begin_rescale.assert_called_once_with("resize-1", "upstream")
    assert task._rescale_edge_indices == (2, 4)
    assert task._source_rescale_barrier == RescaleBarrier("resize-1", 8)
    assert task._source_rescale_requested.is_set()
    assert not task._source_rescale_resume.is_set()


def test_resume_rescale_only_releases_matching_operation() -> None:
    task = _task()
    task._rescale_operation_id = "resize-1"
    task._source_rescale_requested.set()
    task._source_rescale_barrier = RescaleBarrier("resize-1", 8)

    with patch.object(StreamTask, "resume_rescale", return_value=True) as parent_resume:
        assert task.resume_rescale("other") is False
        assert task.resume_rescale("resize-1") is True

    parent_resume.assert_called_once_with("resize-1")
    assert not task._source_rescale_requested.is_set()
    assert task._source_rescale_barrier is None
    assert task._source_rescale_resume.is_set()


def test_run_source_propagates_connector_failure() -> None:
    task = _task()
    task._state.operator.run.side_effect = RuntimeError("poll failed")

    with pytest.raises(RuntimeError, match="poll failed"):
        task._run_source()
    task._state.output.collect.assert_not_called()


def test_run_source_returns_immediately_after_stop_interrupt() -> None:
    task = _task()
    task._state.operator.run.side_effect = task._checkpoint_wait_stop.set

    task._run_source()

    task._state.output.collect.assert_not_called()
    task._job_manager.update_stream_task_status.assert_not_called()


def test_run_source_without_output_reports_finished() -> None:
    task = _task(output=None)
    status_ref = object()
    task._job_manager.update_stream_task_status.return_value = status_ref

    with patch("ray.klein.runtime.worker.source_stream_task.klein.get", return_value=None) as get:
        task._run_source()

    get.assert_called_once_with(status_ref)
    task._job_manager.update_stream_task_status.assert_called_once_with(
        task._vertex_id,
        ExecutionVertexStatus.FINISHED,
        task_name="source-1",
        task_generation=4,
    )
    assert task._eof_reached is True


def test_run_source_emits_terminal_watermark_barrier_and_status() -> None:
    task = _task()
    terminal = Barrier(10, task._vertex_id)
    task._pop_requested_checkpoint = Mock(return_value=9)
    task._await_terminal_barrier = Mock(return_value=terminal)

    with patch("ray.klein.runtime.worker.source_stream_task.klein.get"):
        task._run_source()

    assert task._state.output.collect.call_args_list == [call(Watermark(MAX_WATERMARK)), call(terminal)]
    task._await_terminal_barrier.assert_called_once_with(9)
    task._job_manager.update_stream_task_status.assert_called_once()
    assert task._eof_reached is True


def test_run_source_stops_waiting_for_inflight_state_when_stopped() -> None:
    task = _task()
    task._inflight_source_states[3] = {"offset": 3}
    task._pop_requested_checkpoint = Mock(return_value=None)
    task._emit_pending_rescale_barrier = Mock(side_effect=task._checkpoint_wait_stop.set)
    task._await_terminal_barrier = Mock()

    with patch("ray.klein.runtime.worker.source_stream_task.time.sleep") as sleep:
        task._run_source()

    sleep.assert_called_once_with(0.01)
    task._await_terminal_barrier.assert_not_called()
    task._job_manager.update_stream_task_status.assert_not_called()


def test_run_source_waits_for_inflight_state_then_handles_canceled_terminal_epoch() -> None:
    task = _task()
    task._inflight_source_states[3] = {"offset": 3}
    task._pop_requested_checkpoint = Mock(return_value=None)
    task._emit_pending_rescale_barrier = Mock(side_effect=task._inflight_source_states.clear)
    task._await_terminal_barrier = Mock(return_value=None)

    with patch("ray.klein.runtime.worker.source_stream_task.time.sleep") as sleep:
        task._run_source()

    sleep.assert_called_once_with(0.01)
    task._await_terminal_barrier.assert_called_once_with(None)
    task._job_manager.update_stream_task_status.assert_not_called()


def test_request_drain_interrupts_source() -> None:
    task = _task()
    task.request_drain()
    task._state.operator.interrupt.assert_called_once_with()


@pytest.mark.parametrize("running,eof", [(False, False), (True, True)])
def test_checkpoint_requests_are_rejected_outside_live_source(running: bool, eof: bool) -> None:
    task = _task()
    task._running = running
    task._eof_reached = eof
    assert task.request_checkpoint() is False
    assert task.request_checkpoint(4) is False


@pytest.mark.parametrize("checkpoint_id", [True, 1.5, "1"])
def test_checkpoint_id_must_be_an_integer(checkpoint_id) -> None:
    with pytest.raises(TypeError, match="integer"):
        _task().request_checkpoint(checkpoint_id)


@pytest.mark.parametrize("checkpoint_id", [0, -1])
def test_checkpoint_id_must_be_positive(checkpoint_id: int) -> None:
    with pytest.raises(ValueError, match="greater than zero"):
        _task().request_checkpoint(checkpoint_id)


def test_checkpoint_request_deduplicates_resolved_queued_and_inflight_ids() -> None:
    task = _task()
    task._resolved_checkpoint_floor = 4
    task._requested_checkpoint_ids.append(6)
    task._inflight_source_states[7] = object()

    assert task.request_checkpoint(4) is False
    assert task.request_checkpoint(6) is False
    assert task.request_checkpoint(7) is False
    assert task.request_checkpoint(8) is True
    assert list(task._requested_checkpoint_ids) == [6, 8]
    assert task.request_checkpoint() is True
    assert task._forced_checkpoint_requested.is_set()


def test_pop_checkpoint_skips_resolved_and_inflight_requests() -> None:
    task = _task()
    task._resolved_checkpoint_floor = 3
    task._inflight_source_states[5] = object()
    task._requested_checkpoint_ids.extend((2, 5, 6))
    assert task._pop_requested_checkpoint() == 6
    assert task._pop_requested_checkpoint() is None

    del task._checkpoint_request_lock
    assert task._pop_requested_checkpoint() is None


def test_checkpoint_queue_helpers_are_backward_compatible_with_missing_fields() -> None:
    task = _task()
    task._requested_checkpoint_ids.extend((2, 3))
    task._discard_requested_checkpoint(2)
    task._discard_requested_checkpoint(9)
    assert list(task._requested_checkpoint_ids) == [3]
    task._remember_resolved_checkpoint(4)
    task._remember_resolved_checkpoint(2)
    assert task._resolved_checkpoint_floor == 4

    del task._checkpoint_request_lock
    task._discard_requested_checkpoint(3)
    task._remember_resolved_checkpoint(9)
    assert list(task._requested_checkpoint_ids) == [3]
    assert task._resolved_checkpoint_floor == 4


def test_complete_and_discard_source_checkpoints_are_idempotent() -> None:
    task = _task()
    state = {"offset": 42}
    task._inflight_source_states[5] = state
    task._requested_checkpoint_ids.extend((5, 6))

    assert task.notify_source_checkpoint_complete(5) == (True, state)
    assert task.notify_source_checkpoint_complete(5) == (False, None)
    assert task._resolved_checkpoint_floor == 5
    assert task.discard_source_checkpoint(6) is False
    assert list(task._requested_checkpoint_ids) == [5]
    assert task._pop_requested_checkpoint() is None
    assert list(task._requested_checkpoint_ids) == []
    assert task._resolved_checkpoint_floor == 6


@pytest.mark.asyncio
async def test_abort_checkpoint_releases_source_owned_state() -> None:
    task = _task()
    task._inflight_source_states[5] = object()
    assert await task.abort_checkpoint(5) is True
    assert await task.abort_checkpoint(5) is False


def test_reset_inflight_before_reclaims_only_old_epoch_state_and_requests() -> None:
    task = _task()
    task._inflight_source_states.update({2: "old", 5: "new"})
    task._requested_checkpoint_ids.extend((1, 3, 6))

    assert task.reset_inflight_before(3) == 1
    assert task._inflight_source_states == {5: "new"}
    assert list(task._requested_checkpoint_ids) == [6]
    assert task._resolved_checkpoint_floor == 3
    assert task.reset_inflight_before(3) == 0


def test_reset_inflight_before_handles_legacy_task_without_request_queue() -> None:
    task = _task()
    task._inflight_source_states[2] = "old"
    del task._requested_checkpoint_ids
    del task._checkpoint_request_lock

    assert task.reset_inflight_before(2) == 1


def test_checkpoint_completion_failure_is_contained() -> None:
    class BrokenStates(dict):
        def __contains__(self, key):
            raise RuntimeError("broken storage")

    task = _task()
    task._inflight_source_states = BrokenStates()
    assert task.notify_source_checkpoint_complete(5) == (False, None)


def test_record_boundary_honors_end_of_stream_before_checkpoint_work() -> None:
    task = _task()
    task._emit_pending_rescale_barrier = Mock()
    with patch.object(StreamTask, "_check_end_of_stream", return_value=True):
        assert task._on_records_emitted(True) is None
    task._emit_pending_rescale_barrier.assert_not_called()


def test_record_boundary_emits_coordinator_assigned_checkpoint() -> None:
    task = _task()
    barrier = Barrier(7, task._vertex_id)
    task._requested_checkpoint_ids.append(7)
    task._forced_checkpoint_requested.set()
    task._generate_barrier = Mock(return_value=barrier)

    assert task._on_records_emitted(True, 20) is barrier

    task._generate_barrier.assert_called_once_with(checkpoint_id=7)
    task._state.checkpoint_strategy.reset_trigger.assert_called_once_with()
    task._state.metrics.barriers_out.inc.assert_called_once_with()
    assert not task._forced_checkpoint_requested.is_set()


def test_canceled_assigned_checkpoint_does_not_update_metrics() -> None:
    task = _task()
    task._requested_checkpoint_ids.append(7)
    task._generate_barrier = Mock(return_value=None)
    assert task._on_records_emitted(False) is None
    task._state.checkpoint_strategy.reset_trigger.assert_not_called()
    task._state.metrics.barriers_out.inc.assert_not_called()


@pytest.mark.parametrize("forced", [False, True])
def test_record_boundary_emits_triggered_or_forced_checkpoint(forced: bool) -> None:
    task = _task()
    barrier = Barrier(8, task._vertex_id)
    task._forced_checkpoint_requested.set() if forced else None
    task._state.checkpoint_strategy.should_trigger.return_value = not forced
    task._generate_barrier = Mock(return_value=barrier)

    assert task._on_records_emitted(True, 11) is barrier

    if forced:
        task._state.checkpoint_strategy.should_trigger.assert_not_called()
    else:
        task._state.checkpoint_strategy.should_trigger.assert_called_once_with(True, 11)
    task._generate_barrier.assert_called_once_with(force=forced)
    task._state.metrics.barriers_out.inc.assert_called_once_with()
    assert not task._forced_checkpoint_requested.is_set()


def test_idle_boundary_without_due_checkpoint_returns_none() -> None:
    task = _task()
    assert task._on_records_emitted(False) is None
    task._state.checkpoint_strategy.should_trigger.assert_called_once_with(False, 1)


def test_canceled_forced_checkpoint_remains_requested_for_retry() -> None:
    task = _task()
    task._forced_checkpoint_requested.set()
    task._generate_barrier = Mock(return_value=None)
    assert task._on_records_emitted(False) is None
    assert task._forced_checkpoint_requested.is_set()
    task._state.metrics.barriers_out.inc.assert_not_called()


def test_emit_pending_rescale_barrier_is_noop_without_request() -> None:
    assert _task()._emit_pending_rescale_barrier() is False


@pytest.mark.parametrize("missing", ["barrier", "output"])
def test_emit_pending_rescale_barrier_requires_barrier_and_output(missing: str) -> None:
    task = _task()
    task._source_rescale_requested.set()
    task._source_rescale_barrier = RescaleBarrier("resize-1", 8)
    if missing == "barrier":
        task._source_rescale_barrier = None
    else:
        task._state.output = None
    with pytest.raises(RuntimeError, match="without an output"):
        task._emit_pending_rescale_barrier()


def test_emit_pending_rescale_barrier_requires_setup_event_loop() -> None:
    task = _task()
    task._source_rescale_requested.set()
    task._source_rescale_barrier = RescaleBarrier("resize-1", 8)
    with pytest.raises(RuntimeError, match="event loop is unavailable"):
        task._emit_pending_rescale_barrier()
    assert task._state.output.flush.call_args_list == [call(force=True), call(force=True)]


def test_emit_pending_rescale_barrier_flushes_selected_edges_and_parks() -> None:
    task = _task()
    barrier = RescaleBarrier("resize-1", 8)
    task._source_rescale_requested.set()
    task._source_rescale_barrier = barrier
    task._rescale_edge_indices = (1, 3)
    task._source_rescale_loop = MagicMock()
    task._source_rescale_resume.set()

    assert task._emit_pending_rescale_barrier() is True

    assert task._state.output.flush.call_args_list == [call(force=True), call(force=True)]
    task._state.output.collect_to_edges.assert_called_once_with(barrier, (1, 3))
    task._source_rescale_loop.call_soon_threadsafe.assert_called_once()
    assert not task._source_rescale_requested.is_set()


def test_terminal_barrier_wait_returns_none_after_stop() -> None:
    task = _task()
    task._checkpoint_wait_stop.set()
    task._generate_barrier = Mock()
    assert task._await_terminal_barrier(None) is None
    task._generate_barrier.assert_not_called()


def test_terminal_barrier_retries_with_next_requested_epoch() -> None:
    task = _task()
    barrier = Barrier(8, task._vertex_id)
    task._generate_barrier = Mock(side_effect=(None, barrier))
    task._pop_requested_checkpoint = Mock(return_value=8)
    with patch("ray.klein.runtime.worker.source_stream_task.time.sleep"):
        assert task._await_terminal_barrier(7) is barrier
    assert task._generate_barrier.call_args_list == [
        call(is_eof=True, force=True, checkpoint_id=7),
        call(is_eof=True, force=True, checkpoint_id=8),
    ]


@pytest.mark.parametrize("assigned", [False, True])
def test_generate_barrier_returns_none_when_checkpoint_admission_is_closed(assigned: bool) -> None:
    task = _task()
    task._requested_checkpoint_ids.append(7)
    checkpoint_id = 7 if assigned else None

    assert task._generate_barrier(force=True, checkpoint_id=checkpoint_id) is None

    expected = call(False, force=True, checkpoint_id=7) if assigned else call(False, force=True)
    assert task._state.checkpoint_strategy.generate_next_barrier.call_args == expected
    assert list(task._requested_checkpoint_ids) == ([] if assigned else [7])
    task._state.operator.flush.assert_not_called()


@pytest.mark.parametrize("is_eof", [False, True])
def test_generate_barrier_snapshots_state_and_runs_aligned_lifecycle(is_eof: bool) -> None:
    task = _task()
    barrier = Barrier(9, task._vertex_id)
    state = {"offset": 123}
    task._state.checkpoint_strategy.generate_next_barrier.return_value = barrier
    task._state.operator.snapshot_state.return_value = state
    task.prepare_sink_commit = Mock()
    task.register_checkpoint_metrics = Mock()

    def align(_barrier, callback):
        callback()
        return True

    task._state.checkpoint_strategy.on_barrier_received.side_effect = align
    assert task._generate_barrier(is_eof=is_eof, force=True, checkpoint_id=9) is barrier

    task._state.checkpoint_strategy.generate_next_barrier.assert_called_once_with(
        is_eof,
        force=True,
        checkpoint_id=9,
    )
    task._state.operator.flush.assert_called_once_with()
    task.prepare_sink_commit.assert_called_once_with(9)
    if is_eof:
        task._state.operator.finish.assert_called_once_with()
    else:
        task._state.operator.finish.assert_not_called()
    task._state.operator.snapshot_state.assert_called_once_with(9)
    assert task._inflight_source_states == {9: state}
    task.register_checkpoint_metrics.assert_called_once()
    assert task.register_checkpoint_metrics.call_args.args[0] is barrier
    assert task.register_checkpoint_metrics.call_args.args[1] > 0


def test_generate_barrier_tolerates_unmeasurable_source_state() -> None:
    task = _task()
    barrier = Barrier(9, task._vertex_id)
    task._state.checkpoint_strategy.generate_next_barrier.return_value = barrier
    task._state.operator.snapshot_state.return_value = object()
    task.prepare_sink_commit = Mock()
    task.register_checkpoint_metrics = Mock()

    def align(_barrier, callback):
        callback()

    task._state.checkpoint_strategy.on_barrier_received.side_effect = align
    with patch("ray.klein.runtime.worker.source_stream_task.cloudpickle.dumps", side_effect=TypeError):
        assert task._generate_barrier() is barrier
    task.register_checkpoint_metrics.assert_called_once_with(barrier, 0)


def test_persisted_checkpoint_notifies_connector() -> None:
    task = _task()
    task.notify_source_checkpoint_persisted(12)
    task._state.operator.notify_checkpoint_complete.assert_called_once_with(12)
