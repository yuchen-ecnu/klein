# SPDX-License-Identifier: Apache-2.0
"""Actor-local runtime transaction contracts for operator rescaling."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from ray.klein.observability.metrics.metric_group import TaskMetricGroup
from ray.klein.observability.metrics.task_metrics import TaskMetrics
from ray.klein.runtime.execution_graph.execution_vertex_id import ExecutionVertexId
from ray.klein.runtime.worker import stream_task as stream_task_module
from ray.klein.runtime.worker.stream_task import StreamTask


def _operator(*, stateful: bool) -> SimpleNamespace:
    return SimpleNamespace(
        id=2,
        name="transform",
        operator_type="ONE_INPUT",
        operator_class=object,
        owns_state=stateful,
        children=(),
        source=False,
        stateful=stateful,
    )


def _descriptor(
    operator: SimpleNamespace,
    *,
    parallelism: int,
    restore_operation_id: str | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        vertex_id=ExecutionVertexId(2, 0),
        task_name="transform-0",
        task_generation="generation-1",
        task_index=0,
        namespace="job",
        operator=operator,
        parallelism=parallelism,
        restore_operation_id=restore_operation_id,
    )


def _runtime(
    descriptor: SimpleNamespace,
    name: str,
    *,
    seed: int = 0,
    metrics=None,
):
    state = SimpleNamespace(
        name=name,
        operator=SimpleNamespace(
            records_in=seed + 1,
            records_out=seed + 2,
            bytes_in=seed + 3,
            bytes_out=seed + 4,
            processing_duration_ns=seed + 5,
        ),
        output=SimpleNamespace(
            backpressure_duration_ns=seed + 6,
            backpressure_events=seed + 7,
        ),
        metrics=metrics
        or SimpleNamespace(
            barriers_in=SimpleNamespace(value=seed + 8),
            barriers_out=SimpleNamespace(value=seed + 9),
            checkpoint_barrier_latency_ms=SimpleNamespace(last=seed + 12),
        ),
        inbox=SimpleNamespace(qsize=lambda: seed + 10),
        checkpoint_strategy=SimpleNamespace(last_alignment_duration_ms=seed + 11),
    )
    return stream_task_module._TaskRuntime(
        descriptor=descriptor,
        context=SimpleNamespace(name=name),
        state=state,
        watermark=SimpleNamespace(name=f"{name}-watermark"),
        emit=SimpleNamespace(name=f"{name}-emit"),
        pump=SimpleNamespace(name=f"{name}-pump"),
        state_backend_task_name=name,
    )


def _paused_target(old_descriptor: SimpleNamespace, old_runtime=None):
    task = object.__new__(StreamTask)
    old_runtime = old_runtime or _runtime(old_descriptor, "old")
    task._descriptor = old_descriptor
    task._task_name = old_descriptor.task_name
    task._task_generation = old_descriptor.task_generation
    task._vertex_id = old_descriptor.vertex_id
    task._running = True
    task._active_runtime = old_runtime
    task._state = old_runtime.state
    task._watermark = old_runtime.watermark
    task._emit = old_runtime.emit
    task._pump = old_runtime.pump
    task._runtime_rescale_transaction = None
    task._runtime_rescale_preparing_operation_id = None
    task._runtime_rescale_outcomes = {}
    task._runtime_rescale_lock_obj = None
    task._retired_runtimes = []
    task._retired_runtime_ids = set()
    task._retired_runtime_queue = asyncio.Queue(maxsize=stream_task_module._RETIRED_RUNTIME_LIMIT)
    task._retired_runtime_cleanup_task = None
    task._retired_runtime_cleanup_errors = {}
    task._retired_runtime_cleanup_attempts = {}
    task._initialize_runtime_metrics = Mock()
    task._rescale_operation_id = "resize-1"
    task._rescale_role = "target"
    task._rescale_ready_obj = asyncio.Event()
    task._rescale_ready_obj.set()
    task._rescale_resume_obj = asyncio.Event()
    task._rescale_expected_senders = set()
    task._rescale_seen_senders = set()
    task._rescale_edge_indices = ()
    task._rescale_snapshot = None
    task._rescale_tombstones = []
    task._topology_operation_id = None
    task._topology_active = False
    return task, old_runtime


@pytest.mark.asyncio
async def test_scale_in_retirement_does_not_release_the_fenced_target() -> None:
    task, _runtime = _paused_target(_descriptor(_operator(stateful=False), parallelism=3))
    task._stop_stream_task = AsyncMock()

    assert await task.retire_rescale("resize-1", timeout=7) is True

    task._stop_stream_task.assert_awaited_once_with(7, release_rescale=False)


@pytest.mark.asyncio
async def test_prepare_keeps_pending_runtime_invisible_until_commit() -> None:
    operator = _operator(stateful=True)
    old_descriptor = _descriptor(operator, parallelism=2)
    new_descriptor = _descriptor(
        operator,
        parallelism=4,
        restore_operation_id="resize-1",
    )
    task, old_runtime = _paused_target(old_descriptor)
    pending_runtime = _runtime(new_descriptor, "pending")
    task._build_runtime = AsyncMock(return_value=pending_runtime)
    task._start_runtime_components = Mock()
    task._close_runtime = AsyncMock()

    assert await task.prepare_runtime_rescale("resize-1", new_descriptor) is True

    task._build_runtime.assert_awaited_once_with(
        new_descriptor,
        state_backend_task_name="transform-0.__rescale__.resize-1",
        publish_metrics=False,
    )
    assert task._active_runtime is old_runtime
    assert task._descriptor is old_descriptor
    assert task._state is old_runtime.state
    assert task._watermark is old_runtime.watermark
    assert task._emit is old_runtime.emit
    assert task._pump is old_runtime.pump

    assert await task.commit_runtime_rescale("resize-1") is True

    assert task._active_runtime is pending_runtime
    assert task._descriptor is new_descriptor
    assert task._state is pending_runtime.state
    assert task._watermark is pending_runtime.watermark
    assert task._emit is pending_runtime.emit
    assert task._pump is pending_runtime.pump
    task._start_runtime_components.assert_called_once_with(pending_runtime)
    task._initialize_runtime_metrics.assert_called_once_with(pending_runtime)
    await task._retired_runtime_cleanup_task
    task._close_runtime.assert_awaited_once_with(
        old_runtime,
        stream_task_module._RETIRED_RUNTIME_CLOSE_TIMEOUT_SECONDS,
        discard_backend=True,
    )
    assert await task.commit_runtime_rescale("resize-1") is True
    task._close_runtime.assert_awaited_once()


@pytest.mark.asyncio
async def test_runtime_commit_keeps_retained_actor_progress_monotonic() -> None:
    operator = _operator(stateful=False)
    old_descriptor = _descriptor(operator, parallelism=2)
    old_descriptor.input_buffer_size = 20
    new_descriptor = _descriptor(operator, parallelism=3)
    new_descriptor.input_buffer_size = 30
    task, _old_runtime = _paused_target(old_descriptor)
    pending_runtime = _runtime(new_descriptor, "pending", seed=100)
    task._build_runtime = AsyncMock(return_value=pending_runtime)
    task._start_runtime_components = Mock()
    task._close_runtime = AsyncMock()
    task._last_checkpoint_state_size_bytes = 0
    task._last_checkpoint_id = None

    before = task.progress_counts()
    assert await task.prepare_runtime_rescale("resize-1", new_descriptor) is True
    assert await task.commit_runtime_rescale("resize-1") is True
    after = task.progress_counts()

    assert after.rows_in == before.rows_in + pending_runtime.state.operator.records_in
    assert after.rows_out == before.rows_out + pending_runtime.state.operator.records_out
    assert after.bytes_in == before.bytes_in + pending_runtime.state.operator.bytes_in
    assert after.bytes_out == before.bytes_out + pending_runtime.state.operator.bytes_out
    assert after.busy_ns == before.busy_ns + pending_runtime.state.operator.processing_duration_ns
    assert after.backpressure_ns == before.backpressure_ns + pending_runtime.state.output.backpressure_duration_ns
    assert after.backpressure_events == before.backpressure_events + pending_runtime.state.output.backpressure_events
    assert after.barriers_in == max(before.barriers_in, int(pending_runtime.state.metrics.barriers_in.value))
    assert after.barriers_out == max(before.barriers_out, int(pending_runtime.state.metrics.barriers_out.value))
    assert after.queued == 110
    assert after.capacity == 30

    assert await task.commit_runtime_rescale("resize-1") is True
    assert task.progress_counts() == after


@pytest.mark.asyncio
async def test_runtime_commit_does_not_double_shared_task_metric_counters() -> None:
    operator = _operator(stateful=False)
    old_descriptor = _descriptor(operator, parallelism=2)
    old_descriptor.input_buffer_size = 20
    new_descriptor = _descriptor(operator, parallelism=3)
    new_descriptor.input_buffer_size = 30
    metric_group = TaskMetricGroup(None, "transform-0", "transform", 0)
    old_metrics = TaskMetrics.create(metric_group, 20, 0, 1)
    old_metrics.barriers_in.inc(3)
    old_metrics.barriers_out.inc(2)
    old_runtime = _runtime(old_descriptor, "old", metrics=old_metrics)
    task, _ = _paused_target(old_descriptor, old_runtime)

    pending_metrics = TaskMetrics.create(metric_group, 30, 0, 1, initialize=False)
    assert pending_metrics.barriers_in is old_metrics.barriers_in
    assert pending_metrics.barriers_out is old_metrics.barriers_out
    pending_runtime = _runtime(new_descriptor, "pending", metrics=pending_metrics)
    task._build_runtime = AsyncMock(return_value=pending_runtime)
    task._start_runtime_components = Mock()
    task._close_runtime = AsyncMock()
    task._last_checkpoint_state_size_bytes = 0
    task._last_checkpoint_id = None

    before = task.progress_counts()
    assert await task.prepare_runtime_rescale("resize-1", new_descriptor) is True
    assert await task.commit_runtime_rescale("resize-1") is True
    after = task.progress_counts()

    assert after.barriers_in == before.barriers_in == 3
    assert after.barriers_out == before.barriers_out == 2
    pending_metrics.barriers_in.inc()
    pending_metrics.barriers_out.inc()
    current = task.progress_counts()
    assert current.barriers_in == 4
    assert current.barriers_out == 3


@pytest.mark.asyncio
async def test_runtime_rescale_rollback_discards_pending_and_keeps_exact_old_runtime() -> None:
    operator = _operator(stateful=False)
    old_descriptor = _descriptor(operator, parallelism=2)
    new_descriptor = _descriptor(operator, parallelism=3)
    task, old_runtime = _paused_target(old_descriptor)
    pending_runtime = _runtime(new_descriptor, "pending")
    task._build_runtime = AsyncMock(return_value=pending_runtime)
    task._close_runtime = AsyncMock()

    assert await task.prepare_runtime_rescale("resize-1", new_descriptor) is True
    with pytest.raises(RuntimeError, match="must be committed or rolled back"):
        task.resume_rescale("resize-1")

    assert await task.rollback_runtime_rescale("resize-1") is True

    task._close_runtime.assert_awaited_once_with(pending_runtime, discard_backend=True)
    assert task._active_runtime is old_runtime
    assert task._descriptor is old_descriptor
    assert task._state is old_runtime.state
    assert task._watermark is old_runtime.watermark
    assert task._emit is old_runtime.emit
    assert task._pump is old_runtime.pump
    assert await task.rollback_runtime_rescale("resize-1") is True
    task._close_runtime.assert_awaited_once()
    assert task.resume_rescale("resize-1") is True
    assert task.resume_rescale("resize-1") is True


@pytest.mark.asyncio
async def test_failed_runtime_prepare_is_already_rolled_back_idempotently() -> None:
    operator = _operator(stateful=False)
    old_descriptor = _descriptor(operator, parallelism=2)
    new_descriptor = _descriptor(operator, parallelism=3)
    task, old_runtime = _paused_target(old_descriptor)
    task._build_runtime = AsyncMock(side_effect=RuntimeError("restore failed"))

    with pytest.raises(RuntimeError, match="restore failed"):
        await task.prepare_runtime_rescale("resize-1", new_descriptor)

    assert task._active_runtime is old_runtime
    assert task._descriptor is old_descriptor
    assert task._runtime_rescale_transaction is None
    assert await task.rollback_runtime_rescale("resize-1") is True
    assert await task.rollback_runtime_rescale("resize-1") is True
    assert await task.commit_runtime_rescale("resize-1") is False


@pytest.mark.asyncio
async def test_stopped_actor_accepts_authoritative_parallelism_on_bootstrap() -> None:
    operator = _operator(stateful=False)
    old_descriptor = _descriptor(operator, parallelism=2)
    new_descriptor = _descriptor(operator, parallelism=5)
    task = object.__new__(StreamTask)
    task._descriptor = old_descriptor
    task._task_name = old_descriptor.task_name
    task._task_generation = old_descriptor.task_generation
    task._vertex_id = old_descriptor.vertex_id
    task._running = False
    task.setup_and_run = AsyncMock()

    await task.setup_and_run_with_descriptor(new_descriptor)

    assert task._descriptor is new_descriptor
    task.setup_and_run.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_prepare_accepts_an_equivalent_operator_deserialized_by_ray() -> None:
    old_descriptor = _descriptor(_operator(stateful=False), parallelism=2)
    new_descriptor = _descriptor(_operator(stateful=False), parallelism=3)
    assert old_descriptor.operator is not new_descriptor.operator
    task, _old_runtime = _paused_target(old_descriptor)
    task._build_runtime = AsyncMock(return_value=_runtime(new_descriptor, "pending"))

    assert await task.prepare_runtime_rescale("resize-1", new_descriptor) is True


@pytest.mark.asyncio
async def test_descriptor_validation_failure_is_rollback_idempotent() -> None:
    old_descriptor = _descriptor(_operator(stateful=False), parallelism=2)
    replacement = _operator(stateful=False)
    replacement.id = 99
    new_descriptor = _descriptor(replacement, parallelism=3)
    task, old_runtime = _paused_target(old_descriptor)

    with pytest.raises(ValueError, match="cannot replace"):
        await task.prepare_runtime_rescale("resize-1", new_descriptor)

    assert task._active_runtime is old_runtime
    assert await task.rollback_runtime_rescale("resize-1") is True


@pytest.mark.asyncio
async def test_slow_prepare_finishes_before_concurrent_rollback() -> None:
    old_descriptor = _descriptor(_operator(stateful=False), parallelism=2)
    new_descriptor = _descriptor(_operator(stateful=False), parallelism=3)
    task, old_runtime = _paused_target(old_descriptor)
    pending_runtime = _runtime(new_descriptor, "pending")
    build_started = asyncio.Event()
    allow_build = asyncio.Event()

    async def build_runtime(*_args, **_kwargs):
        build_started.set()
        await allow_build.wait()
        return pending_runtime

    task._build_runtime = AsyncMock(side_effect=build_runtime)
    task._close_runtime = AsyncMock()
    prepare = asyncio.create_task(task.prepare_runtime_rescale("resize-1", new_descriptor))
    await build_started.wait()
    rollback = asyncio.create_task(task.rollback_runtime_rescale("resize-1"))
    await asyncio.sleep(0)
    assert not rollback.done()

    allow_build.set()
    assert await prepare is True
    assert await rollback is True
    assert task._active_runtime is old_runtime
    task._close_runtime.assert_awaited_once_with(pending_runtime, discard_backend=True)


@pytest.mark.asyncio
async def test_commit_does_not_wait_for_retired_runtime_close() -> None:
    old_descriptor = _descriptor(_operator(stateful=False), parallelism=2)
    new_descriptor = _descriptor(_operator(stateful=False), parallelism=3)
    task, old_runtime = _paused_target(old_descriptor)
    pending_runtime = _runtime(new_descriptor, "pending")
    task._build_runtime = AsyncMock(return_value=pending_runtime)
    assert await task.prepare_runtime_rescale("resize-1", new_descriptor) is True

    close_started = asyncio.Event()
    allow_close = asyncio.Event()

    async def close_runtime(runtime, *_args, **_kwargs):
        if runtime is old_runtime:
            close_started.set()
            await allow_close.wait()

    task._close_runtime = AsyncMock(side_effect=close_runtime)
    task._start_runtime_components = Mock()
    assert await task.commit_runtime_rescale("resize-1") is True
    assert task._active_runtime is pending_runtime

    await close_started.wait()
    assert task._retired_runtime_cleanup_task is not None
    assert not task._retired_runtime_cleanup_task.done()
    assert await task.rollback_runtime_rescale("resize-1") is False

    allow_close.set()
    await task._retired_runtime_cleanup_task
    task._close_runtime.assert_awaited_once()


@pytest.mark.asyncio
async def test_background_worker_retries_failed_previous_runtime_cleanup(monkeypatch) -> None:
    old_descriptor = _descriptor(_operator(stateful=False), parallelism=2)
    new_descriptor = _descriptor(_operator(stateful=False), parallelism=3)
    task, old_runtime = _paused_target(old_descriptor)
    pending_runtime = _runtime(new_descriptor, "pending")
    task._build_runtime = AsyncMock(return_value=pending_runtime)
    task._start_runtime_components = Mock()
    task._close_runtime = AsyncMock(side_effect=[RuntimeError("close failed"), None])
    monkeypatch.setattr(stream_task_module, "_RETIRED_RUNTIME_RETRY_DELAY_SECONDS", 0.001)

    assert await task.prepare_runtime_rescale("resize-1", new_descriptor) is True
    assert await task.commit_runtime_rescale("resize-1") is True
    await task._retired_runtime_cleanup_task
    assert task._retired_runtimes == []
    assert task._close_runtime.await_count == 2


@pytest.mark.asyncio
async def test_close_runtime_shares_one_inflight_close_attempt() -> None:
    descriptor = _descriptor(_operator(stateful=False), parallelism=2)
    task, runtime = _paused_target(descriptor)
    close_started = asyncio.Event()
    allow_close = asyncio.Event()

    async def close_components(candidate, _timeout):
        assert candidate is runtime
        close_started.set()
        await allow_close.wait()
        candidate.closed = True

    task._close_runtime_components = AsyncMock(side_effect=close_components)
    first = asyncio.create_task(task._close_runtime(runtime, 0.01, discard_backend=False))
    second = asyncio.create_task(task._close_runtime(runtime, 0.01, discard_backend=False))
    await close_started.wait()
    with pytest.raises(TimeoutError):
        await first
    with pytest.raises(TimeoutError):
        await second
    assert task._close_runtime_components.await_count == 1

    allow_close.set()
    await runtime.close_task
    await task._close_runtime(runtime, 0.01, discard_backend=False)
    assert task._close_runtime_components.await_count == 1


@pytest.mark.asyncio
async def test_retired_runtime_backlog_applies_rescale_admission_backpressure() -> None:
    descriptor = _descriptor(_operator(stateful=False), parallelism=2)
    task, _active_runtime = _paused_target(descriptor)
    close_started = asyncio.Event()
    allow_close = asyncio.Event()

    async def block_close(*_args, **_kwargs):
        close_started.set()
        await allow_close.wait()

    task._close_runtime = AsyncMock(side_effect=block_close)
    runtimes = [_runtime(descriptor, f"old-{index}") for index in range(8)]
    for runtime in runtimes:
        task._retire_runtime(runtime)
    await close_started.wait()

    with pytest.raises(RuntimeError, match="rejecting rescale until cleanup catches up"):
        task._require_retired_runtime_capacity()

    allow_close.set()
    await task._retired_runtime_cleanup_task


@pytest.mark.asyncio
async def test_stop_wait_for_retired_cleanup_is_bounded_and_summarized() -> None:
    descriptor = _descriptor(_operator(stateful=False), parallelism=2)
    task, runtime = _paused_target(descriptor)
    allow_close = asyncio.Event()

    async def block_close(*_args, **_kwargs):
        await allow_close.wait()

    task._close_runtime = AsyncMock(side_effect=block_close)
    task._retire_runtime(runtime)
    error = await task._await_retired_runtime_cleanup(0.01)
    assert isinstance(error, TimeoutError)
    assert "1 retired runtime(s) remain" in str(error)

    allow_close.set()
    await task._retired_runtime_cleanup_task
