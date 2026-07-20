# SPDX-License-Identifier: Apache-2.0
"""Actor-local runtime transaction contracts for operator rescaling."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

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


def _runtime(descriptor: SimpleNamespace, name: str):
    state = SimpleNamespace(name=name)
    return stream_task_module._TaskRuntime(
        descriptor=descriptor,
        context=SimpleNamespace(name=name),
        state=state,
        watermark=SimpleNamespace(name=f"{name}-watermark"),
        emit=SimpleNamespace(name=f"{name}-emit"),
        pump=SimpleNamespace(name=f"{name}-pump"),
        state_backend_task_name=name,
    )


def _paused_target(old_descriptor: SimpleNamespace):
    task = object.__new__(StreamTask)
    old_runtime = _runtime(old_descriptor, "old")
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
    task._close_runtime.assert_awaited_once_with(old_runtime, discard_backend=True)
    assert await task.commit_runtime_rescale("resize-1") is True
    task._close_runtime.assert_awaited_once()


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
async def test_commit_and_rollback_of_one_runtime_are_serialized() -> None:
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
    commit = asyncio.create_task(task.commit_runtime_rescale("resize-1"))
    await close_started.wait()
    rollback = asyncio.create_task(task.rollback_runtime_rescale("resize-1"))
    await asyncio.sleep(0)
    assert not rollback.done()

    allow_close.set()
    assert await commit is True
    assert await rollback is False
    assert task._active_runtime is pending_runtime
    assert pending_runtime.closed is False


@pytest.mark.asyncio
async def test_repeated_commit_retries_failed_previous_runtime_cleanup() -> None:
    old_descriptor = _descriptor(_operator(stateful=False), parallelism=2)
    new_descriptor = _descriptor(_operator(stateful=False), parallelism=3)
    task, old_runtime = _paused_target(old_descriptor)
    pending_runtime = _runtime(new_descriptor, "pending")
    task._build_runtime = AsyncMock(return_value=pending_runtime)
    task._start_runtime_components = Mock()
    task._close_runtime = AsyncMock(side_effect=[RuntimeError("close failed"), None])

    assert await task.prepare_runtime_rescale("resize-1", new_descriptor) is True
    assert await task.commit_runtime_rescale("resize-1") is True
    assert task._retired_runtimes == [old_runtime]

    assert await task.commit_runtime_rescale("resize-1") is True
    assert task._retired_runtimes == []
    assert task._close_runtime.await_count == 2
