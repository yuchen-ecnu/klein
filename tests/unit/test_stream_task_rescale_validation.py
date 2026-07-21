# SPDX-License-Identifier: Apache-2.0
"""Validation contracts for actor-local rescale preparation."""

from types import SimpleNamespace

import pytest

from ray.klein.runtime.execution_graph.execution_vertex_id import ExecutionVertexId
from ray.klein.runtime.worker.stream_task import StreamTask

SENDER = ExecutionVertexId(1, 0)


def _task(*, input_vertex_ids: tuple[ExecutionVertexId, ...] = (SENDER,)) -> StreamTask:
    task = object.__new__(StreamTask)
    task._task_name = "transform-0"
    task._descriptor = SimpleNamespace(input_vertex_ids=input_vertex_ids)
    task._rescale_operation_id = None
    task._rescale_role = None
    task._rescale_expected_senders = set()
    task._rescale_seen_senders = set()
    task._rescale_edge_indices = ()
    task._rescale_snapshot = None
    task._rescale_ready_obj = None
    task._rescale_resume_obj = None
    task._rescale_tombstones = []
    return task


def _rescale_state(task: StreamTask) -> tuple:
    return (
        task._rescale_operation_id,
        task._rescale_role,
        set(task._rescale_expected_senders),
        set(task._rescale_seen_senders),
        task._rescale_edge_indices,
        task._rescale_snapshot,
        task._rescale_ready_obj,
        task._rescale_resume_obj,
        tuple(task._rescale_tombstones),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("operation_id", "target_operator_id", "edge_indices", "error", "message"),
    [
        ("resize-1", 2, (), ValueError, "target output edge"),
        ("", 2, (0,), ValueError, "operation_id cannot be empty"),
        ("resize-1", True, (0,), TypeError, "target_operator_id must be an integer"),
    ],
)
async def test_invalid_upstream_prepare_does_not_start_rescale(
    operation_id,
    target_operator_id,
    edge_indices,
    error,
    message,
) -> None:
    task = _task()
    before = _rescale_state(task)

    with pytest.raises(error, match=message):
        await task.prepare_rescale_upstream(operation_id, target_operator_id, edge_indices, 0.1)

    assert _rescale_state(task) == before


@pytest.mark.parametrize(
    ("input_vertex_ids", "operation_id", "message"),
    [
        ((), "resize-1", "source operators cannot be locally rescaled"),
        ((SENDER,), " ", "operation_id cannot be empty"),
    ],
)
def test_invalid_target_prepare_does_not_start_rescale(
    input_vertex_ids,
    operation_id,
    message,
) -> None:
    task = _task(input_vertex_ids=input_vertex_ids)
    before = _rescale_state(task)

    with pytest.raises(ValueError, match=message):
        task.prepare_rescale_target(operation_id)

    assert _rescale_state(task) == before


@pytest.mark.parametrize(
    ("expected_senders", "error", "message"),
    [
        ((), ValueError, "at least one target input"),
        (([1, 0],), TypeError, "unhashable type"),
    ],
)
def test_invalid_downstream_prepare_does_not_start_rescale(
    expected_senders,
    error,
    message,
) -> None:
    task = _task()
    before = _rescale_state(task)

    with pytest.raises(error, match=message):
        task.prepare_rescale_downstream("resize-1", expected_senders)

    assert _rescale_state(task) == before


@pytest.mark.parametrize(
    ("operation_id", "role", "message"),
    [
        (None, "replacement", "operation_id cannot be empty"),
        ("resize-1", "observer", "unknown rescale role"),
    ],
)
def test_begin_rescale_validates_identity_before_mutating_state(operation_id, role, message) -> None:
    task = _task()
    before = _rescale_state(task)

    with pytest.raises(ValueError, match=message):
        task._begin_rescale(operation_id, role)

    assert _rescale_state(task) == before


def test_conflicting_prepare_preserves_active_rescale_state() -> None:
    task = _task()
    task.prepare_rescale_target("resize-1")
    before = _rescale_state(task)

    with pytest.raises(RuntimeError, match="already participates in rescale resize-1"):
        task.prepare_rescale_downstream("resize-2", (ExecutionVertexId(2, 0),))

    assert _rescale_state(task) == before


def test_valid_target_and_downstream_prepares_publish_complete_state() -> None:
    target = _task()
    target.prepare_rescale_target("resize-target")
    assert target._rescale_operation_id == "resize-target"
    assert target._rescale_role == "target"
    assert target._rescale_expected_senders == {SENDER}

    downstream_sender = ExecutionVertexId(2, 0)
    downstream = _task()
    downstream.prepare_rescale_downstream("resize-downstream", (downstream_sender,))
    assert downstream._rescale_operation_id == "resize-downstream"
    assert downstream._rescale_role == "downstream"
    assert downstream._rescale_expected_senders == {downstream_sender}
