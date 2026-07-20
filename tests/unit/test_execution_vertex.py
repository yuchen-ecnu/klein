# SPDX-License-Identifier: Apache-2.0
from unittest.mock import MagicMock

import pytest

from ray.klein.config.configuration import Configuration
from ray.klein.runtime.execution_graph.execution_vertex import ExecutionVertex
from ray.klein.runtime.execution_graph.execution_vertex_status import ExecutionVertexStatus
from ray.klein.runtime.operator.operator import StreamOperator
from ray.klein.runtime.operator.operator_spec import OperatorSpec
from ray.klein.runtime.operator.operator_type import OperatorType
from ray.klein.runtime.resources import Resources


def _vertex() -> ExecutionVertex:
    return ExecutionVertex(
        vertex_id=1,
        vertex_name="source",
        vertex_resources=Resources(),
        index=0,
        concurrency=1,
        operator_spec=MagicMock(),
        config=Configuration(include_environment=False),
        task_metric_group=MagicMock(),
    )


def test_bounded_source_can_finish_while_deploying() -> None:
    vertex = _vertex()

    vertex.transition_to(ExecutionVertexStatus.DEPLOYED)
    vertex.transition_to(ExecutionVertexStatus.FINISHED)

    assert vertex.status is ExecutionVertexStatus.FINISHED


def test_invalid_transition_fails_immediately() -> None:
    vertex = _vertex()

    with pytest.raises(RuntimeError, match="Invalid execution vertex transition"):
        vertex.transition_to(ExecutionVertexStatus.RUNNING)


def test_reset_clears_terminal_state_and_error() -> None:
    vertex = _vertex()
    vertex.transition_to(ExecutionVertexStatus.FAILED, "boom")

    vertex.reset()

    assert vertex.status is ExecutionVertexStatus.CREATED
    assert vertex.error_message is None


def test_actor_name_is_stable_while_display_name_tracks_parallelism() -> None:
    vertex = _vertex()

    assert vertex.name == "source (1)"
    assert vertex.display_name == "source (1/1)"


def test_rebind_explicitly_preserves_runtime_state_without_sharing_status() -> None:
    vertex = _vertex()
    operator_spec = OperatorSpec(StreamOperator, None, 1, "source", OperatorType.ONE_INPUT)
    vertex.stream_task = MagicMock()
    vertex.restore_operation_id = "resize-1"
    vertex.transition_to(ExecutionVertexStatus.DEPLOYED)
    vertex.transition_to(ExecutionVertexStatus.RUNNING)

    rebound = vertex.rebind(
        concurrency=2,
        vertex_resources=Resources(num_cpus=1, concurrency=2),
        operator_spec=operator_spec,
        config=vertex.config,
    )

    assert rebound is not vertex
    assert rebound.name == vertex.name == "source (1)"
    assert rebound.display_name == "source (1/2)"
    assert vertex.display_name == "source (1/1)"
    assert rebound.stream_task is vertex.stream_task
    assert rebound.task_generation == vertex.task_generation
    assert rebound.restore_operation_id == "resize-1"
    assert rebound.status is ExecutionVertexStatus.RUNNING
    assert rebound.task_metric_group is vertex.task_metric_group
    assert rebound.operator_spec is operator_spec

    rebound.transition_to(ExecutionVertexStatus.CANCELLED)
    assert vertex.status is ExecutionVertexStatus.RUNNING
