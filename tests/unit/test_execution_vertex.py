# SPDX-License-Identifier: Apache-2.0
from unittest.mock import MagicMock

import pytest

from ray.klein.config.configuration import Configuration
from ray.klein.runtime.execution_graph.execution_vertex import ExecutionVertex
from ray.klein.runtime.execution_graph.execution_vertex_status import ExecutionVertexStatus
from ray.klein.runtime.resources import Resources


def _vertex() -> ExecutionVertex:
    return ExecutionVertex(
        vertex_id=1,
        vertex_name="source",
        vertex_resources=Resources(),
        index=0,
        concurrency=1,
        operator=MagicMock(),
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
