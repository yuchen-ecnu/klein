# SPDX-License-Identifier: Apache-2.0
from unittest.mock import AsyncMock, Mock

import pytest

from ray.klein.config.configuration import Configuration
from ray.klein.runtime.execution_graph.execution_vertex_id import ExecutionVertexId
from ray.klein.runtime.execution_graph.execution_vertex_status import ExecutionVertexStatus
from ray.klein.runtime.job_manager.job_manager import JobManager


@pytest.mark.asyncio
async def test_failure_detail_preserves_the_error_that_triggered_restart() -> None:
    manager = JobManager(Configuration(), namespace="job-a")
    vertex_id = ExecutionVertexId(2, 0)
    vertex = Mock(error_message=None)
    vertex.name = "map (1/1)"
    graph = Mock(execution_vertices=[vertex])
    graph.find_execution_vertex.return_value = vertex
    manager.execution_graph = graph
    manager.job_master = Mock()
    manager.run_exclusive = AsyncMock(return_value=(True, False, vertex.name))

    await manager.update_stream_task_status(
        vertex_id,
        ExecutionVertexStatus.FAILED,
        "ValueError: user-code failure",
    )
    # A collateral teardown error can replace the vertex's transient status,
    # but the original diagnostic remains the root cause shown to the user.
    vertex.error_message = "ActorDiedError: downstream was killed during restart"

    detail = await manager.failure_detail()

    assert "ValueError: user-code failure" in detail
    assert "ActorDiedError" not in detail
    manager._writer.shutdown(wait=False)
