# SPDX-License-Identifier: Apache-2.0
from unittest import TestCase

from ray.klein.api.job_status import JobStatus
from ray.klein.runtime.coordinator.checkpoint import CheckpointStatus
from ray.klein.runtime.execution_graph.execution_vertex import ExecutionVertexStatus


class TestEnumType(TestCase):
    def test_terminal_job_status(self):
        self.assertFalse(JobStatus.CREATED.is_terminal)
        self.assertFalse(JobStatus.SUBMITTING.is_terminal)
        self.assertFalse(JobStatus.DEPLOYING.is_terminal)
        self.assertFalse(JobStatus.INITIALIZING.is_terminal)
        self.assertFalse(JobStatus.RUNNING.is_terminal)
        self.assertTrue(JobStatus.FINISHED.is_terminal)
        self.assertTrue(JobStatus.CANCELLED.is_terminal)
        self.assertTrue(JobStatus.FAILED.is_terminal)

    def test_execution_vertex_terminate_status(self):
        self.assertFalse(ExecutionVertexStatus.CREATED.is_terminal)
        self.assertFalse(ExecutionVertexStatus.DEPLOYED.is_terminal)
        self.assertFalse(ExecutionVertexStatus.RUNNING.is_terminal)
        self.assertFalse(ExecutionVertexStatus.CANCELLING.is_terminal)
        self.assertTrue(ExecutionVertexStatus.FAILED.is_terminal)
        self.assertTrue(ExecutionVertexStatus.FINISHED.is_terminal)

    def test_checkpoint_terminal_status(self):
        self.assertFalse(CheckpointStatus.CREATED.is_terminal)
        self.assertFalse(CheckpointStatus.IN_PROGRESS.is_terminal)
        self.assertFalse(CheckpointStatus.NOTIFYING.is_terminal)
        self.assertTrue(CheckpointStatus.COMPLETED.is_terminal)
        self.assertTrue(CheckpointStatus.FAILED.is_terminal)
