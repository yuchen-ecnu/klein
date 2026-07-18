# SPDX-License-Identifier: Apache-2.0
import unittest

from ray.klein.api.job_client import JobClient
from ray.klein.api.job_handle import JobHandle
from ray.klein.api.klein_context import KleinContext
from ray.klein.config.configuration import Configuration
from ray.klein.config.execution_options import ExecutionOptions
from ray.klein.config.runtime_execution_mode import RuntimeExecutionMode


class DataStreamWithoutSinkTest(unittest.TestCase):
    def _test_execute_without_sink(self, mode: RuntimeExecutionMode):
        config: Configuration = Configuration()
        config.set(ExecutionOptions.MODE, mode)
        ctx = KleinContext(config)
        ctx.from_values({"name": "Jack", "age": 23, "height": 164, "gender": "M"})
        with self.assertRaises(ValueError) as context:
            client: JobHandle = ctx.execute("test")
            client.wait()

        self.assertEqual(str(context.exception), JobClient._no_sink_error)

    def test_stream_without_sink(self):
        self._test_execute_without_sink(RuntimeExecutionMode.STREAMING)

    def test_batch_without_sink(self):
        self._test_execute_without_sink(RuntimeExecutionMode.BATCH)

    def test_auto_without_sink(self):
        self._test_execute_without_sink(RuntimeExecutionMode.AUTO)
