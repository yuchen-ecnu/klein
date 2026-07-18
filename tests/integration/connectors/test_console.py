# SPDX-License-Identifier: Apache-2.0
import unittest

from ray.klein.api.job_handle import JobHandle
from ray.klein.api.klein_context import KleinContext
from ray.klein.config.configuration import Configuration
from ray.klein.config.execution_options import ExecutionOptions
from ray.klein.config.runtime_execution_mode import RuntimeExecutionMode


class ConsoleTest(unittest.TestCase):
    """
    ConsoleTest.
    """

    def test_stream_show(self):
        config: Configuration = Configuration()
        config.set(ExecutionOptions.MODE, RuntimeExecutionMode.STREAMING)
        ctx = KleinContext(config)
        ctx.from_values(
            {"name": "Jack", "age": 23, "height": 164, "gender": "M"},
            {"name": "Lucy", "age": 18, "height": 172, "gender": "W"},
            {"name": "Tom", "age": 14, "height": 161, "gender": "M"},
            {"name": "Jerry", "age": 25, "height": 189, "gender": "W"},
        ).show(limit=3)
        client: JobHandle = ctx.execute("test")
        client.wait()
