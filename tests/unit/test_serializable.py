# SPDX-License-Identifier: Apache-2.0
import unittest

from ray.util import inspect_serializability

from ray.klein.runtime.collector.collector import OutputCollector
from ray.klein.runtime.partitioning import ForwardPartitioner


class SerializableTest(unittest.TestCase):
    def test_output_collector_serializable(self):
        success, reason = inspect_serializability(
            OutputCollector(
                [],
                ForwardPartitioner(),
                3,
                [],
                3,
            ),
            name="test",
        )
        self.assertTrue(success, reason)
