# SPDX-License-Identifier: Apache-2.0
import json

from ray.klein.api.klein_context import KleinContext
from ray.klein.config.configuration import Configuration
from ray.klein.config.execution_options import ExecutionOptions
from ray.klein.config.runtime_execution_mode import RuntimeExecutionMode
from tests.integration.external.kafka_test_base import KafkaTestBase


class KafkaTest(KafkaTestBase):
    """Black-box checks for the supported Ray Data Kafka contract."""

    def test_read_kafka_snapshot(self) -> None:
        topic = "klein_test_source"
        self._produce_data(topic)
        context = KleinContext()
        context.enable_interactive_mode()

        rows = context.read_kafka(
            topic,
            bootstrap_servers=self.bootstrap_servers,
            start_offset="earliest",
            end_offset="latest",
        ).data.take_all()

        values = [json.loads(row["value"]) for row in rows]
        self.assertEqual([value["name"] for value in values], ["test1", "test2", "test3"])
        self.assertTrue(all({"offset", "partition", "topic", "value"} <= row.keys() for row in rows))

    def test_write_kafka_then_read_snapshot(self) -> None:
        topic = "test_write_kafka"
        context = KleinContext()
        context.from_items([{"name": "t1"}, {"name": "t2"}, {"name": "t3"}]).write_kafka(
            topic=topic,
            bootstrap_servers=self.bootstrap_servers,
            value_serializer="json",
        )
        context.execute("write-kafka").wait()

        read_context = KleinContext()
        read_context.enable_interactive_mode()
        rows = read_context.read_kafka(
            topic,
            bootstrap_servers=self.bootstrap_servers,
            start_offset="earliest",
            end_offset="latest",
        ).data.take_all()

        values = [json.loads(row["value"]) for row in rows]
        self.assertEqual(values, [{"name": "t1"}, {"name": "t2"}, {"name": "t3"}])

    def test_write_kafka_with_streaming_backend(self) -> None:
        topic = "test_streaming_write_kafka"
        configuration = Configuration()
        configuration.set(ExecutionOptions.MODE, RuntimeExecutionMode.STREAMING)
        context = KleinContext(configuration)
        context.from_values({"name": "t1"}, {"name": "t2"}, {"name": "t3"}).write_kafka(
            topic=topic,
            bootstrap_servers=self.bootstrap_servers,
            value_serializer="json",
            ray_remote_args={"num_cpus": 0.1},
            concurrency=2,
        )

        context.execute("streaming-write-kafka").wait()

        read_context = KleinContext()
        read_context.enable_interactive_mode()
        rows = read_context.read_kafka(
            topic,
            bootstrap_servers=self.bootstrap_servers,
            start_offset="earliest",
            end_offset="latest",
        ).data.take_all()
        values = sorted((json.loads(row["value"]) for row in rows), key=lambda row: row["name"])
        self.assertEqual(values, [{"name": "t1"}, {"name": "t2"}, {"name": "t3"}])
