# SPDX-License-Identifier: Apache-2.0
import time
import unittest
from contextlib import contextmanager
from threading import Event, Thread

from confluent_kafka import Consumer, Producer, TopicPartition


class KafkaTestBase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Start the Kafka container
        from testcontainers.kafka import KafkaContainer

        cls.kafka = KafkaContainer()
        cls.kafka.start(timeout=60)

        # Get Kafka bootstrap servers
        cls.bootstrap_servers = cls.kafka.get_bootstrap_server()

    @classmethod
    def tearDownClass(cls):
        cls.kafka.stop()

    def _produce_data(self, topic: str, timestamp: int | None = None):
        """
        Note that kafka topic will create automatically if it does not exist.
        """
        ts = timestamp or int(time.time() * 1000)
        producer = Producer({"bootstrap.servers": self.bootstrap_servers})
        producer.produce(topic, key="uid1", value='{"name": "test1"}', timestamp=ts)
        producer.produce(topic, key="uid2", value='{"name": "test2"}', timestamp=ts)
        producer.produce(topic, key="uid3", value='{"name": "test3"}', timestamp=ts)
        producer.flush()

    def _get_topic_offsets(self, topic: str) -> dict[int, int]:
        consumer = Consumer({"bootstrap.servers": self.bootstrap_servers, "group.id": "my-group"})
        try:
            partitions = consumer.list_topics(topic).topics[topic].partitions
            return {x: consumer.get_watermark_offsets(TopicPartition(topic, x))[1] for x in partitions}
        finally:
            consumer.close()

    @contextmanager
    def _background_producer(self, topic: str, *, interval: float = 0.1):
        stopped = Event()

        def produce_until_stopped() -> None:
            while not stopped.wait(interval):
                self._produce_data(topic)

        thread = Thread(target=produce_until_stopped, name=f"kafka-producer-{topic}")
        thread.start()
        try:
            yield
        finally:
            stopped.set()
            thread.join(timeout=5)
            if thread.is_alive():
                raise TimeoutError(f"Kafka producer for {topic!r} did not stop")
