# SPDX-License-Identifier: Apache-2.0
import ctypes
import os
import time
import unittest
from contextlib import contextmanager
from pathlib import Path
from threading import Event, Thread

from confluent_kafka import Consumer, Producer, TopicPartition


class _LibrdkafkaMockCluster:
    """Wire-protocol Kafka broker fallback bundled with confluent-kafka."""

    def __init__(self) -> None:
        self._library = None
        self._client = None
        self._cluster = None
        self._bootstrap_servers = ""

    def start(self, timeout: int = 30) -> "_LibrdkafkaMockCluster":
        del timeout
        import confluent_kafka

        library_dir = Path(confluent_kafka.__file__).resolve().parent.parent / "confluent_kafka.libs"
        candidates = tuple(library_dir.glob("librdkafka*.so*"))
        if not candidates:
            candidates = tuple(library_dir.glob("librdkafka*.dylib")) + tuple(library_dir.glob("librdkafka*.dll"))
        if not candidates:
            raise RuntimeError("confluent-kafka does not include a librdkafka shared library")

        library = ctypes.CDLL(str(candidates[0]))
        library.rd_kafka_conf_new.restype = ctypes.c_void_p
        library.rd_kafka_new.argtypes = [ctypes.c_int, ctypes.c_void_p, ctypes.c_char_p, ctypes.c_size_t]
        library.rd_kafka_new.restype = ctypes.c_void_p
        library.rd_kafka_mock_cluster_new.argtypes = [ctypes.c_void_p, ctypes.c_int]
        library.rd_kafka_mock_cluster_new.restype = ctypes.c_void_p
        library.rd_kafka_mock_cluster_bootstraps.argtypes = [ctypes.c_void_p]
        library.rd_kafka_mock_cluster_bootstraps.restype = ctypes.c_char_p
        library.rd_kafka_mock_cluster_destroy.argtypes = [ctypes.c_void_p]
        library.rd_kafka_destroy.argtypes = [ctypes.c_void_p]

        error = ctypes.create_string_buffer(512)
        client = library.rd_kafka_new(0, library.rd_kafka_conf_new(), error, len(error))
        if not client:
            raise RuntimeError(error.value.decode() or "could not create librdkafka client")
        cluster = library.rd_kafka_mock_cluster_new(client, 1)
        if not cluster:
            library.rd_kafka_destroy(client)
            raise RuntimeError("could not create librdkafka mock cluster")

        self._library = library
        self._client = client
        self._cluster = cluster
        self._bootstrap_servers = library.rd_kafka_mock_cluster_bootstraps(cluster).decode()
        return self

    def get_bootstrap_server(self) -> str:
        return self._bootstrap_servers

    def stop(self) -> None:
        if self._library is None:
            return
        if self._cluster is not None:
            self._library.rd_kafka_mock_cluster_destroy(self._cluster)
        if self._client is not None:
            self._library.rd_kafka_destroy(self._client)
        self._cluster = None
        self._client = None
        self._library = None


def _create_kafka_container():
    """Build a Kafka testcontainer using the structured wait-strategy API."""

    if os.environ.get("KLEIN_TEST_EMBEDDED_SERVICES") == "1":
        return _LibrdkafkaMockCluster()

    from testcontainers.core.container import DockerContainer
    from testcontainers.core.wait_strategies import LogMessageWaitStrategy
    from testcontainers.kafka import KafkaContainer

    class StructuredKafkaContainer(KafkaContainer):
        def start(self, timeout: int = 30) -> "StructuredKafkaContainer":
            script = self.TC_START_SCRIPT
            command = f'sh -c "while [ ! -f {script} ]; do sleep 0.1; done; sh {script}"'
            self.configure()
            self.with_command(command)
            DockerContainer.start(self)
            self.tc_start()
            LogMessageWaitStrategy(self.wait_for).with_startup_timeout(timeout).wait_until_ready(self)
            return self

    return StructuredKafkaContainer()


class KafkaTestBase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Start the Kafka container
        cls.kafka = _create_kafka_container()
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
