# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from collections import deque
from types import SimpleNamespace
from typing import Any

import confluent_kafka

from ray.klein.integrations.kafka import KafkaSource


class _Metric:
    def inc(self, value: int | float = 1) -> None:
        pass

    def set(self, value: int | float) -> None:
        pass

    def observe(self, value: int | float) -> None:
        pass

    def observe_elapsed(self, started_at: float) -> None:
        pass


class _MetricGroup:
    def metric(self, spec) -> _Metric:
        return _Metric()

    builtin_counter = metric
    builtin_gauge = metric
    builtin_histogram = metric


class _Message:
    def __init__(self, *, topic: str = "events", partition: int = 0, offset: int = 10) -> None:
        self._topic = topic
        self._partition = partition
        self._offset = offset

    def error(self):
        return None

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset

    def timestamp(self) -> tuple[int, int]:
        return 0, 1_234

    def key(self) -> bytes:
        return b"key"

    def value(self) -> bytes:
        return b"value"

    def headers(self) -> list[tuple[str, bytes]]:
        return [("trace-id", b"abc")]


class _Consumer:
    def __init__(self, messages: list[_Message] | None = None, *, partitions: tuple[int, ...] = (0,)) -> None:
        self.messages = deque([messages or []])
        self.partitions = partitions
        self.assignments: list[list[Any]] = []
        self.commits: list[list[Any]] = []
        self.closed = False
        self.config: dict[str, Any] | None = None

    def list_topics(self, *, topic: str, timeout: float):
        metadata = SimpleNamespace(error=None, partitions={partition: object() for partition in self.partitions})
        return SimpleNamespace(topics={topic: metadata})

    def assign(self, partitions: list[Any]) -> None:
        self.assignments.append(partitions)

    def get_watermark_offsets(self, partition, timeout: float, cached: bool = False) -> tuple[int, int]:
        return 0, 100

    def committed(self, partitions: list[Any], timeout: float) -> list[Any]:
        return [
            confluent_kafka.TopicPartition(item.topic, item.partition, confluent_kafka.OFFSET_INVALID)
            for item in partitions
        ]

    def offsets_for_times(self, partitions: list[Any], timeout: float) -> list[Any]:
        return partitions

    def consume(self, *, num_messages: int, timeout: float) -> list[_Message]:
        return self.messages.popleft() if self.messages else []

    def commit(self, *, offsets: list[Any], asynchronous: bool) -> None:
        assert asynchronous is False
        self.commits.append(offsets)

    def close(self) -> None:
        self.closed = True


def _runtime_context(*, task_index: int = 0, parallelism: int = 1):
    return SimpleNamespace(
        task_index=task_index,
        parallelism=parallelism,
        job_id="job-1",
        metric_group=_MetricGroup(),
    )


def _install_consumer(monkeypatch, consumer: _Consumer) -> None:
    def create_consumer(config: dict[str, Any]) -> _Consumer:
        consumer.config = config
        return consumer

    monkeypatch.setattr(confluent_kafka, "Consumer", create_consumer)


def test_confluent_consumer_config_is_used_for_continuous_reads(monkeypatch) -> None:
    consumer = _Consumer()
    _install_consumer(monkeypatch, consumer)
    source = KafkaSource(
        "events",
        bootstrap_servers="broker:9092",
        consumer_config={
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "PLAIN",
            "sasl.username": "user",
            "sasl.password": "secret",
        },
    )

    source.open(_runtime_context())

    assert consumer.config is not None
    assert consumer.config["security.protocol"] == "SASL_SSL"
    assert consumer.config["sasl.mechanism"] == "PLAIN"
    assert consumer.config["sasl.username"] == "user"
    assert consumer.config["sasl.password"] == "secret"
    assert consumer.config["enable.auto.commit"] is False
    source.close()


def test_checkpoint_captures_next_offset_and_commits_only_after_completion(monkeypatch) -> None:
    consumer = _Consumer([_Message(offset=10)])
    _install_consumer(monkeypatch, consumer)
    source = KafkaSource("events", bootstrap_servers="broker:9092", timeout_ms=1)
    source.open(_runtime_context())

    class _Context:
        def __init__(self) -> None:
            self.rows: list[dict[str, Any]] = []
            self.checkpoint_state: dict[str, Any] | None = None

        def collect(self, row: dict[str, Any]) -> None:
            self.rows.append(row)
            self.checkpoint_state = source.snapshot_state(7)
            source.notify_checkpoint_complete(7)
            source.cancel()

        def on_idle(self) -> None:
            raise AssertionError("the test poll is not idle")

    context = _Context()
    source.run(context)

    assert context.rows == [
        {
            "offset": 10,
            "key": b"key",
            "value": b"value",
            "topic": "events",
            "partition": 0,
            "timestamp": 1_234,
            "timestamp_type": 0,
            "headers": {"trace-id": b"abc"},
        }
    ]
    assert context.checkpoint_state == {"version": 1, "positions": {"events": {0: 11}}}
    assert [[(item.topic, item.partition, item.offset) for item in commit] for commit in consumer.commits] == [
        [("events", 0, 11)]
    ]
    assert consumer.closed is True


def test_uncompleted_checkpoint_never_commits_kafka_offsets(monkeypatch) -> None:
    consumer = _Consumer([_Message(offset=4)])
    _install_consumer(monkeypatch, consumer)
    source = KafkaSource("events", bootstrap_servers="broker:9092", timeout_ms=1)
    source.open(_runtime_context())

    class _Context:
        def collect(self, row: dict[str, Any]) -> None:
            source.snapshot_state(8)
            source.cancel()

        def on_idle(self) -> None:
            raise AssertionError("the test poll is not idle")

    source.run(_Context())

    assert consumer.commits == []


def test_empty_poll_marks_input_idle_without_committing(monkeypatch) -> None:
    consumer = _Consumer([])
    _install_consumer(monkeypatch, consumer)
    source = KafkaSource("events", bootstrap_servers="broker:9092", timeout_ms=1)
    source.open(_runtime_context())

    class _Context:
        idle_calls = 0

        def collect(self, row: dict[str, Any]) -> None:
            raise AssertionError("an empty poll cannot emit a row")

        def on_idle(self) -> None:
            self.idle_calls += 1
            source.cancel()

    context = _Context()
    source.run(context)

    assert context.idle_calls == 1
    assert consumer.commits == []


def test_restore_and_parallel_subtask_ownership_determine_assignment(monkeypatch) -> None:
    consumer = _Consumer(partitions=(0, 1, 2, 3))
    _install_consumer(monkeypatch, consumer)
    source = KafkaSource("events", bootstrap_servers="broker:9092")
    source.restore_state({"version": 1, "positions": {"events": {1: 12, 3: 34}}})

    source.open(_runtime_context(task_index=1, parallelism=2))

    assert [(item.topic, item.partition, item.offset) for item in consumer.assignments[-1]] == [
        ("events", 1, 12),
        ("events", 3, 34),
    ]
    source.close()
    assert consumer.closed is True
