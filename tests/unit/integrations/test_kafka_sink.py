# SPDX-License-Identifier: Apache-2.0
"""Unit tests for the streaming Kafka producer lifecycle."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from ray.klein.integrations.kafka import KafkaSink


class _Producer:
    def __init__(self, config):
        self.config = config
        self.messages = []
        self.callbacks = []
        self.flush_result = 0

    def produce(self, topic, *, value, key, on_delivery):
        self.messages.append((topic, key, value))
        self.callbacks.append(on_delivery)

    def poll(self, _timeout):
        callbacks, self.callbacks = self.callbacks, []
        for callback in callbacks:
            callback(None, object())

    def flush(self, *, timeout):
        self.poll(timeout)
        return self.flush_result


def _runtime_context():
    return SimpleNamespace(metric_group=None, task_index=2)


def test_streaming_sink_serializes_and_flushes_records(monkeypatch) -> None:
    producer = _Producer({})
    producer_factory = MagicMock(return_value=producer)
    monkeypatch.setattr("ray.klein.integrations.kafka.kafka_sink.Producer", producer_factory)
    config = {"acks": "all"}
    sink = KafkaSink(
        "events",
        "broker-a:9092,broker-b:9092",
        key_field="id",
        key_serializer="string",
        value_serializer="json",
        producer_config=config,
    )

    sink.open(_runtime_context())
    sink.write({"id": 7, "name": "Luna"})
    sink.flush()
    sink.close()

    producer_factory.assert_called_once_with({"bootstrap.servers": "broker-a:9092,broker-b:9092", "acks": "all"})
    assert config == {"acks": "all"}
    assert producer.messages == [("events", b"7", b'{"id": 7, "name": "Luna"}')]


def test_streaming_sink_surfaces_delivery_failures(monkeypatch) -> None:
    class FailingProducer(_Producer):
        def poll(self, _timeout):
            callbacks, self.callbacks = self.callbacks, []
            for callback in callbacks:
                callback(RuntimeError("broker rejected message"), object())

    producer = FailingProducer({})
    monkeypatch.setattr("ray.klein.integrations.kafka.kafka_sink.Producer", lambda _config: producer)
    sink = KafkaSink("events", "localhost:9092")
    sink.open(_runtime_context())

    with pytest.raises(RuntimeError, match="failed to deliver 1 message") as error:
        sink.write({"id": 1})

    assert isinstance(error.value.__cause__, RuntimeError)
    assert str(error.value.__cause__) == "broker rejected message"


def test_streaming_sink_retries_once_when_the_producer_queue_is_full(monkeypatch) -> None:
    class FullOnceProducer(_Producer):
        def __init__(self, config):
            super().__init__(config)
            self.attempts = 0

        def produce(self, topic, *, value, key, on_delivery):
            self.attempts += 1
            if self.attempts == 1:
                raise BufferError("full")
            super().produce(topic, value=value, key=key, on_delivery=on_delivery)

    producer = FullOnceProducer({})
    monkeypatch.setattr("ray.klein.integrations.kafka.kafka_sink.Producer", lambda _config: producer)
    sink = KafkaSink("events", "localhost:9092", value_serializer="bytes")
    sink.open(_runtime_context())

    sink.write({"value": b"payload"})

    assert producer.attempts == 2
    assert producer.messages == [("events", None, b"{'value': b'payload'}")]


def test_streaming_sink_rejects_messages_left_after_flush(monkeypatch) -> None:
    producer = _Producer({})
    producer.flush_result = 2
    monkeypatch.setattr("ray.klein.integrations.kafka.kafka_sink.Producer", lambda _config: producer)
    sink = KafkaSink("events", "localhost:9092")
    sink.open(_runtime_context())

    with pytest.raises(RuntimeError, match="still had 2 message"):
        sink.flush()


@pytest.mark.parametrize("parameter", ["key_serializer", "value_serializer"])
def test_streaming_sink_rejects_unknown_serializers(parameter) -> None:
    with pytest.raises(ValueError, match=f"{parameter} must be one of"):
        KafkaSink("events", "localhost:9092", **{parameter: "pickle"})
