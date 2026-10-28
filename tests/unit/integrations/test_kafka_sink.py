# SPDX-License-Identifier: Apache-2.0
"""Unit tests for the streaming Kafka producer lifecycle."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from ray.klein.integrations.kafka import KafkaSink
from ray.klein.integrations.kafka import kafka_sink as sink_module


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


def test_streaming_sink_retries_repeated_queue_pressure_without_reserializing(monkeypatch) -> None:
    class FullOnceProducer(_Producer):
        def __init__(self, config):
            super().__init__(config)
            self.attempts = 0

        def produce(self, topic, *, value, key, on_delivery):
            self.attempts += 1
            if self.attempts <= 2:
                raise BufferError("full")
            super().produce(topic, value=value, key=key, on_delivery=on_delivery)

    producer = FullOnceProducer({})
    monkeypatch.setattr("ray.klein.integrations.kafka.kafka_sink.Producer", lambda _config: producer)
    sink = KafkaSink("events", "localhost:9092", value_serializer="bytes")
    sink.open(_runtime_context())
    serializer = MagicMock(wraps=sink_module._serialize)
    monkeypatch.setattr(sink_module, "_serialize", serializer)

    sink.write({"value": b"payload"})

    assert producer.attempts == 3
    serializer.assert_called_once()
    assert producer.messages == [("events", None, b"{'value': b'payload'}")]


@pytest.mark.parametrize("option", ["delivery.timeout.ms", "message.timeout.ms"])
def test_queue_pressure_obeys_the_total_delivery_deadline(monkeypatch, option) -> None:
    clock = [0.0]
    polls = []

    class FullProducer(_Producer):
        def produce(self, *args, **kwargs):
            raise BufferError("full")

        def poll(self, timeout):
            polls.append(timeout)
            clock[0] += timeout

    monkeypatch.setattr("ray.klein.integrations.kafka.kafka_sink.Producer", FullProducer)
    monkeypatch.setattr("ray.klein.integrations.kafka.kafka_sink.time.monotonic", lambda: clock[0])
    sink = KafkaSink("events", "localhost:9092", producer_config={option: 25_000})
    sink.open(_runtime_context())
    with pytest.raises(TimeoutError, match="queue"):
        sink.write({"id": 1})
    assert polls == [10, 10, 5]


@pytest.mark.parametrize("timeout", [0, -1, float("nan"), float("inf")])
def test_sink_rejects_unbounded_delivery_timeout(timeout) -> None:
    with pytest.raises(ValueError, match=r"delivery\.timeout\.ms"):
        KafkaSink("events", "localhost:9092", producer_config={"delivery.timeout.ms": timeout})


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
