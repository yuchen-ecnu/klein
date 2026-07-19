# SPDX-License-Identifier: Apache-2.0
import pytest
import ray.data

from ray.klein.api.job_client import JobClient
from ray.klein.api.klein_context import KleinContext
from ray.klein.api.row_kind import RowKind
from ray.klein.config.runtime_execution_mode import RuntimeExecutionMode
from ray.klein.integrations.kafka import KafkaSink, KafkaSource
from ray.klein.runtime.graph.logical_graph import LogicalGraph


def test_continuous_kafka_source_rejects_invalid_offsets() -> None:
    with pytest.raises(ValueError, match="Invalid start offset"):
        KafkaSource(
            "events",
            bootstrap_servers="localhost:9092",
            start_offset={"events": {0: []}},  # type: ignore[dict-item]
        )


def test_read_kafka_reuses_the_ray_data_2_56_contract(monkeypatch) -> None:
    def read_kafka(*args, **kwargs):
        raise AssertionError("contract construction must be lazy")

    monkeypatch.setattr(ray.data, "read_kafka", read_kafka, raising=False)
    context = KleinContext()
    stream = context.read_kafka(
        "events",
        bootstrap_servers="localhost:9092",
        start_offset="earliest",
        end_offset="latest",
        consumer_config={"group.id": "consumer"},
        override_num_blocks=3,
        timeout_ms=10_000,
    )

    call = stream.stream_operator.logical_function.batch_lowering

    assert call.target == "read_kafka"
    assert call.args == ("events",)
    assert call.kwargs == {
        "bootstrap_servers": "localhost:9092",
        "trigger": "once",
        "start_offset": "earliest",
        "end_offset": "latest",
        "consumer_config": {"group.id": "consumer"},
        "num_cpus": None,
        "num_gpus": None,
        "memory": None,
        "ray_remote_args": None,
        "override_num_blocks": 3,
        "timeout_ms": 10_000,
    }
    assert stream.stream_operator.bounded is True


def test_read_kafka_continuous_builds_an_unbounded_checkpointed_source() -> None:
    context = KleinContext()
    stream = context.read_kafka(
        "events",
        bootstrap_servers="localhost:9092",
        trigger="continuous",
        start_offset="latest",
        consumer_config={"group.id": "consumer"},
        timeout_ms=2_000,
        concurrency=3,
        partition_discovery_interval_ms=5_000,
        max_batch_size=250,
    )

    logical_function = stream.stream_operator.logical_function
    assert logical_function.function is KafkaSource
    assert logical_function.batch_supported is False
    assert logical_function.constructor_args == ("events",)
    assert logical_function.constructor_kwargs == {
        "bootstrap_servers": "localhost:9092",
        "start_offset": "latest",
        "consumer_config": {"group.id": "consumer"},
        "timeout_ms": 2_000,
        "partition_discovery_interval_ms": 5_000,
        "max_batch_size": 250,
    }
    assert stream.stream_operator.bounded is False
    assert stream.concurrency == 3


def test_write_kafka_reuses_the_ray_data_2_56_contract() -> None:
    context = KleinContext()
    source = context.data.from_items([{"key": "a", "value": 1}])

    sink = source.write_kafka(
        "events",
        "localhost:9092",
        key_field="key",
        key_serializer="string",
        value_serializer="json",
        producer_config={"acks": "all"},
        concurrency=2,
    )
    logical_function = sink.stream_operator.logical_function
    call = logical_function.batch_lowering

    assert logical_function.function is KafkaSink
    assert call.target == "write_kafka"
    assert call.args == ("events", "localhost:9092")
    assert call.kwargs == {
        "key_field": "key",
        "key_serializer": "string",
        "value_serializer": "json",
        "producer_config": {"acks": "all"},
        "ray_remote_args": None,
        "concurrency": 2,
    }


def test_continuous_kafka_pipeline_uses_the_streaming_sink() -> None:
    context = KleinContext()
    source = context.read_kafka(
        "input-events",
        bootstrap_servers="localhost:9092",
        trigger="continuous",
        start_offset="latest",
    )
    sink = source.write_kafka("output-events", "localhost:9092")
    graph = LogicalGraph.from_sinks(context.sinks, "continuous-kafka", context.config)

    assert sink.stream_operator.logical_function.function is KafkaSink
    assert sink.stream_operator.logical_function.batch_supported is True
    assert JobClient._determine_runtime_mode(graph) is RuntimeExecutionMode.STREAMING


def test_read_canal_builds_an_unbounded_changelog_source() -> None:
    context = KleinContext()
    stream = context.read_canal(
        "canal-orders",
        bootstrap_servers="localhost:9092",
        start_offset="latest",
        consumer_config={"group.id": "canal-consumer"},
        concurrency=3,
        include_metadata=False,
        ddl_handling="fail",
    )

    logical_function = stream.stream_operator.logical_function
    assert logical_function.function is KafkaSource
    assert logical_function.constructor_args == ("canal-orders",)
    assert logical_function.constructor_kwargs == {
        "bootstrap_servers": "localhost:9092",
        "start_offset": "latest",
        "consumer_config": {"group.id": "canal-consumer"},
        "timeout_ms": None,
        "partition_discovery_interval_ms": 30_000,
        "max_batch_size": 1_000,
        "value_format": "canal-json",
        "format_options": {"include_metadata": False, "ddl_handling": "fail"},
    }
    assert stream.stream_operator.bounded is False
    assert stream.concurrency == 3
    assert stream.changelog_mode == frozenset(RowKind)


def test_read_kafka_accepts_canal_json_as_a_value_format() -> None:
    context = KleinContext()
    stream = context.read_kafka(
        "canal-orders",
        bootstrap_servers="localhost:9092",
        trigger="continuous",
        value_format="canal-json",
        format_options={"include_metadata": False},
    )

    logical_function = stream.stream_operator.logical_function
    assert logical_function.function is KafkaSource
    assert logical_function.constructor_kwargs["value_format"] == "canal-json"
    assert logical_function.constructor_kwargs["format_options"] == {
        "include_metadata": False,
        "ddl_handling": "ignore",
    }
    assert stream.changelog_mode == frozenset(RowKind)


def test_canal_json_value_format_requires_continuous_kafka() -> None:
    with pytest.raises(ValueError, match="requires trigger='continuous'"):
        KleinContext().read_kafka(
            "canal-orders",
            bootstrap_servers="localhost:9092",
            value_format="canal-json",
        )
