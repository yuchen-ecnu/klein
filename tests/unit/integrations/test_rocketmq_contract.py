# SPDX-License-Identifier: Apache-2.0

import pytest

from ray.klein.api.klein_context import KleinContext
from ray.klein.integrations.rocketmq import RocketMQSource


def test_read_rocketmq_builds_an_unbounded_source(monkeypatch) -> None:
    monkeypatch.setattr("ray.klein.api.klein_context._rocketmq_source_class", lambda: RocketMQSource)
    context = KleinContext()

    stream = context.read_rocketmq(
        "orders",
        name_server_address="nameserver:9876",
        consumer_group="klein-orders",
        tag_expression="paid || refunded",
        access_key="access",
        access_secret="secret",
        channel="INTERNAL",
        ssl_enabled=True,
        ssl_property_file="/etc/rocketmq/ssl.properties",
        consumer_threads=8,
        max_pending_messages=128,
        poll_timeout_ms=250,
        message_trace_enabled=True,
        concurrency=4,
    )

    logical_function = stream.stream_operator.logical_function
    assert logical_function.function is RocketMQSource
    assert logical_function.batch_supported is False
    assert logical_function.constructor_args == ("orders",)
    assert logical_function.constructor_kwargs == {
        "name_server_address": "nameserver:9876",
        "consumer_group": "klein-orders",
        "tag_expression": "paid || refunded",
        "message_model": "clustering",
        "orderly": False,
        "access_key": "access",
        "access_secret": "secret",
        "channel": "INTERNAL",
        "ssl_enabled": True,
        "ssl_property_file": "/etc/rocketmq/ssl.properties",
        "consumer_threads": 8,
        "max_pending_messages": 128,
        "poll_timeout_ms": 250,
        "message_trace_enabled": True,
    }
    assert stream.stream_operator.bounded is False
    assert stream.concurrency == 4


def test_broadcasting_input_rejects_multiple_source_subtasks() -> None:
    context = KleinContext()

    with pytest.raises(ValueError, match="broadcasting RocketMQ input requires concurrency=1"):
        context.read_rocketmq(
            "orders",
            name_server_address="nameserver:9876",
            consumer_group="klein-orders",
            message_model="broadcasting",
            concurrency=2,
        )


def test_rocketmq_source_validates_credentials_as_a_pair() -> None:
    with pytest.raises(ValueError, match="provided together"):
        RocketMQSource(
            "orders",
            name_server_address="nameserver:9876",
            consumer_group="klein-orders",
            access_key="access",
        )
