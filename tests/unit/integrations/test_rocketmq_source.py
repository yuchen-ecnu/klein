# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import sys
import threading
from enum import IntEnum
from types import ModuleType, SimpleNamespace
from typing import Any, ClassVar

import pytest
from tests.support.waiting import wait_until

from ray.klein.integrations.rocketmq import RocketMQSource


class _Metric:
    def inc(self, value: int | float = 1) -> None:
        pass

    def set(self, value: int | float) -> None:
        pass


class _MetricGroup:
    def metric(self, spec) -> _Metric:
        return _Metric()

    builtin_counter = metric
    builtin_gauge = metric


class _ConsumeStatus(IntEnum):
    CONSUME_SUCCESS = 0
    RECONSUME_LATER = 1


class _MessageModel(IntEnum):
    BROADCASTING = 0
    CLUSTERING = 1


class _Message:
    topic = "orders"
    id = "message-1"
    keys = b"order-7"
    body = b'{"id": 7}'
    tags = b"paid"
    queue_id = 2
    queue_offset = 19
    commit_log_offset = 91
    born_timestamp = 1_700_000_000_000
    store_timestamp = 1_700_000_000_100
    reconsume_times = 0
    delay_time_level = 0
    store_size = 128
    prepared_transaction_offset = 0


class _PushConsumer:
    instances: ClassVar[list[_PushConsumer]] = []

    def __init__(self, group_id: str, *, orderly: bool, message_model: _MessageModel) -> None:
        self.group_id = group_id
        self.orderly = orderly
        self.message_model = message_model
        self.callback = None
        self.expression = None
        self.started = False
        self.shutdown_called = False
        self.options: dict[str, Any] = {}
        self.instances.append(self)

    def set_name_server_address(self, address: str) -> None:
        self.options["name_server_address"] = address

    def set_instance_name(self, name: str) -> None:
        self.options["instance_name"] = name

    def set_thread_count(self, count: int) -> None:
        self.options["thread_count"] = count

    def set_message_trace(self, enabled: bool) -> None:
        self.options["message_trace"] = enabled

    def set_session_credentials(self, access_key: str, access_secret: str, channel: str) -> None:
        self.options["credentials"] = (access_key, access_secret, channel)

    def set_ssl_enable(self, enabled: bool) -> None:
        self.options["ssl_enabled"] = enabled

    def set_ssl_property_file(self, path: str) -> None:
        self.options["ssl_property_file"] = path

    def subscribe(self, topic: str, callback, *, expression: str) -> None:
        self.options["topic"] = topic
        self.callback = callback
        self.expression = expression

    def start(self) -> None:
        self.started = True

    def shutdown(self) -> None:
        self.shutdown_called = True


@pytest.fixture(autouse=True)
def fake_rocketmq(monkeypatch):
    package = ModuleType("rocketmq")
    package.__path__ = []  # type: ignore[attr-defined]
    client = ModuleType("rocketmq.client")
    client.ConsumeStatus = _ConsumeStatus
    client.PushConsumer = _PushConsumer
    ffi = ModuleType("rocketmq.ffi")
    ffi.MessageModel = _MessageModel
    monkeypatch.setitem(sys.modules, "rocketmq", package)
    monkeypatch.setitem(sys.modules, "rocketmq.client", client)
    monkeypatch.setitem(sys.modules, "rocketmq.ffi", ffi)
    _PushConsumer.instances.clear()


def _runtime_context():
    return SimpleNamespace(task_index=1, parallelism=3, job_id="job-1", metric_group=_MetricGroup())


def test_message_is_acknowledged_only_after_downstream_emit() -> None:
    source = RocketMQSource(
        "orders",
        name_server_address="nameserver:9876",
        consumer_group="klein-orders",
        tag_expression="paid",
        access_key="access",
        access_secret="secret",
        ssl_enabled=True,
        ssl_property_file="/ssl.properties",
        consumer_threads=4,
        poll_timeout_ms=10,
        message_trace_enabled=True,
    )
    source.open(_runtime_context())
    consumer = _PushConsumer.instances[-1]
    callback_result: list[_ConsumeStatus] = []
    callback_thread = threading.Thread(target=lambda: callback_result.append(consumer.callback(_Message())))
    callback_thread.start()

    class _Context:
        def __init__(self) -> None:
            self.rows: list[dict[str, Any]] = []

        def collect(self, row: dict[str, Any]) -> None:
            self.rows.append(row)
            assert callback_result == []
            source.cancel()

        def on_idle(self) -> None:
            raise AssertionError("the queued message is not idle")

    context = _Context()
    source.run(context)
    callback_thread.join(timeout=1)

    assert callback_result == [_ConsumeStatus.CONSUME_SUCCESS]
    assert context.rows == [
        {
            "topic": "orders",
            "message_id": "message-1",
            "key": b"order-7",
            "value": b'{"id": 7}',
            "tags": b"paid",
            "queue_id": 2,
            "queue_offset": 19,
            "commit_log_offset": 91,
            "born_timestamp": 1_700_000_000_000,
            "store_timestamp": 1_700_000_000_100,
            "reconsume_times": 0,
            "delay_time_level": 0,
            "store_size": 128,
            "prepared_transaction_offset": 0,
        }
    ]
    assert consumer.options == {
        "name_server_address": "nameserver:9876",
        "instance_name": "ray-klein-job-1-1",
        "thread_count": 4,
        "message_trace": True,
        "credentials": ("access", "secret", "KLEIN"),
        "ssl_enabled": True,
        "ssl_property_file": "/ssl.properties",
        "topic": "orders",
    }
    assert consumer.expression == "paid"
    assert consumer.started is True
    assert consumer.shutdown_called is True


def test_cancel_returns_reconsume_later_for_a_message_not_emitted() -> None:
    source = RocketMQSource(
        "orders",
        name_server_address="nameserver:9876",
        consumer_group="klein-orders",
        poll_timeout_ms=10,
    )
    source.open(_runtime_context())
    consumer = _PushConsumer.instances[-1]
    callback_result: list[_ConsumeStatus] = []
    callback_thread = threading.Thread(target=lambda: callback_result.append(consumer.callback(_Message())))
    callback_thread.start()

    wait_until(
        lambda: not source._messages.empty(),
        timeout=1,
        interval=0.001,
        description="RocketMQ callback queueing",
    )
    source.close()
    callback_thread.join(timeout=1)

    assert callback_result == [_ConsumeStatus.RECONSUME_LATER]
    assert consumer.shutdown_called is True


def test_default_options_support_the_stable_client_without_trace_or_ssl(monkeypatch) -> None:
    monkeypatch.delattr(_PushConsumer, "set_message_trace")
    monkeypatch.delattr(_PushConsumer, "set_ssl_enable")
    monkeypatch.delattr(_PushConsumer, "set_ssl_property_file")
    source = RocketMQSource(
        "orders",
        name_server_address="nameserver:9876",
        consumer_group="klein-orders",
    )

    source.open(_runtime_context())
    consumer = _PushConsumer.instances[-1]
    source.close()

    assert consumer.started is True
    assert consumer.shutdown_called is True


def test_checkpoint_state_is_versioned_but_broker_managed() -> None:
    source = RocketMQSource(
        "orders",
        name_server_address="nameserver:9876",
        consumer_group="klein-orders",
    )

    assert source.snapshot_state(7) == {"version": 1}
    source.restore_state({"version": 1})
    with pytest.raises(ValueError, match="Unsupported RocketMQ source checkpoint state"):
        source.restore_state({"version": 2})
