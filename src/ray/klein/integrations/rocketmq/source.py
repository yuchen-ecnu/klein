# SPDX-License-Identifier: Apache-2.0
"""Unbounded Apache RocketMQ source backed by rocketmq-client-python."""

from __future__ import annotations

import queue
import threading
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal

from ray.klein._internal.logging import get_logger
from ray.klein.api.runtime_context import RuntimeContext
from ray.klein.api.source_context import SourceContext
from ray.klein.api.source_function import SourceFunction
from ray.klein.observability.metrics.metric_catalog import KleinMetrics
from ray.klein.observability.metrics.metrics import Counter, Gauge

if TYPE_CHECKING:
    from rocketmq.client import PushConsumer, ReceivedMessage

logger = get_logger(__name__)

_STATE_VERSION = 1


@dataclass(eq=False)
class _PendingMessage:
    record: dict[str, Any]
    completed: threading.Event = field(default_factory=threading.Event)
    processing: bool = False
    acknowledged: bool = False


class _RocketMQSourceMetrics:
    def __init__(self, runtime_context: RuntimeContext) -> None:
        group = runtime_context.metric_group
        self.received_records: Counter = group.builtin_counter(KleinMetrics.ROCKETMQ_RECEIVED_RECORDS)
        self.acknowledged_records: Counter = group.builtin_counter(KleinMetrics.ROCKETMQ_ACKNOWLEDGED_RECORDS)
        self.pending_records: Gauge = group.builtin_gauge(KleinMetrics.ROCKETMQ_PENDING_RECORDS)
        self.errors: Counter = group.builtin_counter(KleinMetrics.ROCKETMQ_ERRORS)


class RocketMQSource(SourceFunction):
    """Continuously consume one Apache RocketMQ topic.

    ``rocketmq-client-python`` exposes a callback-based PushConsumer rather
    than a seekable pull consumer. The callback copies the native message into
    a bounded Python queue and waits until the source thread has emitted it
    downstream before returning ``CONSUME_SUCCESS``. RocketMQ consumer-group
    progress is therefore the recovery boundary; the opaque Klein checkpoint
    state records only the connector-state version.
    """

    def __init__(
        self,
        topic: str,
        *,
        name_server_address: str,
        consumer_group: str,
        tag_expression: str = "*",
        message_model: Literal["clustering", "broadcasting"] = "clustering",
        orderly: bool = False,
        access_key: str | None = None,
        access_secret: str | None = None,
        channel: str = "KLEIN",
        ssl_enabled: bool = False,
        ssl_property_file: str | None = None,
        consumer_threads: int = 20,
        max_pending_messages: int = 1_000,
        poll_timeout_ms: int = 1_000,
        message_trace_enabled: bool = False,
    ) -> None:
        self._topic = _nonempty_string(topic, "topic")
        self._name_server_address = _nonempty_string(name_server_address, "name_server_address")
        self._consumer_group = _nonempty_string(consumer_group, "consumer_group")
        self._tag_expression = _nonempty_string(tag_expression, "tag_expression")
        if message_model not in {"clustering", "broadcasting"}:
            raise ValueError("message_model must be 'clustering' or 'broadcasting'")
        if (access_key is None) != (access_secret is None):
            raise ValueError("access_key and access_secret must be provided together")
        if access_key is not None:
            _nonempty_string(access_key, "access_key")
            _nonempty_string(access_secret, "access_secret")
            _nonempty_string(channel, "channel")
        _positive_integer(consumer_threads, "consumer_threads")
        _positive_integer(max_pending_messages, "max_pending_messages")
        _positive_integer(poll_timeout_ms, "poll_timeout_ms")
        if ssl_property_file is not None:
            _nonempty_string(ssl_property_file, "ssl_property_file")

        self._message_model = message_model
        self._orderly = bool(orderly)
        self._access_key = access_key
        self._access_secret = access_secret
        self._channel = channel
        self._ssl_enabled = bool(ssl_enabled)
        self._ssl_property_file = ssl_property_file
        self._consumer_threads = consumer_threads
        self._message_trace_enabled = bool(message_trace_enabled)
        self._poll_timeout_seconds = poll_timeout_ms / 1_000.0
        self._messages: queue.Queue[_PendingMessage] = queue.Queue(maxsize=max_pending_messages)

        self._consumer: PushConsumer | None = None
        self._consume_success: Any = None
        self._reconsume_later: Any = None
        self._metrics: _RocketMQSourceMetrics | None = None
        self._stop_event = threading.Event()
        self._state_lock = threading.Lock()
        self._shutdown_lock = threading.Lock()
        self._pending: set[_PendingMessage] = set()

    def open(self, runtime_context: RuntimeContext) -> None:
        from rocketmq.client import ConsumeStatus, PushConsumer
        from rocketmq.ffi import MessageModel

        self._stop_event.clear()
        self._metrics = _RocketMQSourceMetrics(runtime_context)
        self._consume_success = ConsumeStatus.CONSUME_SUCCESS
        self._reconsume_later = ConsumeStatus.RECONSUME_LATER
        model = MessageModel.CLUSTERING if self._message_model == "clustering" else MessageModel.BROADCASTING
        consumer = PushConsumer(self._consumer_group, orderly=self._orderly, message_model=model)
        self._consumer = consumer
        try:
            consumer.set_name_server_address(self._name_server_address)
            consumer.set_instance_name(f"ray-klein-{runtime_context.job_id}-{runtime_context.task_index}")
            consumer.set_thread_count(self._consumer_threads)
            if self._message_trace_enabled:
                _optional_client_method(consumer, "set_message_trace", "message_trace_enabled")(True)
            if self._access_key is not None and self._access_secret is not None:
                consumer.set_session_credentials(self._access_key, self._access_secret, self._channel)
            if self._ssl_enabled:
                _optional_client_method(consumer, "set_ssl_enable", "ssl_enabled")(True)
            if self._ssl_property_file is not None:
                _optional_client_method(consumer, "set_ssl_property_file", "ssl_property_file")(self._ssl_property_file)
            consumer.subscribe(self._topic, self._on_message, expression=self._tag_expression)
            consumer.start()
        except Exception:
            self._record_error()
            self._shutdown_consumer()
            raise
        logger.info(
            "RocketMQ source subtask %d/%d joined group %s for topic %s",
            runtime_context.task_index,
            runtime_context.parallelism,
            self._consumer_group,
            self._topic,
        )

    def run(self, context: SourceContext) -> None:
        if self._consumer is None:
            raise RuntimeError("RocketMQSource must be opened before it is run")
        try:
            while not self._stop_event.is_set():
                try:
                    pending = self._messages.get(timeout=self._poll_timeout_seconds)
                except queue.Empty:
                    context.on_idle()
                    continue

                with self._state_lock:
                    pending.processing = True
                try:
                    collect_durable = getattr(context, "collect_durable", None)
                    if callable(collect_durable):
                        collect_durable(pending.record)
                    else:
                        # Compatibility with third-party SourceContext facades.
                        context.collect(pending.record)
                except BaseException:
                    self._record_error()
                    raise
                else:
                    pending.acknowledged = True
                    if self._metrics is not None:
                        self._metrics.acknowledged_records.inc()
                finally:
                    with self._state_lock:
                        self._pending.discard(pending)
                        self._update_pending_metric_locked()
                    pending.completed.set()
                    self._messages.task_done()
        finally:
            self._release_waiting_callbacks()
            self._shutdown_consumer()

    def snapshot_state(self, checkpoint_id: int) -> dict[str, int]:
        del checkpoint_id
        return {"version": _STATE_VERSION}

    def restore_state(self, state: Any) -> None:
        if state is None:
            return
        if not isinstance(state, dict) or state != {"version": _STATE_VERSION}:
            raise ValueError("Unsupported RocketMQ source checkpoint state")

    def cancel(self) -> None:
        self._stop_event.set()
        self._release_waiting_callbacks()

    def close(self) -> None:
        self.cancel()
        self._shutdown_consumer()

    def _on_message(self, message: ReceivedMessage) -> Any:
        if self._stop_event.is_set():
            return self._reconsume_later
        try:
            pending = _PendingMessage(_message_record(message))
            with self._state_lock:
                self._pending.add(pending)
                self._update_pending_metric_locked()
            while not self._stop_event.is_set():
                try:
                    self._messages.put(pending, timeout=min(0.1, self._poll_timeout_seconds))
                    break
                except queue.Full:
                    continue
            else:
                self._discard_pending(pending)
                return self._reconsume_later

            if self._metrics is not None:
                self._metrics.received_records.inc()
            while not pending.completed.wait(timeout=min(0.1, self._poll_timeout_seconds)):
                if self._stop_event.is_set():
                    break
            return self._consume_success if pending.acknowledged else self._reconsume_later
        except BaseException:
            self._record_error()
            logger.exception("RocketMQ message callback failed")
            return self._reconsume_later

    def _discard_pending(self, pending: _PendingMessage) -> None:
        with self._state_lock:
            self._pending.discard(pending)
            self._update_pending_metric_locked()
        pending.completed.set()

    def _release_waiting_callbacks(self) -> None:
        with self._state_lock:
            waiting = [pending for pending in self._pending if not pending.processing]
            for pending in waiting:
                self._pending.discard(pending)
            self._update_pending_metric_locked()
        for pending in waiting:
            pending.completed.set()

    def _shutdown_consumer(self) -> None:
        with self._shutdown_lock:
            consumer = self._consumer
            if consumer is None:
                return
            self._consumer = None
            try:
                consumer.shutdown()
            except Exception:
                self._record_error()
                logger.warning("RocketMQ consumer shutdown failed", exc_info=True)

    def _update_pending_metric_locked(self) -> None:
        if self._metrics is not None:
            self._metrics.pending_records.set(len(self._pending))

    def _record_error(self) -> None:
        if self._metrics is not None:
            self._metrics.errors.inc()


def _message_record(message: ReceivedMessage) -> dict[str, Any]:
    return {
        "topic": message.topic,
        "message_id": message.id,
        "key": _copy_bytes(message.keys),
        "value": _copy_bytes(message.body),
        "tags": _copy_bytes(message.tags),
        "queue_id": message.queue_id,
        "queue_offset": message.queue_offset,
        "commit_log_offset": message.commit_log_offset,
        "born_timestamp": message.born_timestamp,
        "store_timestamp": message.store_timestamp,
        "reconsume_times": message.reconsume_times,
        "delay_time_level": message.delay_time_level,
        "store_size": message.store_size,
        "prepared_transaction_offset": message.prepared_transaction_offset,
    }


def _copy_bytes(value: Any) -> bytes | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value.encode()
    return bytes(value)


def _nonempty_string(value: Any, name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string")
    if not value.strip():
        raise ValueError(f"{name} must not be empty")
    return value


def _positive_integer(value: Any, name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be a positive integer")
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero")


def _optional_client_method(consumer: Any, method_name: str, option_name: str) -> Any:
    method = getattr(consumer, method_name, None)
    if not callable(method):
        raise RuntimeError(
            f"The installed rocketmq-client-python does not support {option_name}; upgrade the client or disable it"
        )
    return method


__all__ = ["RocketMQSource"]
