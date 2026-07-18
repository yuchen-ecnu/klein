# SPDX-License-Identifier: Apache-2.0
"""Unit tests for Redis sink buffering and lifecycle."""

from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from ray.klein.integrations.redis import RedisConnectionConfig, RedisSink, RedisSinkConfig


def _sink(**connection_options) -> RedisSink:
    connection = RedisConnectionConfig("localhost", max_retries=connection_options.pop("max_retries", 5))
    config = RedisSinkConfig(flush_interval=timedelta(seconds=10))
    return RedisSink(connection, lambda row: row["key"], lambda row: row["value"], config=config)


def _redis_context(*, execute_error: Exception | None = None):
    pipeline = MagicMock()
    if execute_error is not None:
        pipeline.execute.side_effect = execute_error
    pipeline_context = MagicMock()
    pipeline_context.__enter__.return_value = pipeline

    client = MagicMock()
    client.pipeline.return_value = pipeline_context
    client_context = MagicMock()
    client_context.__enter__.return_value = client
    return client_context, client, pipeline


def test_failed_transaction_restores_the_entire_batch(monkeypatch) -> None:
    sink = _sink(max_retries=0)
    sink._pool = object()
    client_context, _client, _pipeline = _redis_context(execute_error=ConnectionError("lost"))
    monkeypatch.setattr("ray.klein.integrations.redis.sink.redis.Redis", lambda **kwargs: client_context)
    record = {"key": "a", "value": "1"}
    sink.write(record)

    with pytest.raises(ConnectionError, match="lost"):
        sink._write_batch()

    assert list(sink._buffer) == [record]
    assert sink._inflight == 0


def test_close_flushes_and_joins_the_background_thread(monkeypatch) -> None:
    pool = MagicMock()
    monkeypatch.setattr(
        RedisConnectionConfig,
        "create_pool",
        lambda self, *, retries=None: pool,
    )
    client_context, client, pipeline = _redis_context()
    monkeypatch.setattr("ray.klein.integrations.redis.sink.redis.Redis", lambda **kwargs: client_context)
    metric_group = MagicMock()
    context = SimpleNamespace(metric_group=metric_group, task_index=0)
    sink = _sink()
    sink.open(context)
    thread = sink._flush_thread
    sink.write({"key": "a", "value": "1"})

    sink.close()

    assert thread is not None and not thread.is_alive()
    client.pipeline.assert_called_once_with(transaction=True)
    pipeline.set.assert_called_once_with("a", "1")
    pipeline.execute.assert_called_once_with()
    pool.disconnect.assert_called_once()
