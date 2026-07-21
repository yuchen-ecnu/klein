# SPDX-License-Identifier: Apache-2.0
"""Unit tests for Redis lookup decoding and ownership."""

from collections.abc import Iterable
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock, call

import numpy
import pytest
from redis import RedisError

from ray.klein.api.runtime_info import RuntimeInfo
from ray.klein.integrations.redis import (
    RedisConnectionConfig,
    RedisDataType,
    RedisMissingKeyFilter,
    RedisValueLookup,
)
from ray.klein.integrations.redis._lookup_client import _RedisLookupClient
from ray.klein.observability.metrics.metric_catalog import KleinMetrics


def _pool(monkeypatch):
    pool = MagicMock(name="redis-pool")
    monkeypatch.setattr(
        RedisConnectionConfig,
        "create_pool",
        lambda self, *, retries=None: pool,
    )
    return pool


def _install_client(monkeypatch):
    client = MagicMock(name="redis-client")
    client_context = MagicMock(name="redis-client-context")
    client_context.__enter__.return_value = client

    pipeline = MagicMock(name="redis-pipeline")
    pipeline_context = MagicMock(name="redis-pipeline-context")
    pipeline_context.__enter__.return_value = pipeline
    client.pipeline.return_value = pipeline_context

    factory = MagicMock(name="redis-factory", return_value=client_context)
    monkeypatch.setattr("ray.klein.integrations.redis._lookup_client.redis.Redis", factory)
    return client, pipeline, factory


def _runtime_context(*, batch: bool, batch_format: str = "native"):
    duration = MagicMock(name="lookup-duration")
    batch_size = MagicMock(name="lookup-batch-size")
    failures = MagicMock(name="lookup-failures")
    metric_group = MagicMock(name="metric-group")
    metric_group.builtin_histogram.side_effect = [duration, batch_size]
    metric_group.builtin_counter.return_value = failures
    runtime_info = RuntimeInfo(batch_size=3, batch_timeout=1, batch_format=batch_format) if batch else RuntimeInfo()
    return SimpleNamespace(runtime_info=runtime_info, metric_group=metric_group), duration, batch_size, failures


def test_hash_lookup_preserves_missing_requested_fields(monkeypatch) -> None:
    pool = MagicMock()
    monkeypatch.setattr(
        RedisConnectionConfig,
        "create_pool",
        lambda self, *, retries=None: pool,
    )
    lookup = RedisValueLookup(
        RedisConnectionConfig("localhost"),
        lambda row: row["key"],
        data_type=RedisDataType.HASH,
        hash_fields=["name", "missing"],
    )

    assert lookup._decode([b"Ada", None]) == {"name": "Ada", "missing": None}

    lookup.close()
    pool.disconnect.assert_called_once_with()


def test_lookup_accepts_already_decoded_responses(monkeypatch) -> None:
    monkeypatch.setattr(
        RedisConnectionConfig,
        "create_pool",
        lambda self, *, retries=None: MagicMock(),
    )
    lookup = RedisValueLookup(RedisConnectionConfig("localhost"), lambda row: row["key"])

    assert lookup._decode("value") == "value"


def test_batch_lookup_accepts_numpy_columns(monkeypatch) -> None:
    pool = MagicMock()
    monkeypatch.setattr(
        RedisConnectionConfig,
        "create_pool",
        lambda self, *, retries=None: pool,
    )
    lookup = RedisValueLookup(
        RedisConnectionConfig("localhost"),
        lambda batch: batch["key"],
        key_prefix="user",
    )

    assert lookup._lookup.batch_keys({"key": numpy.array(["Ada", "Grace"])}) == [
        "user:Ada",
        "user:Grace",
    ]


def test_batch_config_keeps_ray_data_elementwise_rows_scalar(monkeypatch) -> None:
    monkeypatch.setattr(
        RedisConnectionConfig,
        "create_pool",
        lambda self, *, retries=None: MagicMock(),
    )
    runtime_context = SimpleNamespace(
        runtime_info=RuntimeInfo(batch_size=2, batch_timeout=3, batch_format="default"),
        metric_group=None,
    )
    lookup = RedisValueLookup(
        RedisConnectionConfig("localhost"),
        lambda row: row["key"],
        key_prefix="user",
        runtime_context=runtime_context,
    )

    assert lookup._lookup.resolve_keys({"key": "Ada"}) == "user:Ada"
    assert lookup._lookup.resolve_keys({"key": numpy.array(["Ada", "Grace"])}) == [
        "user:Ada",
        "user:Grace",
    ]


def test_set_lookup_returns_a_stable_ray_data_compatible_list(monkeypatch) -> None:
    monkeypatch.setattr(
        RedisConnectionConfig,
        "create_pool",
        lambda self, *, retries=None: MagicMock(),
    )
    lookup = RedisValueLookup(
        RedisConnectionConfig("localhost"),
        lambda row: row["key"],
        data_type=RedisDataType.SET,
    )

    assert lookup._decode({b"Grace", b"Ada"}) == ["Ada", "Grace"]


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("host", "", "host must be a non-empty string"),
        ("host", None, "host must be a non-empty string"),
        ("port", 0, "port must be between"),
        ("port", 65536, "port must be between"),
        ("database", -1, "database must be non-negative"),
        ("max_connections", 0, "max_connections must be positive"),
        ("max_retries", -1, "max_retries must be non-negative"),
        ("timeout", timedelta(0), "timeout must be positive"),
        ("max_retry_delay", timedelta(0), "max_retry_delay must be positive"),
    ],
)
def test_connection_config_rejects_invalid_values(field, value, message) -> None:
    options = {"host": "localhost", field: value}

    with pytest.raises(ValueError, match=message):
        RedisConnectionConfig(**options)


def test_connection_config_creates_pool_with_canonical_options(monkeypatch) -> None:
    created_pool = object()
    pool_factory = MagicMock(return_value=created_pool)
    monkeypatch.setattr(
        "ray.klein.integrations.redis.redis_connection_config.redis.BlockingConnectionPool",
        pool_factory,
    )
    supplied_options = {
        "decode_responses": False,
        "host": "ignored",
        "socket_timeout": 99,
    }
    config = RedisConnectionConfig(
        "redis.internal",
        port=6380,
        database=2,
        username="reader",
        password="secret",
        max_connections=7,
        timeout=timedelta(seconds=2),
        max_retries=4,
        max_retry_delay=timedelta(seconds=3),
        connection_options=supplied_options,
    )
    supplied_options["decode_responses"] = True

    assert config.create_pool(retries=2) is created_pool

    options = pool_factory.call_args.kwargs
    assert options == {
        "decode_responses": False,
        "host": "redis.internal",
        "port": 6380,
        "db": 2,
        "username": "reader",
        "password": "secret",
        "max_connections": 7,
        "socket_timeout": 2.0,
        "socket_connect_timeout": 2.0,
        "retry": options["retry"],
    }
    assert options["retry"].get_retries() == 2
    assert "secret" not in repr(config)
    with pytest.raises(TypeError):
        config.connection_options["new"] = "value"
    with pytest.raises(ValueError, match="retries must be non-negative"):
        config.create_pool(retries=-1)


def test_connection_config_uses_default_retry_count(monkeypatch) -> None:
    pool_factory = MagicMock(return_value=object())
    monkeypatch.setattr(
        "ray.klein.integrations.redis.redis_connection_config.redis.BlockingConnectionPool",
        pool_factory,
    )
    config = RedisConnectionConfig("localhost", max_retries=3)

    config.create_pool()

    assert pool_factory.call_args.kwargs["retry"].get_retries() == 3


def test_lookup_client_and_value_lookup_validate_configuration() -> None:
    connection = RedisConnectionConfig("localhost")

    def key(row):
        return row["key"]

    with pytest.raises(TypeError, match="connection must be"):
        _RedisLookupClient(object(), key, key_prefix=None, delimiter=":", runtime_context=None)
    with pytest.raises(TypeError, match="key must be callable"):
        _RedisLookupClient(connection, None, key_prefix=None, delimiter=":", runtime_context=None)
    with pytest.raises(ValueError, match="key_prefix must be"):
        _RedisLookupClient(connection, key, key_prefix=" ", delimiter=":", runtime_context=None)
    with pytest.raises(ValueError, match="delimiter must be non-empty"):
        _RedisLookupClient(connection, key, key_prefix=None, delimiter="", runtime_context=None)
    with pytest.raises(TypeError, match="data_type must be"):
        RedisValueLookup(connection, key, data_type="string")
    with pytest.raises(ValueError, match="hash_fields can only"):
        RedisValueLookup(connection, key, hash_fields=["name"])
    with pytest.raises(ValueError, match="result_field must be non-empty"):
        RedisValueLookup(connection, key, result_field=" ")


class _BrokenIterable(Iterable):
    def __iter__(self):
        raise TypeError("cannot iterate")


def test_lookup_client_formats_keys_and_rejects_invalid_batches(monkeypatch) -> None:
    pool = _pool(monkeypatch)
    lookup = _RedisLookupClient(
        RedisConnectionConfig("localhost"),
        lambda row: row["keys"],
        key_prefix="user",
        delimiter="|",
        runtime_context=None,
    )

    assert lookup.single_key({"keys": memoryview(b"Ada")}) == "user|Ada"
    assert lookup.batch_keys({"keys": (value for value in (1, 2))}) == ["user|1", "user|2"]
    assert not lookup._is_key_collection("Ada")
    assert not lookup._is_key_collection({"id": 1})
    assert not lookup._is_key_collection(1)
    assert lookup._is_key_collection(["Ada"])
    with pytest.raises(TypeError, match="non-empty iterable"):
        lookup.batch_keys({"keys": 1})
    with pytest.raises(TypeError, match="non-empty iterable"):
        lookup.batch_keys({"keys": _BrokenIterable()})
    with pytest.raises(ValueError, match="empty sequence"):
        lookup.batch_keys({"keys": []})

    lookup.record_failure()
    lookup.close()
    lookup.close()
    pool.disconnect.assert_called_once_with()


def test_lookup_client_owns_client_lifecycle_and_metrics(monkeypatch) -> None:
    pool = _pool(monkeypatch)
    redis_client = object()
    factory = MagicMock(return_value=redis_client)
    monkeypatch.setattr("ray.klein.integrations.redis._lookup_client.redis.Redis", factory)
    context, duration, batch_size, failures = _runtime_context(batch=True)
    lookup = _RedisLookupClient(
        RedisConnectionConfig("localhost"),
        lambda row: row["key"],
        key_prefix=None,
        delimiter=":",
        runtime_context=context,
    )

    assert lookup.client() is redis_client
    factory.assert_called_once_with(connection_pool=pool)
    lookup.record_success(10.0)
    lookup.record_success(11.0, batch_size=3)
    lookup.record_failure()

    assert duration.observe_elapsed.call_args_list == [call(10.0), call(11.0)]
    batch_size.observe.assert_called_once_with(3)
    failures.inc.assert_called_once_with()
    assert context.metric_group.builtin_histogram.call_args_list == [
        call(KleinMetrics.REDIS_LOOKUP_DURATION_MS),
        call(KleinMetrics.REDIS_LOOKUP_BATCH_RECORDS),
    ]
    context.metric_group.builtin_counter.assert_called_once_with(KleinMetrics.REDIS_FAILURES)

    lookup.close()
    with pytest.raises(RuntimeError, match="Redis lookup is closed"):
        lookup.client()


@pytest.mark.parametrize(
    ("data_type", "hash_fields", "method", "response", "expected", "expected_args"),
    [
        (RedisDataType.STRING, None, "get", b"Ada", "Ada", ("cache|user",)),
        (RedisDataType.STRING, None, "get", None, None, ("cache|user",)),
        (
            RedisDataType.HASH,
            None,
            "hgetall",
            {b"name": memoryview(b"Ada"), b"city": bytearray(b"Paris")},
            {"name": "Ada", "city": "Paris"},
            ("cache|user",),
        ),
        (
            RedisDataType.HASH,
            ["name", "missing"],
            "hmget",
            [b"Ada", None],
            {"name": "Ada", "missing": None},
            ("cache|user", ("name", "missing")),
        ),
        (
            RedisDataType.LIST,
            None,
            "lrange",
            [b"Ada", bytearray(b"Grace")],
            ["Ada", "Grace"],
            ("cache|user", 0, -1),
        ),
        (
            RedisDataType.SET,
            None,
            "smembers",
            {b"Grace", b"Ada"},
            ["Ada", "Grace"],
            ("cache|user",),
        ),
    ],
)
def test_single_value_lookup_fetches_and_decodes_every_data_type(
    monkeypatch,
    data_type,
    hash_fields,
    method,
    response,
    expected,
    expected_args,
) -> None:
    pool = _pool(monkeypatch)
    client, _pipeline, factory = _install_client(monkeypatch)
    getattr(client, method).return_value = response
    lookup = RedisValueLookup(
        RedisConnectionConfig("localhost"),
        lambda row: row["key"],
        data_type=data_type,
        key_prefix="cache",
        delimiter="|",
        hash_fields=hash_fields,
        result_field="cached",
    )
    record = {"key": "user", "source": "input"}

    result = lookup(record)

    assert result == {"key": "user", "source": "input", "cached": expected}
    assert record == {"key": "user", "source": "input"}
    getattr(client, method).assert_called_once_with(*expected_args)
    factory.assert_called_once_with(connection_pool=pool)
    lookup.close()


@pytest.mark.parametrize(
    ("data_type", "hash_fields", "method", "responses", "expected", "expected_calls"),
    [
        (
            RedisDataType.STRING,
            None,
            "get",
            [b"Ada", None],
            ["Ada", None],
            [call("cache:a"), call("cache:b")],
        ),
        (
            RedisDataType.HASH,
            ["name", "missing"],
            "hmget",
            [[b"Ada", None], [b"Grace", b"present"]],
            [{"name": "Ada", "missing": None}, {"name": "Grace", "missing": "present"}],
            [call("cache:a", ("name", "missing")), call("cache:b", ("name", "missing"))],
        ),
        (
            RedisDataType.HASH,
            None,
            "hgetall",
            [{b"name": b"Ada"}, {b"name": b"Grace"}],
            [{"name": "Ada"}, {"name": "Grace"}],
            [call("cache:a"), call("cache:b")],
        ),
        (
            RedisDataType.LIST,
            None,
            "lrange",
            [[b"a1", b"a2"], [b"b1"]],
            [["a1", "a2"], ["b1"]],
            [call("cache:a", 0, -1), call("cache:b", 0, -1)],
        ),
        (
            RedisDataType.SET,
            None,
            "smembers",
            [{b"a2", b"a1"}, {b"b1"}],
            [["a1", "a2"], ["b1"]],
            [call("cache:a"), call("cache:b")],
        ),
    ],
)
def test_batch_value_lookup_pipelines_and_decodes_every_data_type(
    monkeypatch,
    data_type,
    hash_fields,
    method,
    responses,
    expected,
    expected_calls,
) -> None:
    _pool(monkeypatch)
    client, pipeline, _factory = _install_client(monkeypatch)
    pipeline.execute.return_value = responses
    context, duration, batch_size, failures = _runtime_context(batch=True)
    lookup = RedisValueLookup(
        RedisConnectionConfig("localhost"),
        lambda batch: batch["key"],
        data_type=data_type,
        key_prefix="cache",
        hash_fields=hash_fields,
        runtime_context=context,
    )
    record = {"key": ["a", "b"], "source": [1, 2]}

    result = lookup(record)

    assert result == {"key": ["a", "b"], "source": [1, 2], "redis_value": expected}
    client.pipeline.assert_called_once_with(transaction=False)
    assert getattr(pipeline, method).call_args_list == expected_calls
    pipeline.execute.assert_called_once_with()
    duration.observe_elapsed.assert_called_once()
    batch_size.observe.assert_called_once_with(2)
    failures.inc.assert_not_called()
    lookup.close()


def test_batch_value_lookup_honors_default_batch_format(monkeypatch) -> None:
    _pool(monkeypatch)
    _client, pipeline, _factory = _install_client(monkeypatch)
    pipeline.execute.return_value = [b"Ada", None]
    runtime_context = SimpleNamespace(
        runtime_info=RuntimeInfo(batch_size=2, batch_timeout=1, batch_format="default"),
        metric_group=None,
    )
    lookup = RedisValueLookup(
        RedisConnectionConfig("localhost"),
        lambda batch: batch["key"],
        runtime_context=runtime_context,
    )

    result = lookup({"key": numpy.array(["a", "b"])})

    assert isinstance(result["redis_value"], numpy.ndarray)
    assert result["redis_value"].tolist() == ["Ada", None]


def test_single_value_lookup_records_redis_failure_and_propagates(monkeypatch) -> None:
    _pool(monkeypatch)
    client, _pipeline, _factory = _install_client(monkeypatch)
    failure = RedisError("lookup unavailable")
    client.get.side_effect = failure
    context, duration, batch_size, failures = _runtime_context(batch=False)
    logged = MagicMock()
    monkeypatch.setattr("ray.klein.integrations.redis.redis_value_lookup.logger.exception", logged)
    lookup = RedisValueLookup(
        RedisConnectionConfig("localhost"),
        lambda row: row["key"],
        runtime_context=context,
    )

    with pytest.raises(RedisError) as captured:
        lookup({"key": "a"})

    assert captured.value is failure
    failures.inc.assert_called_once_with()
    duration.observe_elapsed.assert_not_called()
    batch_size.observe.assert_not_called()
    logged.assert_called_once_with("Redis value lookup failed for key %r", "a")


def test_batch_value_lookup_records_os_failure_and_propagates(monkeypatch) -> None:
    _pool(monkeypatch)
    _client, pipeline, _factory = _install_client(monkeypatch)
    failure = OSError("connection reset")
    pipeline.execute.side_effect = failure
    context, duration, batch_size, failures = _runtime_context(batch=True)
    logged = MagicMock()
    monkeypatch.setattr("ray.klein.integrations.redis.redis_value_lookup.logger.exception", logged)
    lookup = RedisValueLookup(
        RedisConnectionConfig("localhost"),
        lambda batch: batch["key"],
        runtime_context=context,
    )

    with pytest.raises(OSError) as captured:
        lookup({"key": ["a", "b"]})

    assert captured.value is failure
    failures.inc.assert_called_once_with()
    duration.observe_elapsed.assert_not_called()
    batch_size.observe.assert_not_called()
    logged.assert_called_once_with("Redis value lookup failed for %d keys", 2)


def test_value_lookup_propagates_invalid_redis_response_type(monkeypatch) -> None:
    _pool(monkeypatch)
    client, _pipeline, _factory = _install_client(monkeypatch)
    client.get.return_value = 42
    context, duration, _batch_size, failures = _runtime_context(batch=False)
    lookup = RedisValueLookup(
        RedisConnectionConfig("localhost"),
        lambda row: row["key"],
        runtime_context=context,
    )

    with pytest.raises(TypeError, match="Expected Redis text response, got int"):
        lookup({"key": "a"})

    duration.observe_elapsed.assert_called_once()
    failures.inc.assert_not_called()


def test_value_lookup_decode_validation_and_defensive_data_type_errors(monkeypatch) -> None:
    _pool(monkeypatch)
    lookup = RedisValueLookup(RedisConnectionConfig("localhost"), lambda row: row["key"])
    assert lookup._decode(bytearray(b"Ada")) == "Ada"
    assert lookup._decode(memoryview(b"Grace")) == "Grace"
    with pytest.raises(UnicodeDecodeError):
        lookup._decode(b"\xff")

    selected_hash = RedisValueLookup(
        RedisConnectionConfig("localhost"),
        lambda row: row["key"],
        data_type=RedisDataType.HASH,
        hash_fields=["name", "city"],
    )
    with pytest.raises(ValueError, match="zip"):
        selected_hash._decode([b"Ada"])

    lookup._data_type = object()
    with pytest.raises(ValueError, match="Unsupported Redis data type"):
        lookup._fetch(MagicMock(), "key")
    with pytest.raises(ValueError, match="Unsupported Redis data type"):
        lookup._decode(b"value")


def test_missing_key_filter_keeps_only_absent_single_keys(monkeypatch) -> None:
    pool = _pool(monkeypatch)
    client, _pipeline, factory = _install_client(monkeypatch)
    client.exists.side_effect = [0, 2]
    context, duration, batch_size, failures = _runtime_context(batch=False)
    missing = RedisMissingKeyFilter(
        RedisConnectionConfig("localhost"),
        lambda row: row["key"],
        key_prefix="processed",
        runtime_context=context,
    )

    assert missing({"key": "a"}) is True
    assert missing({"key": "b"}) is False

    assert client.exists.call_args_list == [call("processed:a"), call("processed:b")]
    assert duration.observe_elapsed.call_count == 2
    batch_size.observe.assert_not_called()
    failures.inc.assert_not_called()
    assert factory.call_args_list == [call(connection_pool=pool), call(connection_pool=pool)]
    missing.close()


def test_missing_key_filter_pipelines_batch_existence_checks(monkeypatch) -> None:
    _pool(monkeypatch)
    client, pipeline, _factory = _install_client(monkeypatch)
    pipeline.execute.return_value = [0, 1, ""]
    context, duration, batch_size, failures = _runtime_context(batch=True)
    missing = RedisMissingKeyFilter(
        RedisConnectionConfig("localhost"),
        lambda batch: batch["key"],
        runtime_context=context,
    )

    assert missing({"key": numpy.array(["a", "b", "c"])}) == [True, False, True]

    client.pipeline.assert_called_once_with(transaction=False)
    assert pipeline.exists.call_args_list == [call("a"), call("b"), call("c")]
    pipeline.execute.assert_called_once_with()
    duration.observe_elapsed.assert_called_once()
    batch_size.observe.assert_called_once_with(3)
    failures.inc.assert_not_called()


def test_single_missing_key_filter_records_redis_failure(monkeypatch) -> None:
    _pool(monkeypatch)
    client, _pipeline, _factory = _install_client(monkeypatch)
    failure = RedisError("lookup unavailable")
    client.exists.side_effect = failure
    context, duration, _batch_size, failures = _runtime_context(batch=False)
    logged = MagicMock()
    monkeypatch.setattr("ray.klein.integrations.redis.redis_missing_key_filter.logger.exception", logged)
    missing = RedisMissingKeyFilter(
        RedisConnectionConfig("localhost"),
        lambda row: row["key"],
        runtime_context=context,
    )

    with pytest.raises(RedisError) as captured:
        missing({"key": "a"})

    assert captured.value is failure
    failures.inc.assert_called_once_with()
    duration.observe_elapsed.assert_not_called()
    logged.assert_called_once_with("Redis existence lookup failed for key %r", "a")


def test_batch_missing_key_filter_records_os_failure(monkeypatch) -> None:
    _pool(monkeypatch)
    _client, pipeline, _factory = _install_client(monkeypatch)
    failure = OSError("connection reset")
    pipeline.execute.side_effect = failure
    context, duration, batch_size, failures = _runtime_context(batch=True)
    logged = MagicMock()
    monkeypatch.setattr("ray.klein.integrations.redis.redis_missing_key_filter.logger.exception", logged)
    missing = RedisMissingKeyFilter(
        RedisConnectionConfig("localhost"),
        lambda batch: batch["key"],
        runtime_context=context,
    )

    with pytest.raises(OSError) as captured:
        missing({"key": ["a", "b"]})

    assert captured.value is failure
    failures.inc.assert_called_once_with()
    duration.observe_elapsed.assert_not_called()
    batch_size.observe.assert_not_called()
    logged.assert_called_once_with("Redis existence lookup failed for %d keys", 2)
