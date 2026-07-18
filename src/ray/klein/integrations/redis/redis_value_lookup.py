# SPDX-License-Identifier: Apache-2.0
"""Redis value-enrichment transform."""

from __future__ import annotations

import time
from collections.abc import Sequence
from typing import Any

import redis
from redis import RedisError
from redis.client import Pipeline

from ray.klein._internal.block import wrapper_batch_data
from ray.klein._internal.logging import get_logger
from ray.klein.api.runtime_context import RuntimeContext
from ray.klein.integrations.redis._lookup_client import LookupKeyExtractor, Record, _RedisLookupClient
from ray.klein.integrations.redis.data_type import RedisDataType
from ray.klein.integrations.redis.redis_connection_config import RedisConnectionConfig

logger = get_logger(__name__)


class RedisValueLookup:
    """Enrich each record with a value fetched from Redis."""

    def __init__(
        self,
        connection: RedisConnectionConfig,
        key: LookupKeyExtractor,
        *,
        data_type: RedisDataType = RedisDataType.STRING,
        key_prefix: str | None = None,
        delimiter: str = ":",
        hash_fields: Sequence[str] | None = None,
        result_field: str = "redis_value",
        runtime_context: RuntimeContext | None = None,
    ) -> None:
        if not isinstance(data_type, RedisDataType):
            raise TypeError("data_type must be a RedisDataType")
        if hash_fields is not None and data_type is not RedisDataType.HASH:
            raise ValueError("hash_fields can only be used with RedisDataType.HASH")
        if not result_field.strip():
            raise ValueError("result_field must be non-empty")

        self._lookup = _RedisLookupClient(
            connection,
            key,
            key_prefix=key_prefix,
            delimiter=delimiter,
            runtime_context=runtime_context,
        )
        self._data_type = data_type
        self._hash_fields = tuple(hash_fields) if hash_fields else None
        self._result_field = result_field

    def __call__(self, record: Record) -> Record:
        keys = self._lookup.resolve_keys(record)
        if isinstance(keys, list):
            return self._fetch_batch(record, keys)
        return self._fetch_one(record, keys)

    def _fetch_one(self, record: Record, key: str) -> Record:
        started_at = time.monotonic()
        try:
            with self._lookup.client() as client:
                response = self._fetch(client, key)
        except (RedisError, OSError):
            self._lookup.record_failure()
            logger.exception("Redis value lookup failed for key %r", key)
            raise
        self._lookup.record_success(started_at)
        return self._with_result(record, self._decode(response))

    def _fetch_batch(self, record: Record, keys: list[str]) -> Record:
        started_at = time.monotonic()
        try:
            with (
                self._lookup.client() as client,
                client.pipeline(transaction=False) as pipeline,
            ):
                for key in keys:
                    self._queue_fetch(pipeline, key)
                responses = pipeline.execute()
        except (RedisError, OSError):
            self._lookup.record_failure()
            logger.exception("Redis value lookup failed for %d keys", len(keys))
            raise
        self._lookup.record_success(started_at, len(keys))
        values = wrapper_batch_data(
            [self._decode(response) for response in responses],
            self._lookup.runtime_info.batch_format,
        )
        return self._with_result(record, values)

    def _fetch(self, client: redis.Redis, key: str) -> Any:
        if self._data_type is RedisDataType.STRING:
            return client.get(key)
        if self._data_type is RedisDataType.HASH:
            return client.hmget(key, self._hash_fields) if self._hash_fields else client.hgetall(key)
        if self._data_type is RedisDataType.LIST:
            return client.lrange(key, 0, -1)
        if self._data_type is RedisDataType.SET:
            return client.smembers(key)
        raise ValueError(f"Unsupported Redis data type: {self._data_type}")

    def _queue_fetch(self, pipeline: Pipeline, key: str) -> None:
        if self._data_type is RedisDataType.STRING:
            pipeline.get(key)
        elif self._data_type is RedisDataType.HASH:
            if self._hash_fields:
                pipeline.hmget(key, self._hash_fields)
            else:
                pipeline.hgetall(key)
        elif self._data_type is RedisDataType.LIST:
            pipeline.lrange(key, 0, -1)
        elif self._data_type is RedisDataType.SET:
            pipeline.smembers(key)
        else:  # pragma: no cover - guarded by constructor
            raise ValueError(f"Unsupported Redis data type: {self._data_type}")

    def _decode(self, value: Any) -> Any:
        if value is None:
            return None
        if self._data_type is RedisDataType.STRING:
            return _decode_text(value)
        if self._data_type is RedisDataType.HASH:
            return self._decode_hash(value)
        if self._data_type is RedisDataType.LIST:
            return [_decode_text(item) for item in value]
        if self._data_type is RedisDataType.SET:
            # Redis sets are unordered, while Ray Data normalizes Python sets
            # to list-valued Arrow columns. Return a stable list in both
            # backends so batch and streaming have the same public schema.
            return sorted(_decode_text(item) for item in value)
        raise ValueError(f"Unsupported Redis data type: {self._data_type}")

    def _decode_hash(self, value: Any) -> dict[str, str | None]:
        if self._hash_fields:
            return {
                field: None if field_value is None else _decode_text(field_value)
                for field, field_value in zip(self._hash_fields, value, strict=True)
            }
        return {_decode_text(field): _decode_text(field_value) for field, field_value in value.items()}

    def _with_result(self, record: Record, value: Any) -> Record:
        return {**record, self._result_field: value}

    def close(self) -> None:
        self._lookup.close()


def _decode_text(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).decode("utf-8")
    raise TypeError(f"Expected Redis text response, got {type(value).__name__}")
