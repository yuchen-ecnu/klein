# SPDX-License-Identifier: Apache-2.0
"""Translate records into deterministic Redis replacement commands."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from redis.client import Pipeline

from ray.klein.integrations.redis.data_type import RedisDataType

Record = dict[str, Any]
KeyExtractor = Callable[[Record], Any]
ValueExtractor = Callable[[Record], Any]

_ENCODABLE_TYPES = (bytes, bytearray, memoryview, str, int, float)
_TEXT_TYPES = (str, bytes, bytearray, memoryview)


def format_redis_key(raw_key: Any, prefix: str | None, delimiter: str) -> str:
    """Normalize a user key and apply the configured namespace prefix."""

    if raw_key is None:
        raise ValueError("Redis key extractor returned None")
    if isinstance(raw_key, (bytes, bytearray, memoryview)):
        try:
            key = bytes(raw_key).decode("utf-8")
        except UnicodeDecodeError as error:
            raise ValueError("Redis keys must be valid UTF-8 when supplied as bytes") from error
    else:
        key = str(raw_key)
    if not key.strip():
        raise ValueError("Redis key extractor returned an empty key")
    return key if prefix is None else f"{prefix}{delimiter}{key}"


def _require_encodable(value: Any, label: str) -> Any:
    if isinstance(value, bool) or not isinstance(value, _ENCODABLE_TYPES):
        expected = "str, bytes, int, or float"
        raise TypeError(f"{label} must be Redis-encodable ({expected}); got {type(value).__name__}")
    return value


def _materialize_collection(value: Any, label: str) -> tuple[Any, ...]:
    if isinstance(value, (*_TEXT_TYPES, Mapping)) or not isinstance(value, Iterable):
        raise TypeError(f"Redis {label} values must be a non-string iterable")
    items = tuple(value)
    for index, item in enumerate(items):
        _require_encodable(item, f"Redis {label} item at index {index}")
    return items


@dataclass(frozen=True, slots=True)
class RedisWriter:
    """Queue one record as an idempotent replacement of one Redis key."""

    data_type: RedisDataType
    key: KeyExtractor
    value: ValueExtractor
    key_prefix: str | None = None
    delimiter: str = ":"
    ttl: timedelta | None = None

    def __post_init__(self) -> None:
        if not callable(self.key):
            raise TypeError("key must be callable")
        if not callable(self.value):
            raise TypeError("value must be callable")

    def queue(self, pipeline: Pipeline, record: Record) -> None:
        key = format_redis_key(self.key(record), self.key_prefix, self.delimiter)
        value = self.value(record)

        if self.data_type is RedisDataType.STRING:
            self._queue_string(pipeline, key, value)
        elif self.data_type is RedisDataType.HASH:
            self._queue_hash(pipeline, key, value)
        elif self.data_type is RedisDataType.LIST:
            self._queue_list(pipeline, key, value)
        elif self.data_type is RedisDataType.SET:
            self._queue_set(pipeline, key, value)
        else:  # pragma: no cover - guarded by RedisSinkConfig
            raise ValueError(f"Unsupported Redis data type: {self.data_type}")

    def _queue_string(self, pipeline: Pipeline, key: str, value: Any) -> None:
        value = _require_encodable(value, "Redis string value")
        if self.ttl is None:
            pipeline.set(key, value)
        else:
            pipeline.set(key, value, ex=self.ttl)

    def _queue_hash(self, pipeline: Pipeline, key: str, value: Any) -> None:
        if not isinstance(value, Mapping):
            raise TypeError(f"Redis hash values must be mappings; got {type(value).__name__}")
        mapping = dict(value)
        for field, field_value in mapping.items():
            _require_encodable(field, "Redis hash field")
            _require_encodable(field_value, f"Redis hash value for field {field!r}")
        pipeline.delete(key)
        if mapping:
            pipeline.hset(key, mapping=mapping)
            self._queue_expiry(pipeline, key)

    def _queue_list(self, pipeline: Pipeline, key: str, value: Any) -> None:
        items = _materialize_collection(value, "list")
        pipeline.delete(key)
        if items:
            pipeline.rpush(key, *items)
            self._queue_expiry(pipeline, key)

    def _queue_set(self, pipeline: Pipeline, key: str, value: Any) -> None:
        items = _materialize_collection(value, "set")
        pipeline.delete(key)
        if items:
            pipeline.sadd(key, *items)
            self._queue_expiry(pipeline, key)

    def _queue_expiry(self, pipeline: Pipeline, key: str) -> None:
        if self.ttl is not None:
            pipeline.expire(key, self.ttl)
