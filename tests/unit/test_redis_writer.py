# SPDX-License-Identifier: Apache-2.0
"""Unit tests for Redis command generation."""

from datetime import timedelta
from unittest.mock import MagicMock

import pytest

from ray.klein.integrations.redis import RedisDataType
from ray.klein.integrations.redis.writer import RedisWriter


def _writer(data_type: RedisDataType, *, ttl: timedelta | None = None) -> RedisWriter:
    return RedisWriter(data_type, lambda row: row["key"], lambda row: row["value"], ttl=ttl)


def test_list_replacement_preserves_input_order() -> None:
    pipeline = MagicMock()

    _writer(RedisDataType.LIST).queue(
        pipeline,
        {"key": "names", "value": ["Tom", "Lucy"]},
    )

    pipeline.delete.assert_called_once_with("names")
    pipeline.rpush.assert_called_once_with("names", "Tom", "Lucy")
    pipeline.lpush.assert_not_called()


def test_empty_collection_deletes_the_existing_value() -> None:
    pipeline = MagicMock()

    _writer(RedisDataType.SET).queue(pipeline, {"key": "names", "value": []})

    pipeline.delete.assert_called_once_with("names")
    pipeline.sadd.assert_not_called()
    pipeline.expire.assert_not_called()


def test_complex_value_replacement_applies_ttl() -> None:
    pipeline = MagicMock()
    ttl = timedelta(minutes=5)

    _writer(RedisDataType.HASH, ttl=ttl).queue(
        pipeline,
        {"key": "person", "value": {"age": 23}},
    )

    pipeline.delete.assert_called_once_with("person")
    pipeline.hset.assert_called_once_with("person", mapping={"age": 23})
    pipeline.expire.assert_called_once_with("person", ttl)


@pytest.mark.parametrize("data_type", [RedisDataType.LIST, RedisDataType.SET])
def test_collection_writers_reject_strings(data_type) -> None:
    with pytest.raises(TypeError, match="non-string iterable"):
        _writer(data_type).queue(MagicMock(), {"key": "names", "value": "Tom"})


def test_string_writer_rejects_boolean_values() -> None:
    with pytest.raises(TypeError, match="Redis-encodable"):
        _writer(RedisDataType.STRING).queue(MagicMock(), {"key": "enabled", "value": True})
