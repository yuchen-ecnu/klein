# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from datetime import timedelta
from typing import Any

import pytest

from ray.klein.api.klein_context import KleinContext
from ray.klein.integrations.redis import (
    RedisConnectionConfig,
    RedisDataType,
    RedisSinkConfig,
)

CASES = [
    (
        RedisDataType.STRING,
        [
            {"name": "Jack", "value": 23},
            {"name": "Lucy", "value": 18},
        ],
        {"Jack": b"23", "Lucy": b"18"},
    ),
    (
        RedisDataType.HASH,
        [
            {"name": "Jack", "value": {"age": 23, "gender": "M"}},
            {"name": "Lucy", "value": {"age": 18, "gender": "W"}},
        ],
        {
            "Jack": {b"age": b"23", b"gender": b"M"},
            "Lucy": {b"age": b"18", b"gender": b"W"},
        },
    ),
    (
        RedisDataType.LIST,
        [
            {"name": "Jack", "value": ["Tom", "Lucy"]},
            {"name": "Lucy", "value": ["Jack", "Tom"]},
        ],
        {"Jack": [b"Tom", b"Lucy"], "Lucy": [b"Jack", b"Tom"]},
    ),
    (
        RedisDataType.SET,
        [
            {"name": "Jack", "value": ["Tom", "Lucy"]},
            {"name": "Lucy", "value": ["Jack", "Tom"]},
        ],
        {"Jack": {b"Tom", b"Lucy"}, "Lucy": {b"Jack", b"Tom"}},
    ),
]


def _read_value(client, data_type: RedisDataType, key: str) -> Any:
    if data_type is RedisDataType.STRING:
        return client.get(key)
    if data_type is RedisDataType.HASH:
        return client.hgetall(key)
    if data_type is RedisDataType.LIST:
        return client.lrange(key, 0, -1)
    if data_type is RedisDataType.SET:
        return client.smembers(key)
    raise AssertionError(f"unhandled test data type: {data_type}")


@pytest.mark.parametrize(("data_type", "rows", "expected"), CASES)
def test_write_redis_round_trip(clean_redis, data_type, rows, expected) -> None:
    context = KleinContext()
    prefix = f"test-{data_type.value}"
    connection = RedisConnectionConfig(clean_redis.host, port=clean_redis.port)
    config = RedisSinkConfig(
        data_type=data_type,
        ttl=timedelta(hours=1),
        key_prefix=prefix,
    )

    sink = context.from_values(*rows).write_redis(
        connection,
        key=lambda row: row["name"],
        value=lambda row: row["value"],
        config=config,
        num_cpus=0.1,
        concurrency=2,
    )

    context.execute(f"write-{data_type.value}", sinks=(sink,)).wait()

    actual = {name: _read_value(clean_redis.client, data_type, f"{prefix}:{name}") for name in expected}
    assert actual == expected
