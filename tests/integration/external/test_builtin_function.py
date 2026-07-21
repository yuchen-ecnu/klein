# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from typing import Any

import pytest

from ray.klein.api.klein_context import KleinContext
from ray.klein.integrations.redis import (
    RedisConnectionConfig,
    RedisDataType,
    RedisMissingKeyFilter,
    RedisValueLookup,
)
from tests.support.assertions import assert_rows_equal
from tests.support.terminal import execute_terminal

ROWS = [
    {"id": "known", "name": "Jack"},
    {"id": "new", "name": "Lucy"},
]


@pytest.fixture()
def seeded_redis(clean_redis):
    client = clean_redis.client
    client.set("seen:known", "1")
    client.set("value:Jack", "23")
    client.set("value:Lucy", "18")
    client.hset("hash:Jack", mapping={"age": "23", "gender": "M"})
    client.hset("hash:Lucy", mapping={"age": "18", "gender": "W"})
    client.rpush("list:Jack", "Tom", "Lucy")
    client.rpush("list:Lucy", "Jack", "Tom")
    client.sadd("set:Jack", "Tom", "Lucy")
    client.sadd("set:Lucy", "Jack", "Tom")
    return clean_redis


@pytest.mark.parametrize("batch_size", [None, 2])
def test_redis_filter_handles_single_rows_and_batches(seeded_redis, batch_size) -> None:
    context = KleinContext()
    connection = RedisConnectionConfig(seeded_redis.host, port=seeded_redis.port)

    sink = (
        context.data.from_items(ROWS)
        .filter(
            RedisMissingKeyFilter,
            fn_constructor_args=[connection, lambda row: row["id"]],
            fn_constructor_kwargs={"key_prefix": "seen"},
            batch_size=batch_size,
        )
        .take_all()
    )
    actual = execute_terminal(sink, job_name=f"redis-filter-{batch_size}")

    assert actual == [{"id": "new", "name": "Lucy"}]


@pytest.mark.parametrize("batch_size", [None, 2])
@pytest.mark.parametrize(
    ("data_type", "key_prefix", "expected"),
    [
        (RedisDataType.STRING, "value", ["23", "18"]),
        (
            RedisDataType.HASH,
            "hash",
            [{"age": "23", "gender": "M"}, {"age": "18", "gender": "W"}],
        ),
        (RedisDataType.LIST, "list", [["Tom", "Lucy"], ["Jack", "Tom"]]),
        (RedisDataType.SET, "set", [["Lucy", "Tom"], ["Jack", "Tom"]]),
    ],
)
def test_redis_lookup_handles_single_rows_and_batches(
    seeded_redis,
    batch_size,
    data_type: RedisDataType,
    key_prefix: str,
    expected: list[Any],
) -> None:
    context = KleinContext()
    connection = RedisConnectionConfig(seeded_redis.host, port=seeded_redis.port)

    sink = (
        context.data.from_items(ROWS)
        .map(
            RedisValueLookup,
            fn_constructor_args=[connection, lambda row: row["name"]],
            fn_constructor_kwargs={
                "data_type": data_type,
                "key_prefix": key_prefix,
            },
            batch_size=batch_size,
        )
        .take_all()
    )
    actual = execute_terminal(sink, job_name=f"redis-lookup-{data_type.value}-{batch_size}")

    expected_rows = [{**row, "redis_value": value} for row, value in zip(ROWS, expected, strict=True)]
    assert_rows_equal(actual, expected_rows)
