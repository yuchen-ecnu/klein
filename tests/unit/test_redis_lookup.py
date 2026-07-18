# SPDX-License-Identifier: Apache-2.0
"""Unit tests for Redis lookup decoding and ownership."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import numpy

from ray.klein.api.runtime_info import RuntimeInfo
from ray.klein.integrations.redis import RedisConnectionConfig, RedisDataType, RedisValueLookup


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
