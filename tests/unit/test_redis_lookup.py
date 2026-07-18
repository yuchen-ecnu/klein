# SPDX-License-Identifier: Apache-2.0
"""Unit tests for Redis lookup decoding and ownership."""

from unittest.mock import MagicMock

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
