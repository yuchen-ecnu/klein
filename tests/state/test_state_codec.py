# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import hashlib

import pytest

from ray.klein.state.state_codec import (
    decode_expiry_key,
    decode_state_key,
    decode_state_namespace,
    decode_state_value,
    decode_timer,
    encode_expiry_key,
    encode_state_key,
    encode_state_value,
    encode_timer_key,
    encode_timer_value,
    state_key_prefix,
    timer_prefix,
)
from ray.klein.state.timer_domain import TimerDomain
from ray.klein.state.value_state_descriptor import ValueStateDescriptor


@pytest.mark.parametrize(
    ("logical_key", "namespace"),
    [
        ("customer/一", None),
        (b"\x00\x04binary", (0, 2**32)),
        (("tenant", 7), {"window": [1, 2]}),
    ],
)
def test_state_key_round_trip_preserves_typed_components(logical_key, namespace) -> None:
    descriptor = ValueStateDescriptor("value/状态")

    encoded = encode_state_key(descriptor, logical_key, namespace)

    assert decode_state_key(encoded) == (descriptor.name, logical_key, namespace)
    assert decode_state_namespace(encoded) == namespace
    assert encoded.startswith(state_key_prefix(descriptor, logical_key))


@pytest.mark.parametrize("suffix", [b"\x00", b"trailing"])
def test_state_key_decoder_rejects_trailing_bytes(suffix: bytes) -> None:
    descriptor = ValueStateDescriptor("value")
    encoded = encode_state_key(descriptor, "key", "namespace")

    with pytest.raises(ValueError, match="trailing bytes"):
        decode_state_key(encoded + suffix)
    with pytest.raises(ValueError, match="trailing bytes"):
        decode_state_namespace(encoded + suffix)


@pytest.mark.parametrize("encoded", [b"", b"\x00\x00\x00", b"\x00\x00\x00\x05abc"])
def test_state_key_decoder_rejects_truncated_components(encoded: bytes) -> None:
    with pytest.raises(ValueError, match="truncated"):
        decode_state_key(encoded)


@pytest.mark.parametrize("expiry", [None, 0, 1, 2**63 - 1])
def test_state_value_round_trip_preserves_expiry_and_binary_payload(expiry: int | None) -> None:
    payload = b"\x00\xffpayload\x00"

    assert decode_state_value(encode_state_value(payload, expiry)) == (expiry, payload)


def test_state_value_decoder_rejects_truncated_header() -> None:
    with pytest.raises(ValueError, match="truncated"):
        decode_state_value(b"1234567")


def test_expiry_keys_are_order_preserving_and_lossless() -> None:
    keys = [encode_expiry_key(timestamp, b"state-key") for timestamp in (10, 1, 2**63)]

    assert sorted(keys) == [keys[1], keys[0], keys[2]]
    assert [decode_expiry_key(encoded) for encoded in keys] == [
        (10, b"state-key"),
        (1, b"state-key"),
        (2**63, b"state-key"),
    ]
    with pytest.raises(ValueError, match="truncated"):
        decode_expiry_key(b"short")


@pytest.mark.parametrize("domain", list(TimerDomain))
def test_timer_encoding_is_deterministic_and_domain_scoped(domain: TimerDomain) -> None:
    key = ("customer", 7)
    namespace = {"window": (100, 200)}
    encoded_value = encode_timer_value(key, namespace)

    first = encode_timer_key(123, key, namespace, domain)
    second = encode_timer_key(123, key, namespace, domain)

    assert first == second
    assert first.startswith(timer_prefix(domain))
    assert first[-32:] == hashlib.sha256(encoded_value).digest()
    assert decode_timer(first, encoded_value) == (123, key, namespace)


def test_timer_domains_produce_distinct_keys_and_truncation_is_rejected() -> None:
    event = encode_timer_key(1, "key", None, TimerDomain.EVENT_TIME)
    processing = encode_timer_key(1, "key", None, TimerDomain.PROCESSING_TIME)

    assert event != processing
    with pytest.raises(ValueError, match="truncated"):
        decode_timer(event[:8], encode_timer_value("key", None))
