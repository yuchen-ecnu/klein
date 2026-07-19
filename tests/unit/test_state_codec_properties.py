# SPDX-License-Identifier: Apache-2.0
"""Property tests for durable managed-state binary encodings."""

from __future__ import annotations

from hypothesis import given
from hypothesis import strategies as st

from ray.klein.state.state_codec import (
    decode_expiry_key,
    decode_state_key,
    decode_state_value,
    decode_timer,
    encode_expiry_key,
    encode_state_key,
    encode_state_value,
    encode_timer_key,
    encode_timer_value,
)
from ray.klein.state.state_descriptor import StateDescriptor
from ray.klein.state.timer_domain import TimerDomain

STATE_VALUES = st.one_of(
    st.none(),
    st.booleans(),
    st.integers(),
    st.binary(max_size=128),
    st.text(max_size=128),
    st.tuples(st.integers(), st.text(max_size=32)),
)
DESCRIPTOR_NAMES = st.text(
    alphabet=st.characters(whitelist_categories=("L", "N")),
    min_size=1,
    max_size=32,
)
TIMESTAMPS = st.integers(min_value=0, max_value=(1 << 63) - 1)


@given(DESCRIPTOR_NAMES, STATE_VALUES, STATE_VALUES)
def test_state_key_encoding_round_trips(name: str, key: object, namespace: object) -> None:
    descriptor = StateDescriptor(name)

    assert decode_state_key(encode_state_key(descriptor, key, namespace)) == (name, key, namespace)


@given(st.binary(max_size=512), st.one_of(st.none(), TIMESTAMPS))
def test_state_value_encoding_round_trips(payload: bytes, expires_at_ms: int | None) -> None:
    assert decode_state_value(encode_state_value(payload, expires_at_ms)) == (expires_at_ms, payload)


@given(TIMESTAMPS, st.binary(max_size=256))
def test_expiry_key_encoding_round_trips(expires_at_ms: int, state_key: bytes) -> None:
    assert decode_expiry_key(encode_expiry_key(expires_at_ms, state_key)) == (expires_at_ms, state_key)


@given(TIMESTAMPS, STATE_VALUES, STATE_VALUES, st.sampled_from(list(TimerDomain)))
def test_timer_encoding_round_trips(
    timestamp: int,
    key: object,
    namespace: object,
    domain: TimerDomain,
) -> None:
    encoded_key = encode_timer_key(timestamp, key, namespace, domain)
    encoded_value = encode_timer_value(key, namespace)

    assert decode_timer(encoded_key, encoded_value) == (timestamp, key, namespace)
