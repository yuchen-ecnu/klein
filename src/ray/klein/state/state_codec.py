# SPDX-License-Identifier: Apache-2.0
import hashlib
import struct
from typing import Any, cast

from ray.klein.state.pickle_state_serializer import PickleStateSerializer
from ray.klein.state.state_descriptor import StateDescriptor
from ray.klein.state.timer_domain import TimerDomain

_KEY_SERIALIZER: PickleStateSerializer[Any] = PickleStateSerializer()
_NO_EXPIRY = -1


def encode_state_key(descriptor: StateDescriptor, key: Any, namespace: Any) -> bytes:
    return b"".join(
        (
            _component(descriptor.name.encode("utf-8")),
            _component(_KEY_SERIALIZER.dumps(key)),
            _component(_KEY_SERIALIZER.dumps(namespace)),
        )
    )


def state_key_prefix(descriptor: StateDescriptor, key: Any) -> bytes:
    return _component(descriptor.name.encode("utf-8")) + _component(_KEY_SERIALIZER.dumps(key))


def decode_state_namespace(encoded: bytes) -> Any:
    _name, offset = _read_component(encoded, 0)
    _key, offset = _read_component(encoded, offset)
    namespace, offset = _read_component(encoded, offset)
    if offset != len(encoded):
        raise ValueError("managed state key contains trailing bytes")
    return _KEY_SERIALIZER.loads(namespace)


def decode_state_key(encoded: bytes) -> tuple[str, Any, Any]:
    """Decode a physical managed-state key for key-group checkpointing."""

    name, offset = _read_component(encoded, 0)
    key, offset = _read_component(encoded, offset)
    namespace, offset = _read_component(encoded, offset)
    if offset != len(encoded):
        raise ValueError("managed state key contains trailing bytes")
    return (
        name.decode("utf-8"),
        _KEY_SERIALIZER.loads(key),
        _KEY_SERIALIZER.loads(namespace),
    )


def encode_state_value(payload: bytes, expires_at_ms: int | None) -> bytes:
    expiry = _NO_EXPIRY if expires_at_ms is None else expires_at_ms
    return struct.pack(">q", expiry) + payload


def decode_state_value(encoded: bytes) -> tuple[int | None, bytes]:
    if len(encoded) < 8:
        raise ValueError("managed state value is truncated")
    expiry = struct.unpack(">q", encoded[:8])[0]
    return (None if expiry == _NO_EXPIRY else expiry), encoded[8:]


def encode_expiry_key(expires_at_ms: int, state_key: bytes) -> bytes:
    return struct.pack(">Q", expires_at_ms) + state_key


def decode_expiry_key(encoded: bytes) -> tuple[int, bytes]:
    if len(encoded) < 8:
        raise ValueError("state expiry key is truncated")
    return struct.unpack(">Q", encoded[:8])[0], encoded[8:]


def encode_timer_key(timestamp: int, key: Any, namespace: Any, domain: TimerDomain) -> bytes:
    payload = _KEY_SERIALIZER.dumps((key, namespace))
    domain_prefix = b"e" if domain == TimerDomain.EVENT_TIME else b"p"
    return domain_prefix + struct.pack(">Q", timestamp) + hashlib.sha256(payload).digest()


def timer_prefix(domain: TimerDomain) -> bytes:
    return b"e" if domain == TimerDomain.EVENT_TIME else b"p"


def encode_timer_value(key: Any, namespace: Any) -> bytes:
    return cast(bytes, _KEY_SERIALIZER.dumps((key, namespace)))


def decode_timer(encoded_key: bytes, encoded_value: bytes) -> tuple[int, Any, Any]:
    if len(encoded_key) < 9:
        raise ValueError("state timer key is truncated")
    timestamp = struct.unpack(">Q", encoded_key[1:9])[0]
    key, namespace = _KEY_SERIALIZER.loads(encoded_value)
    return timestamp, key, namespace


def _component(value: bytes) -> bytes:
    return struct.pack(">I", len(value)) + value


def _read_component(value: bytes, offset: int) -> tuple[bytes, int]:
    if offset + 4 > len(value):
        raise ValueError("managed state key is truncated")
    size = struct.unpack(">I", value[offset : offset + 4])[0]
    start = offset + 4
    end = start + size
    if end > len(value):
        raise ValueError("managed state key is truncated")
    return value[start:end], end
