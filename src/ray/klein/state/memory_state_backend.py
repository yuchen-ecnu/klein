# SPDX-License-Identifier: Apache-2.0
import pickle
import time
from collections.abc import Callable, Iterable, Mapping
from typing import Any

from ray.klein.state.key_group_range import key_group_for_key
from ray.klein.state.managed_state_backend import ManagedStateBackend
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
from ray.klein.state.state_descriptor import StateDescriptor
from ray.klein.state.state_ttl_update_type import StateTTLUpdateType
from ray.klein.state.state_visibility import StateVisibility
from ray.klein.state.timer_domain import TimerDomain
from ray.klein.state.timer_event import TimerEvent


class MemoryStateBackend(ManagedStateBackend):
    """Reference backend for tests and small state."""

    def __init__(self, clock: Callable[[], int] | None = None) -> None:
        self._clock = clock or (lambda: int(time.time() * 1000))
        self._current_key = None
        self._key_set = False
        self._state: dict[bytes, bytes] = {}
        self._expiry: dict[bytes, bytes] = {}
        self._timers: dict[bytes, bytes] = {}

    @property
    def current_key(self) -> Any:
        self._require_current_key()
        return self._current_key

    @current_key.setter
    def current_key(self, key: Any) -> None:
        self._current_key = key
        self._key_set = True

    def get(self, descriptor: StateDescriptor, namespace: Any = None) -> Any:
        state_key = self._state_key(descriptor, namespace)
        encoded = self._state.get(state_key)
        if encoded is None:
            return None
        expires_at, payload = decode_state_value(encoded)
        now = self._clock()
        if (
            expires_at is not None
            and expires_at <= now
            and (
                descriptor.ttl_config is None
                or descriptor.ttl_config.visibility == StateVisibility.NEVER_RETURN_EXPIRED
            )
        ):
            self._state.pop(state_key, None)
            return None
        value = descriptor.serializer.loads(payload)
        if (
            descriptor.ttl_config is not None
            and descriptor.ttl_config.update_type == StateTTLUpdateType.ON_READ_AND_WRITE
        ):
            self.put(descriptor, value, namespace)
        return value

    def put(self, descriptor: StateDescriptor, value: Any, namespace: Any = None) -> None:
        state_key = self._state_key(descriptor, namespace)
        expires_at = self._expiry_for(descriptor)
        self._state[state_key] = encode_state_value(descriptor.serializer.dumps(value), expires_at)
        if expires_at is not None:
            self._expiry[encode_expiry_key(expires_at, state_key)] = b""

    def delete(self, descriptor: StateDescriptor, namespace: Any = None) -> None:
        self._state.pop(self._state_key(descriptor, namespace), None)

    def namespaces(self, descriptor: StateDescriptor) -> tuple[Any, ...]:
        prefix = state_key_prefix(descriptor, self.current_key)
        return tuple(decode_state_namespace(key) for key in sorted(self._state) if key.startswith(prefix))

    def register_timer(self, timestamp: int, namespace: Any, domain: TimerDomain) -> None:
        if timestamp < 0:
            raise ValueError("timer timestamp must be non-negative")
        key = self.current_key
        self._timers[encode_timer_key(timestamp, key, namespace, domain)] = encode_timer_value(key, namespace)

    def delete_timer(self, timestamp: int, namespace: Any, domain: TimerDomain) -> None:
        self._timers.pop(encode_timer_key(timestamp, self.current_key, namespace, domain), None)

    def pop_due_timers(
        self,
        timestamp: int,
        domain: TimerDomain,
        limit: int | None = None,
    ) -> tuple[TimerEvent, ...]:
        result: list[TimerEvent] = []
        prefix = timer_prefix(domain)
        for encoded_key in sorted(self._timers):
            if not encoded_key.startswith(prefix):
                continue
            timer_timestamp, key, namespace = decode_timer(encoded_key, self._timers[encoded_key])
            if timer_timestamp > timestamp:
                break
            result.append(TimerEvent(timer_timestamp, key, namespace, domain))
            if limit is not None and len(result) >= limit:
                break
        for event in result:
            self._timers.pop(encode_timer_key(event.timestamp, event.key, event.namespace, event.domain), None)
        return tuple(result)

    def cleanup_expired(self, now_ms: int | None = None, limit: int | None = None) -> int:
        now_ms = self._clock() if now_ms is None else now_ms
        removed = 0
        for expiry_key in sorted(self._expiry):
            expires_at, state_key = decode_expiry_key(expiry_key)
            if expires_at > now_ms or (limit is not None and removed >= limit):
                break
            encoded = self._state.get(state_key)
            if encoded is not None and decode_state_value(encoded)[0] == expires_at:
                self._state.pop(state_key, None)
                removed += 1
            self._expiry.pop(expiry_key, None)
        return removed

    def snapshot(self) -> bytes:
        return pickle.dumps((self._state, self._expiry, self._timers), protocol=pickle.HIGHEST_PROTOCOL)

    def restore(self, snapshot: bytes) -> None:
        state, expiry, timers = pickle.loads(snapshot)
        self._state = dict(state)
        self._expiry = dict(expiry)
        self._timers = dict(timers)

    def snapshot_key_groups(
        self,
        max_parallelism: int,
        key_groups: Iterable[int],
    ) -> Mapping[int, bytes]:
        requested = frozenset(key_groups)
        buckets: dict[int, dict[str, dict[bytes, bytes]]] = {}

        def bucket(key_group: int) -> dict[str, dict[bytes, bytes]]:
            return buckets.setdefault(
                key_group,
                {"state": {}, "expiry": {}, "timers": {}},
            )

        for encoded_key, encoded_value in self._state.items():
            _name, key, _namespace = decode_state_key(encoded_key)
            key_group = key_group_for_key(key, max_parallelism)
            if key_group in requested:
                bucket(key_group)["state"][encoded_key] = encoded_value
        for encoded_key, encoded_value in self._expiry.items():
            _expires_at, state_key = decode_expiry_key(encoded_key)
            _name, key, _namespace = decode_state_key(state_key)
            key_group = key_group_for_key(key, max_parallelism)
            if key_group in requested:
                bucket(key_group)["expiry"][encoded_key] = encoded_value
        for encoded_key, encoded_value in self._timers.items():
            _timestamp, key, _namespace = decode_timer(encoded_key, encoded_value)
            key_group = key_group_for_key(key, max_parallelism)
            if key_group in requested:
                bucket(key_group)["timers"][encoded_key] = encoded_value

        return {
            key_group: pickle.dumps(
                {"format_version": 1, **contents},
                protocol=pickle.HIGHEST_PROTOCOL,
            )
            for key_group, contents in buckets.items()
        }

    def restore_key_groups(self, snapshots: Mapping[int, bytes]) -> None:
        self._state = {}
        self._expiry = {}
        self._timers = {}
        for snapshot in snapshots.values():
            payload = pickle.loads(snapshot)
            if payload.get("format_version") != 1:
                raise ValueError("unsupported memory key-group state format")
            self._state.update(payload["state"])
            self._expiry.update(payload["expiry"])
            self._timers.update(payload["timers"])

    def close(self) -> None:
        return None

    def _state_key(self, descriptor: StateDescriptor, namespace: Any) -> bytes:
        return encode_state_key(descriptor, self.current_key, namespace)

    def _expiry_for(self, descriptor: StateDescriptor) -> int | None:
        if descriptor.ttl_config is None:
            return None
        return self._clock() + descriptor.ttl_config.ttl_milliseconds

    def _require_current_key(self) -> None:
        if not self._key_set:
            raise RuntimeError("current key is not set")
