# SPDX-License-Identifier: Apache-2.0
"""Codec helpers for redistributing managed keyed-state snapshots."""

from __future__ import annotations

import pickle
from collections.abc import Iterable, Mapping
from typing import Any

from ray.klein.state.key_group_range import KeyGroupRange, assign_key_group_range, key_group_owner

_FORMAT_VERSION = 2


def encode_managed_state_snapshot(
    *,
    max_parallelism: int,
    key_group_range: KeyGroupRange,
    key_groups: Mapping[int, bytes],
    watermark: int,
) -> bytes:
    """Serialize one managed-state envelope after validating its metadata."""

    payload = {
        "format_version": _FORMAT_VERSION,
        "max_parallelism": max_parallelism,
        "key_group_range": key_group_range,
        "key_groups": dict(key_groups),
        "watermark": watermark,
    }
    _validate_snapshot_payload(payload)
    return pickle.dumps(payload, protocol=pickle.HIGHEST_PROTOCOL)


def decode_managed_state_snapshot(snapshot: bytes) -> dict[str, Any]:
    """Deserialize and validate one managed-state envelope."""

    if not isinstance(snapshot, bytes):
        raise TypeError("managed state snapshots must be bytes")
    payload = pickle.loads(snapshot)
    if not isinstance(payload, dict):
        raise ValueError("managed state snapshot must contain a mapping")
    _validate_snapshot_payload(payload)
    return payload


def repartition_managed_state_snapshots(
    snapshots: Iterable[bytes],
    target_parallelism: int,
) -> tuple[bytes, ...]:
    """Build one self-contained managed-state snapshot per target subtask.

    Snapshot envelopes are decoded once on the coordinator. Their backend
    key-group payloads remain opaque bytes, so this redistribution works for
    both memory and RocksDB state without materializing backend entries.
    """

    _validate_target_parallelism(target_parallelism)
    snapshot_iterator = iter(snapshots)
    try:
        first_payload = decode_managed_state_snapshot(next(snapshot_iterator))
    except StopIteration as error:
        raise ValueError("at least one managed state snapshot is required") from error

    max_parallelism = _max_parallelism(first_payload)
    # This also rejects a target parallelism above max-parallelism.
    assign_key_group_range(max_parallelism, target_parallelism, 0)
    buckets: list[dict[int, bytes]] = [{} for _ in range(target_parallelism)]
    watermark = _watermark(first_payload)
    _redistribute_key_groups(first_payload, max_parallelism, target_parallelism, buckets)

    for snapshot in snapshot_iterator:
        payload = decode_managed_state_snapshot(snapshot)
        if _max_parallelism(payload) != max_parallelism:
            raise ValueError("managed state snapshots have inconsistent max_parallelism values")
        watermark = min(watermark, _watermark(payload))
        _redistribute_key_groups(payload, max_parallelism, target_parallelism, buckets)

    return tuple(
        encode_managed_state_snapshot(
            max_parallelism=max_parallelism,
            key_group_range=assign_key_group_range(max_parallelism, target_parallelism, index),
            key_groups={key: bucket[key] for key in sorted(bucket)},
            watermark=watermark,
        )
        for index, bucket in enumerate(buckets)
    )


def _validate_snapshot_payload(payload: dict[str, Any]) -> None:
    if payload.get("format_version") != _FORMAT_VERSION:
        raise ValueError("unsupported managed operator state format")
    max_parallelism = _max_parallelism(payload)
    key_group_range = payload.get("key_group_range")
    if not isinstance(key_group_range, KeyGroupRange) or key_group_range.end >= max_parallelism:
        raise ValueError("managed state snapshot has an invalid key_group_range")
    _watermark(payload)
    for key_group in _key_groups(payload):
        if key_group >= max_parallelism or key_group not in key_group_range:
            raise ValueError(f"managed state snapshot contains out-of-range key group {key_group}")


def _max_parallelism(payload: dict[str, Any]) -> int:
    value = payload.get("max_parallelism")
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError("managed state snapshot max_parallelism must be a positive integer")
    return value


def _validate_target_parallelism(target_parallelism: int) -> None:
    if isinstance(target_parallelism, bool) or not isinstance(target_parallelism, int):
        raise TypeError("target_parallelism must be an integer")
    if target_parallelism < 1:
        raise ValueError("target_parallelism must be at least 1")


def _watermark(payload: dict[str, Any]) -> int:
    value = payload.get("watermark", -1)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError("managed state snapshot watermark must be an integer")
    return value


def _redistribute_key_groups(
    payload: dict[str, Any],
    max_parallelism: int,
    target_parallelism: int,
    buckets: list[dict[int, bytes]],
) -> None:
    key_groups = _key_groups(payload)

    for key_group, group_snapshot in key_groups.items():
        owner_bucket = buckets[key_group_owner(key_group, max_parallelism, target_parallelism)]
        previous = owner_bucket.get(key_group)
        if previous is not None and previous != group_snapshot:
            raise ValueError(f"checkpoint contains conflicting key group {key_group}")
        owner_bucket[key_group] = group_snapshot


def _key_groups(payload: dict[str, Any]) -> Mapping[int, bytes]:
    key_groups = payload.get("key_groups", {})
    if not isinstance(key_groups, Mapping):
        raise ValueError("managed state snapshot key_groups must be a mapping")
    for key_group, group_snapshot in key_groups.items():
        if isinstance(key_group, bool) or not isinstance(key_group, int):
            raise ValueError("managed state snapshot key-group ids must be integers")
        if not isinstance(group_snapshot, bytes):
            raise ValueError("managed state snapshot key-group payloads must be bytes")
    return key_groups
