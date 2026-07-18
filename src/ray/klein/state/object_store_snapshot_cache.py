# SPDX-License-Identifier: Apache-2.0
import hashlib
from collections.abc import Callable
from typing import Any

from ray.klein.state.state_snapshot_reference import StateSnapshotReference


class ObjectStoreSnapshotCache:
    """Caches only large immutable snapshots; small snapshots remain inline."""

    def __init__(
        self,
        put: Callable[[bytes], Any],
        get: Callable[[Any], bytes],
        *,
        min_size_bytes: int = 1024 * 1024,
        enabled: bool = True,
    ) -> None:
        if min_size_bytes < 0:
            raise ValueError("min_size_bytes must be non-negative")
        self._put = put
        self._get = get
        self._min_size_bytes = min_size_bytes
        self._enabled = enabled

    def cache(self, payload: bytes) -> StateSnapshotReference:
        checksum = f"sha256:{hashlib.sha256(payload).hexdigest()}"
        if self._enabled and len(payload) >= self._min_size_bytes:
            return StateSnapshotReference(len(payload), checksum, object_ref=self._put(payload))
        return StateSnapshotReference(len(payload), checksum, inline_payload=payload)

    def materialize(self, reference: StateSnapshotReference) -> bytes:
        return reference.materialize(self._get)
