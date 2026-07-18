# SPDX-License-Identifier: Apache-2.0
import hashlib
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class StateSnapshotReference:
    size_bytes: int
    checksum: str
    inline_payload: bytes | None = None
    object_ref: Any = None

    def __post_init__(self) -> None:
        if self.size_bytes < 0:
            raise ValueError("snapshot size must be non-negative")
        if not self.checksum:
            raise ValueError("snapshot checksum must not be empty")
        if (self.inline_payload is None) == (self.object_ref is None):
            raise ValueError("snapshot reference must contain exactly one payload representation")

    def materialize(self, get: Callable[[Any], bytes]) -> bytes:
        """Resolve and verify an inline or nested Ray ObjectRef payload."""

        payload = self.inline_payload
        if payload is None:
            payload = get(self.object_ref)
        if not isinstance(payload, bytes):
            raise TypeError("state snapshot payload must be bytes")
        if len(payload) != self.size_bytes:
            raise ValueError("state snapshot size mismatch")
        checksum = f"sha256:{hashlib.sha256(payload).hexdigest()}"
        if checksum != self.checksum:
            raise ValueError("state snapshot checksum mismatch")
        return payload
