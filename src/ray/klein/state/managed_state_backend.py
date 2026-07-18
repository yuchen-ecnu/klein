# SPDX-License-Identifier: Apache-2.0
from abc import ABC, abstractmethod
from collections.abc import Iterable, Mapping
from typing import Any

from ray.klein.state.state_descriptor import StateDescriptor
from ray.klein.state.timer_domain import TimerDomain
from ray.klein.state.timer_event import TimerEvent


class ManagedStateBackend(ABC):
    """Backend-neutral storage contract used by keyed operators."""

    @property
    @abstractmethod
    def current_key(self) -> Any:
        """Currently selected state key."""

    @current_key.setter
    @abstractmethod
    def current_key(self, key: Any) -> None:
        """Select the key used by subsequent state operations."""

    @abstractmethod
    def get(self, descriptor: StateDescriptor, namespace: Any = None) -> Any:
        """Read one state value."""

    @abstractmethod
    def put(self, descriptor: StateDescriptor, value: Any, namespace: Any = None) -> None:
        """Write one state value."""

    @abstractmethod
    def delete(self, descriptor: StateDescriptor, namespace: Any = None) -> None:
        """Delete one state value."""

    @abstractmethod
    def namespaces(self, descriptor: StateDescriptor) -> tuple[Any, ...]:
        """Return namespaces containing the described state."""

    @abstractmethod
    def register_timer(self, timestamp: int, namespace: Any, domain: TimerDomain) -> None:
        """Register a timer."""

    @abstractmethod
    def delete_timer(self, timestamp: int, namespace: Any, domain: TimerDomain) -> None:
        """Delete a timer."""

    @abstractmethod
    def pop_due_timers(
        self,
        timestamp: int,
        domain: TimerDomain,
        limit: int | None = None,
    ) -> tuple[TimerEvent, ...]:
        """Remove and return timers due by ``timestamp``."""

    @abstractmethod
    def cleanup_expired(self, now_ms: int | None = None, limit: int | None = None) -> int:
        """Remove expired values and return the number deleted."""

    @abstractmethod
    def snapshot(self) -> bytes:
        """Serialize the complete backend state."""

    @abstractmethod
    def restore(self, snapshot: bytes) -> None:
        """Replace backend contents from a complete snapshot."""

    @abstractmethod
    def snapshot_key_groups(
        self,
        max_parallelism: int,
        key_groups: Iterable[int],
    ) -> Mapping[int, bytes]:
        """Export portable logical snapshots for the requested key groups."""

    @abstractmethod
    def restore_key_groups(self, snapshots: Mapping[int, bytes]) -> None:
        """Replace current contents with the union of logical key-group snapshots."""

    @abstractmethod
    def close(self) -> None:
        """Release backend resources."""
