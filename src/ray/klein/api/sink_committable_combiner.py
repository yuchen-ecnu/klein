# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from typing import Protocol, runtime_checkable

from ray.klein.api.sink_committable import SinkCommittable


@runtime_checkable
class SinkCommittableCombiner(Protocol):
    """Optional hook for sinks that publish all parallel writers atomically.

    The checkpoint coordinator discovers this protocol structurally and stays
    independent of connector implementations. Implementations must validate
    that every supplied committable belongs to the same sink and return one
    idempotent transaction for the complete writer group.
    """

    @property
    def global_commit_namespace(self) -> str:
        """Short stable namespace used in the global transaction ID."""

    def combine_committables(
        self,
        committables: tuple[SinkCommittable, ...],
        *,
        transaction_id: str,
    ) -> SinkCommittable:
        """Combine one checkpoint's parallel writer transactions."""
