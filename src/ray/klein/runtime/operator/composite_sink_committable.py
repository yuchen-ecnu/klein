# SPDX-License-Identifier: Apache-2.0
"""One checkpoint transaction composed from multiple chained sinks."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

from ray.klein.api.sink_committable import SinkCommittable


@dataclass(frozen=True, slots=True)
class CompositeSinkCommittable(SinkCommittable):
    """Commit or abort every child while preserving idempotent retries."""

    committables: tuple[SinkCommittable, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "committables", tuple(self.committables))
        if not self.committables:
            raise ValueError("a composite sink committable requires at least one child")
        if not all(isinstance(item, SinkCommittable) for item in self.committables):
            raise TypeError("composite children must be SinkCommittable values")

    @property
    def transaction_id(self) -> str:
        identity = "".join(f"{len(item.transaction_id)}:{item.transaction_id}" for item in self.committables)
        return f"composite-{hashlib.sha256(identity.encode()).hexdigest()}"

    def commit(self) -> None:
        # Stop at the first failure. A retry starts at the first child again;
        # the contract requires every child commit to be idempotent.
        for committable in self.committables:
            committable.commit()

    def abort(self) -> None:
        first_error: Exception | None = None
        for committable in self.committables:
            try:
                committable.abort()
            except Exception as error:
                if first_error is None:
                    first_error = error
        if first_error is not None:
            raise first_error.with_traceback(first_error.__traceback__)
