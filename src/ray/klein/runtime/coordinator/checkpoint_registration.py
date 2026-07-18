# SPDX-License-Identifier: Apache-2.0
from typing import NamedTuple


class CheckpointRegistration(NamedTuple):
    """Result of ``register_checkpoint``.

    ``barrier_id`` is None when no checkpoint was triggered. Build via the
    ``success`` / ``skip`` factories so each case only takes applicable fields.
    """

    barrier_id: int | None
    reason: str

    @classmethod
    def success(cls, barrier_id: int) -> "CheckpointRegistration":
        return cls(barrier_id, "")

    @classmethod
    def skip(cls, reason: str) -> "CheckpointRegistration":
        return cls(None, reason)
