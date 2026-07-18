# SPDX-License-Identifier: Apache-2.0
from dataclasses import dataclass


@dataclass(frozen=True, order=True, slots=True)
class StatePartition:
    """Stable identity of one operator key group."""

    job_id: str
    operator_id: str
    key_group: int

    def __post_init__(self) -> None:
        if not self.job_id:
            raise ValueError("job_id must not be empty")
        if not self.operator_id:
            raise ValueError("operator_id must not be empty")
        if self.key_group < 0:
            raise ValueError("key_group must be non-negative")

    @property
    def storage_key(self) -> str:
        """Return a stable, human-readable checkpoint key."""

        return f"{self.job_id}/{self.operator_id}/{self.key_group}"
