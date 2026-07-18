# SPDX-License-Identifier: Apache-2.0
from dataclasses import dataclass

from ray.klein.runtime.resources import Resources


@dataclass(frozen=True, slots=True)
class ResourceNode:
    """Validated resource settings for one stream graph node."""

    id: int
    name: str
    num_cpus: float | None
    num_gpus: float | None
    concurrency: int | tuple[int, int] | None
    batch_size: int | None
    async_buffer_size: int | None

    def __post_init__(self) -> None:
        if isinstance(self.id, bool) or not isinstance(self.id, int) or self.id < 0:
            raise ValueError("resource node id must be a non-negative integer")
        if not self.name.strip():
            raise ValueError("resource node name cannot be blank")
        Resources(self.num_cpus, self.num_gpus, self.concurrency)
        for field_name, value in (
            ("batch_size", self.batch_size),
            ("async_buffer_size", self.async_buffer_size),
        ):
            if value is not None and (isinstance(value, bool) or not isinstance(value, int) or value <= 0):
                raise ValueError(f"{field_name} must be a positive integer or None")

    @property
    def key(self) -> str:
        return f"{self.name}[{self.id}]"

    @property
    def cpus(self) -> float:
        return self.num_cpus if self.num_cpus is not None else 1.0

    @property
    def gpus(self) -> float:
        return self.num_gpus if self.num_gpus is not None else 0.0

    @property
    def effective_concurrency(self) -> int | tuple[int, int]:
        return self.concurrency if self.concurrency is not None else 1
