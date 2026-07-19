# SPDX-License-Identifier: Apache-2.0
"""Immutable recipes for task-local partitioners."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from ray.klein._internal.frozen_mapping import FrozenMapping
from ray.klein.runtime.partitioning.channel_topology import ALL_TO_ALL, ChannelTopology
from ray.klein.runtime.partitioning.partitioner import Partitioner


@dataclass(frozen=True, slots=True)
class PartitionerSpec:
    """Picklable constructor recipe stored on logical and physical edges."""

    partitioner_class: type[Partitioner]
    args: tuple[Any, ...] = ()
    kwargs: Mapping[str, Any] = field(default_factory=FrozenMapping)
    name: str = ""
    topology: ChannelTopology = ALL_TO_ALL

    def __post_init__(self) -> None:
        if not isinstance(self.partitioner_class, type) or not issubclass(self.partitioner_class, Partitioner):
            raise TypeError("partitioner_class must be a Partitioner subclass")
        if not isinstance(self.topology, ChannelTopology):
            raise TypeError("topology must be a ChannelTopology")
        object.__setattr__(self, "args", tuple(self.args))
        object.__setattr__(self, "kwargs", FrozenMapping(self.kwargs))
        if not self.name:
            object.__setattr__(self, "name", self.partitioner_class.__name__)

    def build(self) -> Partitioner:
        partitioner = self.partitioner_class(*self.args, **dict(self.kwargs))
        if not isinstance(partitioner, Partitioner):
            raise TypeError(f"{self.partitioner_class!r} did not build a Partitioner")
        return partitioner

    def target_indices(
        self,
        source_parallelism: int,
        target_parallelism: int,
        source_index: int,
    ) -> tuple[int, ...]:
        return self.topology.target_indices(source_parallelism, target_parallelism, source_index)

    def is_type(self, partitioner_type: type[Partitioner]) -> bool:
        return issubclass(self.partitioner_class, partitioner_type)

    def __str__(self) -> str:
        return self.name
