# SPDX-License-Identifier: Apache-2.0
"""Static physical-channel topology shared by planning and runtime control flow."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class ChannelPattern(Enum):
    ALL_TO_ALL = "all-to-all"
    FORWARD = "forward"
    RESCALE = "rescale"


@dataclass(frozen=True, slots=True)
class ChannelTopology:
    """Pure mapping from one upstream subtask to its downstream channels.

    Data partitioners may select one or more channels inside this topology, but
    barriers and physical execution edges always use this single static mapping.
    Keeping it outside the mutable runtime partitioner prevents planning/runtime
    drift during checkpoint alignment.
    """

    pattern: ChannelPattern = ChannelPattern.ALL_TO_ALL

    def target_indices(
        self,
        source_parallelism: int,
        target_parallelism: int,
        source_index: int,
    ) -> tuple[int, ...]:
        if source_parallelism <= 0 or target_parallelism <= 0:
            raise ValueError("channel parallelism must be greater than zero")
        if source_index < 0 or source_index >= source_parallelism:
            raise ValueError(f"source index {source_index} is outside parallelism {source_parallelism}")
        if self.pattern is ChannelPattern.ALL_TO_ALL:
            return tuple(range(target_parallelism))
        if self.pattern is ChannelPattern.FORWARD:
            if source_parallelism != target_parallelism:
                raise ValueError(
                    "forward topology requires equal source and target parallelism: "
                    f"{source_parallelism} != {target_parallelism}"
                )
            return (source_index,)
        if source_parallelism >= target_parallelism:
            return (source_index % target_parallelism,)
        return tuple(range(source_index, target_parallelism, source_parallelism))


ALL_TO_ALL = ChannelTopology(ChannelPattern.ALL_TO_ALL)
FORWARD = ChannelTopology(ChannelPattern.FORWARD)
RESCALE = ChannelTopology(ChannelPattern.RESCALE)
