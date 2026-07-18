# SPDX-License-Identifier: Apache-2.0
from enum import Enum, auto


class RayDataMethodKind(Enum):
    """How a Ray ``Dataset`` method participates in a Klein graph."""

    TRANSFORM = auto()
    CONSUME = auto()
