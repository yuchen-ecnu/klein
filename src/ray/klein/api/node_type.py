# SPDX-License-Identifier: Apache-2.0
from enum import Enum


class NodeType(Enum):
    SOURCE = 1
    SINK = 2
    TRANSFORM = 3
    TAKE = 4
    UNION = 5
