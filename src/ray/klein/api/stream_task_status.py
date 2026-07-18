# SPDX-License-Identifier: Apache-2.0
from enum import Enum


class StreamTaskStatus(Enum):
    ALIVE = "ALIVE"
    DEAD = "DEAD"
    NOT_EXIST = "NOT_EXIST"
