# SPDX-License-Identifier: Apache-2.0
from enum import Enum


class CheckpointFileScope(str, Enum):
    """Ownership scope matching Flink filesystem checkpoint storage."""

    EXCLUSIVE = "exclusive"
    SHARED = "shared"
    TASK_OWNED = "taskowned"
