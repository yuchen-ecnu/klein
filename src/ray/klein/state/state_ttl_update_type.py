# SPDX-License-Identifier: Apache-2.0
from enum import Enum


class StateTTLUpdateType(str, Enum):
    """Controls when state access refreshes its time-to-live timestamp."""

    ON_CREATE_AND_WRITE = "on_create_and_write"
    ON_READ_AND_WRITE = "on_read_and_write"
