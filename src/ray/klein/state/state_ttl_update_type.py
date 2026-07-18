# SPDX-License-Identifier: Apache-2.0
from enum import Enum


class StateTTLUpdateType(str, Enum):
    ON_CREATE_AND_WRITE = "on_create_and_write"
    ON_READ_AND_WRITE = "on_read_and_write"
