# SPDX-License-Identifier: Apache-2.0
"""Redis value shapes supported by the integration."""

from enum import Enum


class RedisDataType(str, Enum):
    """Redis structures that can be read and replaced by Klein."""

    STRING = "string"
    HASH = "hash"
    SET = "set"
    LIST = "list"
