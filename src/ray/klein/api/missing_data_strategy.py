# SPDX-License-Identifier: Apache-2.0
from enum import Enum


class MissingDataStrategy(Enum):
    """
    MissingDataStrategy for MapReduce Operator.
    """

    IGNORE = "IGNORE"
    WARNING = "WARNING"
    ERROR = "ERROR"
