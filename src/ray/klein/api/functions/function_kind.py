# SPDX-License-Identifier: Apache-2.0
from enum import Enum, auto


class FunctionKind(Enum):
    """How a logical function is materialized by the streaming runtime."""

    STATELESS = auto()
    COLLECT = auto()
    LIFECYCLE = auto()
    CALLABLE_CLASS = auto()
