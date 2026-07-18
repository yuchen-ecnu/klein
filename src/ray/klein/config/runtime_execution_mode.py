# SPDX-License-Identifier: Apache-2.0
from enum import Enum


class RuntimeExecutionMode(Enum):
    """Execution mode used by a Klein job."""

    STREAMING = "STREAMING"
    BATCH = "BATCH"
    AUTO = "AUTO"
