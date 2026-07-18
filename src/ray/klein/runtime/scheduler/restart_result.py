# SPDX-License-Identifier: Apache-2.0
from dataclasses import dataclass
from enum import Enum


class RestartStatus(str, Enum):
    """Outcome of a restart attempt."""

    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    SUPPRESSED = "SUPPRESSED"


@dataclass(frozen=True, slots=True)
class RestartResult:
    """Status and diagnostic returned by one restart attempt."""

    status: RestartStatus
    message: str
