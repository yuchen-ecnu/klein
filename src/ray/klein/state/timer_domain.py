# SPDX-License-Identifier: Apache-2.0
from enum import Enum


class TimerDomain(str, Enum):
    """Clock domain used to schedule a stateful timer."""

    EVENT_TIME = "event_time"
    PROCESSING_TIME = "processing_time"
