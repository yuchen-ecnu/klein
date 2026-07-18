# SPDX-License-Identifier: Apache-2.0
from enum import Enum


class TimerDomain(str, Enum):
    EVENT_TIME = "event_time"
    PROCESSING_TIME = "processing_time"
