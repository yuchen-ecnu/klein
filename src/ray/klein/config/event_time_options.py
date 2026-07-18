# SPDX-License-Identifier: Apache-2.0
from datetime import timedelta

from ray.klein.config.config_option import ConfigOption


class EventTimeOptions:
    """Configuration for event-time progress and idle-input detection."""

    IDLE_INPUT_CHECK_INTERVAL = ConfigOption(
        "event-time.idle-input.check-interval",
        timedelta(seconds=1),
        timedelta,
        description="How often an input-idleness strategy is evaluated while a task inbox is empty.",
    )
