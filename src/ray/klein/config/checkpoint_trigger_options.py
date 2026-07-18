# SPDX-License-Identifier: Apache-2.0
from datetime import timedelta

from ray.klein.config.config_option import ConfigOption


class CheckpointTriggerOptions:
    """Independent volume and time thresholds for source checkpoints.

    Both thresholds are active. A checkpoint is triggered by whichever one is
    reached first, then both counters reset.
    """

    INTERVAL_DURATION = ConfigOption(
        "execution.checkpointing.trigger.interval-duration",
        timedelta(seconds=60),
        timedelta,
        description="Maximum duration between source checkpoints.",
    )

    INTERVAL_RECORDS = ConfigOption(
        "execution.checkpointing.trigger.interval-records",
        512,
        int,
        description="Maximum emitted records between source checkpoints.",
    )
