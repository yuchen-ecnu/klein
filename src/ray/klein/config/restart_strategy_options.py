# SPDX-License-Identifier: Apache-2.0
from datetime import timedelta

from ray.klein.config.config_option import ConfigOption


class RestartStrategyOptions:
    MAX_ATTEMPTS = ConfigOption("execution.restart-strategy.fixed-delay.attempts", 3, int)

    DELAY = ConfigOption("execution.restart-strategy.fixed-delay.delay", timedelta(seconds=10), timedelta)

    COUNT_INTERVAL = ConfigOption(
        "execution.restart-strategy.fixed-delay.count-interval", timedelta(minutes=10), timedelta
    )
