# SPDX-License-Identifier: Apache-2.0
from datetime import timedelta

from ray.klein.config.config_option import ConfigOption


class TableOptions:
    """Flink-compatible options used by the streaming SQL runtime."""

    STATE_TTL = ConfigOption(
        "table.exec.state.ttl",
        None,
        timedelta,
        description="Idle retention for regular joins and non-windowed aggregations.",
    )
