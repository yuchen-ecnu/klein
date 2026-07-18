# SPDX-License-Identifier: Apache-2.0
from ray.klein.config.config_option import ConfigOption


class ObservabilityOptions:
    """Configuration for Klein's metrics and cluster state publication."""

    DASHBOARD_ENABLED = ConfigOption(
        "observability.dashboard.enabled",
        True,
        bool,
        description="Publish redacted job snapshots to the detached cluster state actor.",
    )

    DASHBOARD_HISTORY_SIZE = ConfigOption(
        "observability.dashboard.history-size",
        100,
        int,
        description="Maximum number of Klein jobs retained by the cluster state actor.",
    )
