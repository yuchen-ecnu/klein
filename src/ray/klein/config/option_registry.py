# SPDX-License-Identifier: Apache-2.0
"""Registry of framework-owned configuration options."""

from __future__ import annotations

from functools import lru_cache

from ray.klein.config.checkpoint_options import CheckpointOptions
from ray.klein.config.checkpoint_trigger_options import CheckpointTriggerOptions
from ray.klein.config.config_option import ConfigOption
from ray.klein.config.deployment_options import DeploymentOptions
from ray.klein.config.event_time_options import EventTimeOptions
from ray.klein.config.execution_options import ExecutionOptions
from ray.klein.config.job_manager_options import JobManagerOptions
from ray.klein.config.observability_options import ObservabilityOptions
from ray.klein.config.pipeline_options import PipelineOptions
from ray.klein.config.restart_strategy_options import RestartStrategyOptions
from ray.klein.config.serve_options import ServeOptions
from ray.klein.config.sql_download_options import SQLDownloadOptions
from ray.klein.config.state_options import StateOptions
from ray.klein.config.table_options import TableOptions
from ray.klein.config.udf_options import UDFOptions

_OPTION_NAMESPACES = (
    CheckpointOptions,
    CheckpointTriggerOptions,
    DeploymentOptions,
    EventTimeOptions,
    ExecutionOptions,
    JobManagerOptions,
    ObservabilityOptions,
    PipelineOptions,
    RestartStrategyOptions,
    ServeOptions,
    SQLDownloadOptions,
    StateOptions,
    TableOptions,
    UDFOptions,
)


@lru_cache(maxsize=1)
def known_config_options() -> dict[str, ConfigOption]:
    """Return every configuration option owned by the Klein runtime."""

    options: dict[str, ConfigOption] = {}
    for namespace in _OPTION_NAMESPACES:
        for value in vars(namespace).values():
            if isinstance(value, ConfigOption):
                options[value.key] = value
    return dict(sorted(options.items()))


__all__ = ["known_config_options"]
