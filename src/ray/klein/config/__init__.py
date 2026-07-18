# SPDX-License-Identifier: Apache-2.0
"""Configuration models and option namespaces."""

from typing import Any

from ray.klein._internal.lazy_exports import resolve_lazy_export

_EXPORTS = {
    "CheckpointOptions": ("ray.klein.config.checkpoint_options", "CheckpointOptions"),
    "CheckpointTriggerOptions": ("ray.klein.config.checkpoint_trigger_options", "CheckpointTriggerOptions"),
    "ConfigOption": ("ray.klein.config.config_option", "ConfigOption"),
    "Configuration": ("ray.klein.config.configuration", "Configuration"),
    "ConfigInput": ("ray.klein.config.configuration", "ConfigInput"),
    "DeploymentMode": ("ray.klein.config.deployment_mode", "DeploymentMode"),
    "DeploymentOptions": ("ray.klein.config.deployment_options", "DeploymentOptions"),
    "ExecutionOptions": ("ray.klein.config.execution_options", "ExecutionOptions"),
    "EventTimeOptions": ("ray.klein.config.event_time_options", "EventTimeOptions"),
    "JobManagerOptions": ("ray.klein.config.job_manager_options", "JobManagerOptions"),
    "EnvironmentVariables": ("ray.klein.config.environment_variables", "EnvironmentVariables"),
    "ObservabilityOptions": ("ray.klein.config.observability_options", "ObservabilityOptions"),
    "RuntimeExecutionMode": ("ray.klein.config.runtime_execution_mode", "RuntimeExecutionMode"),
    "ServeOptions": ("ray.klein.config.serve_options", "ServeOptions"),
    "StateOptions": ("ray.klein.config.state_options", "StateOptions"),
    "UDFOptions": ("ray.klein.config.udf_options", "UDFOptions"),
}

__all__ = sorted(_EXPORTS)


def __getattr__(name: str) -> Any:
    return resolve_lazy_export(name, _EXPORTS, globals(), __name__)
