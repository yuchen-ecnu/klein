# SPDX-License-Identifier: Apache-2.0
from ray.klein.config.config_option import ConfigOption
from ray.klein.config.deployment_mode import DeploymentMode


class DeploymentOptions:
    MODE = ConfigOption(
        "execution.task.deployment.mode",
        DeploymentMode.DEFAULT,
        DeploymentMode,
        description="Task placement mode.",
    )
