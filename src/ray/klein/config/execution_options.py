# SPDX-License-Identifier: Apache-2.0
from ray.klein.config.config_option import ConfigOption
from ray.klein.config.runtime_execution_mode import RuntimeExecutionMode


class ExecutionOptions:
    MODE = ConfigOption(
        "execution.runtime.mode", RuntimeExecutionMode.AUTO, RuntimeExecutionMode, description="Execution mode."
    )
