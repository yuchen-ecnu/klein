# SPDX-License-Identifier: Apache-2.0


class EnvironmentVariables:
    """Environment-variable names consumed directly by the runtime."""

    DEBUG = "RAY_KLEIN_DEBUG"
    COMPILE_ONLY = "RAY_KLEIN_COMPILE_ONLY"
    RESOURCE_PLAN_INPUT = "RAY_KLEIN_RESOURCE_PLAN_LOAD_PATH"
    RESOURCE_PLAN_OUTPUT = "RAY_KLEIN_RESOURCE_PLAN_PERSIST_PATH"
    SERVICE_NAME = "RAY_SERVICE_NAME"
