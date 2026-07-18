# SPDX-License-Identifier: Apache-2.0
from enum import Enum


class DeploymentMode(Enum):
    """Deployment policy for stream tasks."""

    DEFAULT = "DEFAULT"
    BALANCED = "BALANCED"
