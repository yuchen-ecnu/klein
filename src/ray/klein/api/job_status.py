# SPDX-License-Identifier: Apache-2.0
from ray.klein.api._terminal_status_enum import _TerminalStatusEnum


class JobStatus(_TerminalStatusEnum):
    """Lifecycle states exposed by a Klein job handle."""

    CREATED = "CREATED", False
    SUBMITTING = "SUBMITTING", False
    DEPLOYING = "DEPLOYING", False
    INITIALIZING = "INITIALIZING", False
    RUNNING = "RUNNING", False
    FINISHED = "FINISHED", True
    CANCELLED = "CANCELLED", True
    FAILED = "FAILED", True
