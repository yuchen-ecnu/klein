# SPDX-License-Identifier: Apache-2.0
from ray.klein.api._terminal_status_enum import _TerminalStatusEnum


class ExecutionVertexStatus(_TerminalStatusEnum):
    """Lifecycle state of one physical execution vertex."""

    CREATED = "CREATED", False
    DEPLOYED = "DEPLOYED", False
    RUNNING = "RUNNING", False
    CANCELLING = "CANCELLING", False
    FAILED = "FAILED", True
    FINISHED = "FINISHED", True
    CANCELLED = "CANCELLED", True
