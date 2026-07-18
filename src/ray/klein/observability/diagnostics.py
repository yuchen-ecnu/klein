# SPDX-License-Identifier: Apache-2.0
"""Bounded operational diagnostics routed through standard logging."""

from __future__ import annotations

import logging
import traceback
from enum import Enum

from ray.klein._internal.logging import get_logger

logger = get_logger(__name__)

_MAX_LOG_MESSAGE_LENGTH = 32_768


class DiagnosticLevel(Enum):
    WARN = "warning"
    ERROR = "error"


def report_diagnostic(diagnostic_level: DiagnosticLevel, message: str) -> None:
    """Report one bounded operational diagnostic."""

    level = logging.ERROR if diagnostic_level is DiagnosticLevel.ERROR else logging.WARNING
    logger.log(level, "%s", truncate_diagnostic(message))


def truncate_diagnostic(message: str, max_length: int = _MAX_LOG_MESSAGE_LENGTH) -> str:
    """Bound diagnostic payloads without depending on a private backend."""
    if len(message) <= max_length:
        return message
    suffix = "\n... [truncated by Klein for Ray]"
    return message[: max_length - len(suffix)] + suffix


def current_exception_diagnostic() -> str:
    """Return the active exception traceback as a bounded string."""
    return truncate_diagnostic(traceback.format_exc())
