# SPDX-License-Identifier: Apache-2.0
"""JSON-safe and secret-safe conversion for dashboard payloads."""

from __future__ import annotations

import dataclasses
import enum
import re
from datetime import timedelta
from typing import Any

from ray.klein.config.configuration import Configuration

_SECRET_KEY = re.compile(
    r"(?:^|[.\-_])(password|passwd|secret|token|credential|api[.\-_]?key)(?:$|[.\-_])",
    re.IGNORECASE,
)


def dashboard_value(value: Any) -> Any:
    """Convert runtime values to JSON-compatible primitives."""

    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {key: dashboard_value(item) for key, item in dataclasses.asdict(value).items()}
    if isinstance(value, enum.Enum):
        return value.name
    if isinstance(value, timedelta):
        return value.total_seconds()
    if isinstance(value, dict):
        return {str(key): dashboard_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [dashboard_value(item) for item in value]
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def safe_configuration(config: Configuration | None) -> dict[str, Any]:
    """Return explicit engine options with credential-like values redacted."""

    if config is None:
        return {}
    return {
        key: "<redacted>" if _SECRET_KEY.search(key) else dashboard_value(value)
        for key, value in sorted(config.to_dict().items())
    }
