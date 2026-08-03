# SPDX-License-Identifier: Apache-2.0
"""Configuration duration parsing."""

import math
import re
from datetime import timedelta

_DURATION_PATTERN = re.compile(r"^([-+]?\d+(?:\.\d+)?)([A-Za-z]+)$")
_DURATION_UNITS = {
    "ms": "milliseconds",
    "millisecond": "milliseconds",
    "milliseconds": "milliseconds",
    "s": "seconds",
    "second": "seconds",
    "seconds": "seconds",
    "min": "minutes",
    "minute": "minutes",
    "minutes": "minutes",
    "h": "hours",
    "hour": "hours",
    "hours": "hours",
    "d": "days",
    "day": "days",
    "days": "days",
    "w": "weeks",
    "week": "weeks",
    "weeks": "weeks",
}


def parse_duration(value: str) -> timedelta:
    """Parse a compact duration such as ``500ms`` or ``1.5hours``."""

    match = _DURATION_PATTERN.fullmatch(value.strip())
    if match is None:
        raise ValueError(f"Cannot parse duration {value!r}")
    amount, raw_unit = match.groups()
    try:
        unit = _DURATION_UNITS[raw_unit.lower()]
    except KeyError as exc:
        raise ValueError(f"Unsupported duration unit {raw_unit!r}") from exc
    try:
        numeric_amount = float(amount)
        if not math.isfinite(numeric_amount):
            raise ValueError("duration amount must be finite")
        return timedelta(**{unit: numeric_amount})
    except (OverflowError, ValueError) as exc:
        raise ValueError(f"Duration is out of range: {value!r}") from exc
