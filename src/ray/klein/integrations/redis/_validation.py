# SPDX-License-Identifier: Apache-2.0
"""Validation shared by Redis integration configuration objects."""

from datetime import timedelta


def positive_seconds(duration: timedelta, name: str) -> float:
    seconds = duration.total_seconds()
    if seconds <= 0:
        raise ValueError(f"{name} must be positive")
    return seconds
