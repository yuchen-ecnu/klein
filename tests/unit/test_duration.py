# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import pytest

from ray.klein._internal.duration import parse_duration


@pytest.mark.parametrize(
    ("value", "expected_seconds"),
    [
        ("1000ms", 1),
        ("100s", 100),
        ("1min", 60),
        ("0.5h", 1800),
        ("1d", 24 * 60 * 60),
        ("1w", 7 * 24 * 60 * 60),
        ("0.5seconds", 0.5),
        ("1.6minutes", 1.6 * 60),
    ],
)
def test_parse_duration(value: str, expected_seconds: float) -> None:
    assert parse_duration(value).total_seconds() == expected_seconds
