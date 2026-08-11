# SPDX-License-Identifier: Apache-2.0
"""Project-owned markers for documented API stability."""

from __future__ import annotations

from typing import TypeVar

T = TypeVar("T")


def public_api(value: T) -> T:
    """Mark a documented Klein API without inheriting another project's policy."""

    vars(value)["__klein_api_stability__"] = "alpha"
    return value
