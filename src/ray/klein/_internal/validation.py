# SPDX-License-Identifier: Apache-2.0
"""Internal string validation."""


def is_blank(value: str | None) -> bool:
    return value is None or not value.strip()
