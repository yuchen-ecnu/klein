# SPDX-License-Identifier: Apache-2.0
"""Small collection transformations shared across API lowering paths."""

from typing import Any


def filter_none_items(data: dict[str, Any]) -> dict[str, Any]:
    """Return a shallow copy without values equal to ``None``."""

    return {key: value for key, value in data.items() if value is not None}
