# SPDX-FileCopyrightText: 2024-2026 Klein for Ray Authors
#
# SPDX-License-Identifier: Apache-2.0
"""Small value helpers kept local to avoid depending on Ray private modules."""

from collections.abc import Sequence
from typing import Any

import numpy as np


def truncated_repr(value: Any, limit: int = 200) -> str:
    """Return a bounded string representation suitable for error messages."""
    message = str(value)
    return message if len(message) <= limit else f"{message[:limit]}..."


def is_valid_column_values(value: Any) -> bool:
    """Return whether *value* can represent a column in a batched record."""
    return isinstance(value, (list, np.ndarray)) or (hasattr(value, "__array__") and hasattr(value, "__len__"))


def create_ragged_ndarray(values: Sequence[Any]) -> np.ndarray:
    """Create a one-dimensional object array without broadcasting its values."""
    result = np.empty(len(values), dtype=object)
    result[:] = list(values)
    return result
