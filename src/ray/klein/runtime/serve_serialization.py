# SPDX-License-Identifier: Apache-2.0
"""Wire-format conversion and shape guards for Klein's Ray Serve integration."""

from collections.abc import Mapping
from typing import Any

import numpy as np


def numpy_encoder(value: Any) -> Any:
    """Convert NumPy values to JSON-compatible Python values."""
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, (np.integer, np.floating)):
        return value.item()
    raise TypeError(f"Type {type(value).__name__} is not JSON serializable")


def decode_batch(data: dict[str, Any]) -> dict[str, Any]:
    """Restore JSON list columns to arrays before operator execution."""
    if not isinstance(data, dict):
        raise TypeError(f"Serve request body must be an object, got {type(data).__name__}")
    return {key: np.asarray(value) if isinstance(value, list) else value for key, value in data.items()}


def row_count(value: Any) -> int:
    """Return the largest top-level column/result length without materializing it."""

    if isinstance(value, np.ndarray):
        return 1 if value.ndim == 0 else int(value.shape[0])
    if isinstance(value, Mapping):
        return max((row_count(item) for item in value.values()), default=0)
    if isinstance(value, (list, tuple)):
        return len(value)
    return 1


def enforce_row_limit(value: Any, limit: int, label: str) -> None:
    rows = row_count(value)
    if rows > limit:
        raise ValueError(f"{label} has {rows} rows, exceeding the {limit}-row limit")
