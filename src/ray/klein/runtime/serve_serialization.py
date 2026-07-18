# SPDX-License-Identifier: Apache-2.0
"""Wire-format conversion for Klein's Ray Serve integration."""

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
