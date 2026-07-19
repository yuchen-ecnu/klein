# SPDX-License-Identifier: Apache-2.0
"""Column-oriented record block operations."""

import warnings
from collections.abc import Sequence
from typing import Any

import numpy as np
import pyarrow as pa

from ray.klein._internal.values import create_ragged_ndarray


def wrapper_batch_data(data: list[Any], batch_format: str | None) -> Any:
    if batch_format in {None, "native"}:
        return data
    if batch_format == "pyarrow":
        return pa.array(data)
    if batch_format in {"numpy", "default"}:
        return create_possibly_ragged_ndarray(data)
    raise ValueError(f"Unsupported batch format {batch_format!r}")


def block_num_rows(block: dict[str, Any] | None) -> int:
    """Return the row count of a column-oriented block."""

    if not block:
        return 0
    return len(next(iter(block.values())))


def slice_block_rows(block: dict[str, Any], indices: Sequence[int] | slice) -> dict[str, Any]:
    """Select the same row indices from every column."""

    contiguous = _contiguous_slice(indices)
    if contiguous is not None:
        start, stop = contiguous
        result: dict[str, Any] = {}
        for column, values in block.items():
            if isinstance(values, pa.Array):
                result[column] = values.slice(start, max(0, stop - start))
            else:
                # NumPy basic slicing returns a view. Preserve the original
                # sequence for a full-span slice to avoid copying Python lists.
                result[column] = values if start == 0 and stop == len(values) else values[start:stop]
        return result

    selected = list(indices)
    result: dict[str, Any] = {}
    for column, values in block.items():
        if isinstance(values, np.ndarray):
            result[column] = values[selected]
        elif isinstance(values, pa.Array):
            result[column] = values.take(pa.array(selected))
        else:
            result[column] = [values[index] for index in selected]
    return result


def _contiguous_slice(indices: Sequence[int] | slice) -> tuple[int, int] | None:
    if isinstance(indices, slice):
        if indices.step not in (None, 1) or indices.start is None or indices.stop is None:
            return None
        return indices.start, indices.stop
    if not indices:
        return 0, 0
    start = indices[0]
    if any(index != start + offset for offset, index in enumerate(indices)):
        return None
    return start, start + len(indices)


def block_row_dict(block: dict[str, Any], index: int) -> dict[str, Any]:
    """Extract one row from a column-oriented block."""

    return {column: values[index] for column, values in block.items()}


def concat_blocks(blocks: list[dict[str, Any]]) -> dict[str, Any]:
    """Concatenate same-schema blocks without converting them to row dictionaries."""

    if not blocks:
        return {}
    if len(blocks) == 1:
        return blocks[0]
    result: dict[str, Any] = {}
    for column in blocks[0]:
        parts = [block[column] for block in blocks]
        first = parts[0]
        if isinstance(first, np.ndarray):
            result[column] = np.concatenate(parts)
        elif isinstance(first, pa.Array):
            result[column] = pa.concat_arrays(parts)
        else:
            result[column] = [value for part in parts for value in part]
    return result


def create_possibly_ragged_ndarray(values: np.ndarray | Sequence[Any]) -> np.ndarray:
    """Create an ndarray and preserve ragged values when NumPy rejects the shape."""

    try:
        with warnings.catch_warnings():
            visible_deprecation_warning = getattr(
                getattr(np, "exceptions", None),
                "VisibleDeprecationWarning",
                DeprecationWarning,
            )
            warnings.simplefilter("ignore", category=visible_deprecation_warning)
            return np.asarray(values)
    except ValueError as error:
        message = str(error)
        if (
            "could not broadcast input array from shape" in message
            or "The requested array has an inhomogeneous shape" in message
        ):
            return create_ragged_ndarray(values)
        raise
