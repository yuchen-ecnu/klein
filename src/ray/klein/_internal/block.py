# SPDX-License-Identifier: Apache-2.0
"""Column-oriented record block operations."""

import warnings
from collections.abc import Mapping, Sequence
from typing import Any

import numpy as np
import pyarrow as pa

from ray.klein._internal.values import create_ragged_ndarray

ArrowBlock = pa.RecordBatch | pa.Table
ColumnarBlock = Mapping[str, Any] | ArrowBlock


def wrapper_batch_data(data: list[Any], batch_format: str | None) -> Any:
    if batch_format in {None, "native"}:
        return data
    if batch_format == "pyarrow":
        return pa.array(data)
    if batch_format in {"numpy", "default"}:
        if any(isinstance(value, bytes | bytearray | memoryview) for value in data):
            # A NumPy ``S`` array cannot distinguish binary trailing NULs from
            # fixed-width padding. Keep binary values as Python objects.
            return create_ragged_ndarray(data)
        return create_possibly_ragged_ndarray(data)
    raise ValueError(f"Unsupported batch format {batch_format!r}")


def block_num_rows(block: ColumnarBlock | None) -> int:
    """Return the row count of a column-oriented block."""

    if not block:
        return 0
    if isinstance(block, pa.RecordBatch | pa.Table):
        return block.num_rows
    return len(next(iter(block.values())))


def slice_block_rows(block: ColumnarBlock, indices: Sequence[int] | slice) -> ColumnarBlock:
    """Select the same row indices from every column."""

    contiguous = _contiguous_slice(indices)
    if isinstance(block, pa.RecordBatch | pa.Table):
        batch = to_arrow_record_batch(block)
        if contiguous is not None:
            start, stop = contiguous
            return batch.slice(start, max(0, stop - start))
        return batch.take(pa.array(list(indices), type=pa.int64()))
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


def block_row_dict(block: ColumnarBlock, index: int) -> dict[str, Any]:
    """Extract one row from a column-oriented block."""

    if isinstance(block, pa.RecordBatch | pa.Table):
        return {
            name: _python_value(block.column(column_index)[index])
            for column_index, name in enumerate(block.schema.names)
        }
    return {column: _python_value(values[index]) for column, values in block.items()}


def concat_blocks(blocks: list[ColumnarBlock]) -> ColumnarBlock:
    """Concatenate same-schema blocks without converting them to row dictionaries."""

    if not blocks:
        return {}
    if len(blocks) == 1:
        return blocks[0]
    if any(isinstance(block, pa.RecordBatch | pa.Table) for block in blocks):
        batches = [to_arrow_record_batch(block) for block in blocks]
        schema = batches[0].schema
        if any(not batch.schema.equals(schema) for batch in batches[1:]):
            raise ValueError("cannot concatenate Arrow blocks with different schemas")
        arrays = [pa.concat_arrays([batch.column(index) for batch in batches]) for index in range(len(schema))]
        return pa.RecordBatch.from_arrays(arrays, schema=schema)
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


def to_arrow_record_batch(
    block: ColumnarBlock,
    *,
    expected_rows: int | None = None,
    row_shaped: bool = False,
) -> pa.RecordBatch:
    """Convert one internal block to a single Arrow transport batch."""

    if isinstance(block, pa.RecordBatch):
        batch = block
    elif isinstance(block, pa.Table):
        combined = block.combine_chunks()
        arrays = [column.combine_chunks() for column in combined.columns]
        batch = pa.RecordBatch.from_arrays(arrays, schema=combined.schema)
    else:
        names = list(block)
        if any(not isinstance(name, str) for name in names):
            raise TypeError("Arrow transport column names must be strings")
        values = [[block[name]] for name in names] if row_shaped else [block[name] for name in names]
        arrays = [_to_arrow_array(column) for column in values]
        batch = pa.RecordBatch.from_arrays(arrays, names=names)
    if expected_rows is not None and batch.num_rows != expected_rows:
        raise ValueError(f"Arrow block has {batch.num_rows} rows; expected {expected_rows}")
    return batch


def try_to_arrow_record_batch(
    block: ColumnarBlock,
    *,
    expected_rows: int | None = None,
    row_shaped: bool = False,
) -> pa.RecordBatch | None:
    """Best-effort Arrow conversion for the Python-object compatibility path."""

    try:
        return to_arrow_record_batch(block, expected_rows=expected_rows, row_shaped=row_shaped)
    except (pa.ArrowInvalid, pa.ArrowNotImplementedError, pa.ArrowTypeError, TypeError, ValueError, OverflowError):
        return None


def arrow_block_to_mapping(block: ArrowBlock, batch_format: str | None) -> dict[str, Any]:
    """Materialize an Arrow transport batch only at a declared UDF boundary."""

    batch = to_arrow_record_batch(block)
    if batch_format == "pyarrow":
        return {name: batch.column(index) for index, name in enumerate(batch.schema.names)}
    if batch_format in {None, "native"}:
        return {name: batch.column(index).to_pylist() for index, name in enumerate(batch.schema.names)}
    if batch_format in {"numpy", "default"}:
        return {name: _arrow_array_to_numpy(batch.column(index)) for index, name in enumerate(batch.schema.names)}
    raise ValueError(f"Unsupported batch format {batch_format!r}")


def _to_arrow_array(values: Any) -> pa.Array:
    if isinstance(values, pa.Array):
        return values
    if isinstance(values, pa.ChunkedArray):
        return values.combine_chunks()
    if isinstance(values, np.ndarray):
        if values.ndim > 1 and values.dtype != object:
            return pa.FixedShapeTensorArray.from_numpy_ndarray(values)
        if values.ndim == 0:
            return pa.array([values.item()])
        if values.dtype.kind in {"O", "S"}:
            return _numpy_object_array_to_arrow(values)
        return pa.array(values)
    if isinstance(values, Sequence) and not isinstance(values, str | bytes | bytearray | memoryview):
        materialized = list(values)
        tensors = [value for value in materialized if value is not None]
        if tensors and len(tensors) == len(materialized) and all(isinstance(value, np.ndarray) for value in tensors):
            first = tensors[0]
            if first.dtype != object and all(
                value.shape == first.shape and value.dtype == first.dtype for value in tensors
            ):
                return pa.FixedShapeTensorArray.from_numpy_ndarray(np.stack(tensors))
        return pa.array([_nested_python_value(value) for value in materialized])
    return pa.array(values)


def _numpy_object_array_to_arrow(values: np.ndarray) -> pa.Array:
    if values.dtype.kind == "S":
        # NumPy fixed-width byte strings are C-string-like at the Arrow
        # boundary and otherwise lose everything after the first NUL.
        return pa.array([bytes(value) for value in values], type=pa.binary())
    return pa.array([_nested_python_value(value) for value in values])


def _arrow_array_to_numpy(values: pa.Array) -> np.ndarray:
    to_numpy_ndarray = getattr(values, "to_numpy_ndarray", None)
    if callable(to_numpy_ndarray):
        return to_numpy_ndarray()
    if (
        pa.types.is_binary(values.type)
        or pa.types.is_large_binary(values.type)
        or pa.types.is_fixed_size_binary(values.type)
    ):
        # NumPy's fixed-width ``S`` dtype is C-string-like when PyArrow reads
        # it back and truncates binary payloads at their first NUL byte (for
        # example, every PNG after its 8-byte signature). Keep binary as an
        # object array so embedded NULs survive repeated Arrow/UDF boundaries.
        return create_ragged_ndarray(values.to_pylist())
    if pa.types.is_list(values.type) or pa.types.is_large_list(values.type) or pa.types.is_fixed_size_list(values.type):
        # Arrow list columns are semantically nested values, not tensor axes.
        # A one-dimensional object array prevents equal-length PDF page lists
        # from being reinterpreted as a fixed-shape NumPy tensor on the next hop.
        return create_ragged_ndarray(values.to_pylist())
    if values.null_count:
        return create_possibly_ragged_ndarray(values.to_pylist())
    try:
        return values.to_numpy(zero_copy_only=True)
    except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
        return create_possibly_ragged_ndarray(values.to_pylist())


def _nested_python_value(value: Any) -> Any:
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, memoryview):
        return bytes(value)
    return value


def _python_value(value: Any) -> Any:
    if isinstance(value, pa.Scalar):
        return value.as_py()
    return value.item() if isinstance(value, np.generic) else value


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
