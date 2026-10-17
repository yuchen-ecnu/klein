# SPDX-License-Identifier: Apache-2.0
"""Equality keys for Ray Data joins that are independent of block nullability."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timezone
from typing import Any, cast

import pyarrow as pa
import pyarrow.compute as pc
from ray.data.datatype import DataType
from ray.data.expressions import Expr, col, pyarrow_udf

from ray.klein.runtime.operator.sql_value_semantics import state_value_key

JOIN_KEY_COLUMN = "_klein_sql_join_key"


def coalesced_join_key_expression(left: str, right: str) -> Expr:
    # Arrow chooses the common input type, including when one side is null.
    # Do not impose a fixed dtype on a USING column, whose type is query-specific.
    @pyarrow_udf(return_dtype=None)
    def coalesce(left_values: pa.Array, right_values: pa.Array) -> pa.Array:
        return pc.coalesce(left_values, right_values)

    return coalesce(col(left), col(right))


def join_key_expression(columns: Sequence[str], *, left: bool) -> Expr:
    """Add a typed key without rebuilding rows or losing an empty schema.

    Ray 2.56's pandas-based shuffle hashes nullable integers differently from
    integers in a block without NULLs. A binary transport key avoids that
    conversion and gives NULL-only blocks a joinable type. Side-specific NULL
    tokens never match, preserving unmatched rows for outer joins.
    """

    @pyarrow_udf(return_dtype=DataType.binary())
    def encode(*arrays: pa.Array) -> pa.Array:
        rows = zip(*(array.to_pylist() for array in arrays), strict=True)
        return pa.array([encode_join_key(values, left=left) for values in rows], type=pa.binary())

    return encode(*(col(column) for column in columns))


def encode_join_key(values: Sequence[Any], *, left: bool) -> bytes:
    if any(value is None for value in values):
        return b"\x00left" if left else b"\x00right"
    # Encode each component separately: pickle memoization of shared objects
    # must not make equal composite values hash differently.
    parts = [_encode_value(value) for value in values]
    return b"\x01" + b"".join(len(part).to_bytes(8, "big") + part for part in parts)


def _encode_value(value: Any) -> bytes:
    # Equal aware datetimes can have different timezone representations.
    if isinstance(value, datetime) and value.tzinfo is not None:
        value = value.astimezone(timezone.utc)
    return cast(bytes, state_value_key(value))
