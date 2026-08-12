# SPDX-License-Identifier: Apache-2.0
"""Stable value identity and ordering for stateful SQL operators."""

from __future__ import annotations

import math
from collections.abc import Mapping, Set
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, cast

from ray.klein.state.key_encoding import encode_key


@dataclass(frozen=True, slots=True)
class _NaNIdentity:
    """Equality token used only inside durable, encoded multiset keys."""

    kind: str
    detail: Any = None


def state_value_key(value: Any) -> bytes:
    """Encode SQL values with deterministic identity for otherwise-unequal NaNs."""

    return cast(bytes, encode_key(_normalize_nan(value, set())))


def is_nan_value(value: Any) -> bool:
    """Return whether a scalar follows IEEE/Decimal NaN self-inequality."""

    if isinstance(value, Decimal):
        return value.is_nan()
    try:
        return bool(value != value)
    except (TypeError, ValueError):
        return False


def infinity_sign(value: Any) -> int:
    """Return -1/1 for real infinities and zero for other SQL values."""

    try:
        if not math.isinf(value):
            return 0
        return 1 if value > 0 else -1
    except (TypeError, ValueError):
        return 0


def compare_non_null_values(left: Any, right: Any) -> int:
    """Compare non-null SQL values, treating all NaNs as one greatest value."""

    left_nan = is_nan_value(left)
    right_nan = is_nan_value(right)
    if left_nan or right_nan:
        if left_nan and right_nan:
            return 0
        return 1 if left_nan else -1
    if left > right:
        return 1
    if left < right:
        return -1
    return 0


def _normalize_nan(value: Any, active: set[int]) -> Any:
    if type(value) is float and math.isnan(value):
        return _NaNIdentity("float")
    if type(value) is Decimal and value.is_nan():
        return _NaNIdentity("decimal")
    if type(value) is complex and (math.isnan(value.real) or math.isnan(value.imag)):
        real = _NaNIdentity("float") if math.isnan(value.real) else value.real
        imaginary = _NaNIdentity("float") if math.isnan(value.imag) else value.imag
        return _NaNIdentity("complex", (real, imaginary))
    if type(value) is tuple:
        return tuple(_normalize_items(value, active))
    if type(value) is list:
        return list(_normalize_items(value, active))
    if type(value) is dict:
        return _normalize_mapping(value, active)
    if type(value) is set:
        return set(_normalize_items(value, active))
    if type(value) is frozenset:
        return frozenset(_normalize_items(value, active))
    return value


def _normalize_items(values: tuple | list | Set, active: set[int]) -> tuple[Any, ...]:
    identity = id(values)
    if identity in active:
        raise TypeError("cyclic SQL values cannot be used in retractable state")
    active.add(identity)
    try:
        return tuple(_normalize_nan(value, active) for value in values)
    finally:
        active.remove(identity)


def _normalize_mapping(value: Mapping, active: set[int]) -> dict[Any, Any]:
    identity = id(value)
    if identity in active:
        raise TypeError("cyclic SQL values cannot be used in retractable state")
    active.add(identity)
    try:
        return {_normalize_nan(key, active): _normalize_nan(item, active) for key, item in value.items()}
    finally:
        active.remove(identity)
