# SPDX-License-Identifier: Apache-2.0
"""Equality-preserving, process-independent encoding for keyed state."""

from __future__ import annotations

import math
import pickle
from collections.abc import Iterable, Mapping, Set
from dataclasses import dataclass
from decimal import Decimal
from fractions import Fraction
from typing import Any

# Version 1 was the unversioned direct-pickle representation.
KEY_ENCODING_VERSION = 2


class _CanonicalizationCycleError(Exception):
    pass


@dataclass(frozen=True, slots=True)
class _CanonicalSet:
    """Pickle a set as an ordered tuple, but decode it as a frozenset."""

    items: tuple[Any, ...]

    def __reduce__(self) -> tuple[Any, tuple[tuple[Any, ...]]]:
        return (_decode_canonical_set, (self.items,))


def _decode_canonical_set(items: tuple[Any, ...]) -> frozenset[Any]:
    # This function name is part of the durable pickle representation.
    return frozenset(items)


def encode_key(key: Any, *, protocol: int = 4) -> bytes:
    """Encode supported structural key values consistently with equality.

    Pickle remains the outer representation so existing state keys stay
    decodable. Common scalar encodings are unchanged; only equality classes
    whose pickle representations differ (for example ``1``/``1.0`` and
    unordered containers) are normalized.

    The equality guarantee covers standard numeric and string/bytes-like
    values plus exact built-in tuple, list, range, dict, set, and frozenset
    containers composed from them. Subclasses and custom classes fall back to
    pickle: callers must project custom ``__eq__`` semantics onto supported
    structural values themselves.
    """

    try:
        canonical = _canonicalize(key, set())
    except _CanonicalizationCycleError:
        canonical = key
    try:
        return pickle.dumps(canonical, protocol=protocol)
    except (pickle.PickleError, TypeError, AttributeError) as exc:
        raise TypeError("keyed stream keys must be pickle-serializable") from exc


def decode_key(encoded: bytes) -> Any:
    """Decode both legacy pickle keys and normalized keys."""

    return pickle.loads(encoded)


def require_key_encoding_version(version: Any, *, context: str) -> None:
    """Reject snapshots that cannot be restored without changing key identity."""

    if version is None:
        raise ValueError(
            f"{context} uses the legacy key encoding and cannot be restored safely; "
            "resume it with the Klein version that wrote it, or restart from clean state"
        )
    if isinstance(version, bool) or not isinstance(version, int) or version != KEY_ENCODING_VERSION:
        raise ValueError(
            f"{context} uses unsupported key encoding version {version!r}; expected {KEY_ENCODING_VERSION}"
        )


def _canonicalize(value: Any, active: set[int]) -> Any:  # noqa: C901
    if value is None:
        return None
    if type(value) is str:
        return str(value)
    if type(value) in (bytes, bytearray):
        return bytes(value)
    if type(value) is memoryview:
        return _canonical_memoryview(value)
    if type(value) is Decimal:
        return _canonical_decimal(value)
    if type(value) in (bool, int):
        return int(value)
    if type(value) is Fraction:
        return _canonical_ratio(value.numerator, value.denominator)
    if type(value) is float:
        return _canonical_real(value)
    if type(value) is complex:
        return _canonical_complex(value)
    if type(value) is tuple:
        return tuple(_canonicalize_items(value, active))
    if type(value) is list:
        return list(_canonicalize_items(value, active))
    if type(value) is range:
        return _canonical_range(value)
    if type(value) is dict:
        return _canonical_mapping(value, active)
    if type(value) in (set, frozenset):
        return _canonical_set(value, active)
    return value


def _canonical_memoryview(value: memoryview) -> bytes:
    """Normalize only views that really participate in bytes equality."""

    try:
        payload = bytes(value)
    except (TypeError, ValueError) as error:
        raise TypeError("memoryview keys must expose byte-compatible equality") from error
    if value != payload:
        raise TypeError(
            "memoryview keys with format or shape-sensitive equality are unsupported; "
            "project the key to bytes or a structural tuple"
        )
    return payload


def _canonical_decimal(value: Decimal) -> Any:
    if value.is_nan():
        raise TypeError("NaN values are unsupported as keyed-state keys")
    if value.is_infinite():
        return math.inf if value > 0 else -math.inf
    numerator, denominator = value.as_integer_ratio()
    return _canonical_ratio(numerator, denominator)


def _canonical_real(value: Any) -> Any:
    if math.isnan(value):
        raise TypeError("NaN values are unsupported as keyed-state keys")
    try:
        numerator, denominator = value.as_integer_ratio()
    except (OverflowError, ValueError):
        return float(value)
    return _canonical_ratio(numerator, denominator)


def _canonical_complex(value: complex) -> Any:
    if math.isnan(value.real) or math.isnan(value.imag):
        raise TypeError("NaN values are unsupported as keyed-state keys")
    real = 0.0 if value.real == 0 else value.real
    imaginary = 0.0 if value.imag == 0 else value.imag
    if imaginary == 0:
        return _canonical_real(real)
    return complex(real, imaginary)


def _canonical_ratio(numerator: int, denominator: int) -> Any:
    ratio = Fraction(numerator, denominator)
    if ratio.denominator == 1:
        return ratio.numerator
    if ratio.denominator & (ratio.denominator - 1) == 0:
        try:
            number = ratio.numerator / ratio.denominator
        except OverflowError:
            return ratio
        if math.isfinite(number) and number.as_integer_ratio() == (
            ratio.numerator,
            ratio.denominator,
        ):
            return number
    return ratio


def _canonicalize_items(values: Iterable[Any], active: set[int]) -> tuple[Any, ...]:
    identity = id(values)
    if identity in active:
        raise _CanonicalizationCycleError
    active.add(identity)
    try:
        return tuple(_canonicalize(value, active) for value in values)
    finally:
        active.remove(identity)


def _canonical_mapping(value: Mapping, active: set[int]) -> dict[Any, Any]:
    identity = id(value)
    if identity in active:
        raise _CanonicalizationCycleError
    active.add(identity)
    try:
        items = [(_canonicalize(key, active), _canonicalize(item, active)) for key, item in value.items()]
        items.sort(key=lambda pair: encode_key(pair[0]))
        return dict(items)
    finally:
        active.remove(identity)


def _canonical_set(value: Set, active: set[int]) -> _CanonicalSet:
    items = _canonicalize_items(value, active)
    return _CanonicalSet(tuple(sorted(items, key=encode_key)))


def _canonical_range(value: range) -> range:
    if value.step > 0:
        size = 0 if value.start >= value.stop else (value.stop - value.start - 1) // value.step + 1
    else:
        size = 0 if value.start <= value.stop else (value.start - value.stop - 1) // -value.step + 1
    if size == 0:
        return range(0)
    first = value[0]
    if size == 1:
        return range(first, first + 1)
    return range(first, first + value.step * size, value.step)
