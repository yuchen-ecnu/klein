# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import math
import re
from dataclasses import dataclass
from typing import Any, Generic, TypeVar, cast

from ray.klein._internal.validation import is_blank

_VALID_KEY = re.compile(r"^[a-z0-9]+(?:[.-][a-z0-9]+)*$")
T = TypeVar("T")


def _validate_typed_value(value_type: type[T], value: Any) -> T:
    """Validate and normalize an already-typed configuration value."""

    if value_type is int:
        valid = isinstance(value, int) and not isinstance(value, bool)
    elif value_type is float:
        valid = isinstance(value, int | float) and not isinstance(value, bool)
        if valid:
            try:
                normalized = float(value)
            except OverflowError as error:
                raise TypeError(f"wrong type of value {value!r}; expected a finite float") from error
            if not math.isfinite(normalized):
                raise TypeError(f"wrong type of value {value!r}; expected a finite float")
            return cast(T, normalized)
    else:
        valid = isinstance(value, value_type)
    if not valid:
        raise TypeError(f"wrong type of value {value!r}; expected {value_type.__name__}")
    return cast(T, value)


def normalize_config_key(key: str) -> str:
    """Return Klein's canonical lower-case dotted/kebab configuration key."""

    if not isinstance(key, str) or is_blank(key):
        raise ValueError("configuration key cannot be blank")
    normalized = key.strip().lower().replace("_", "-")
    normalized = re.sub(r"-+", "-", normalized)
    normalized = re.sub(r"\.+", ".", normalized)
    if _VALID_KEY.fullmatch(normalized) is None:
        raise ValueError(
            f"invalid configuration key {key!r}; use lower-case dotted segments and kebab-case compound names"
        )
    return normalized


def environment_variable_for(key: str) -> str:
    canonical = normalize_config_key(key)
    return "RAY_KLEIN_" + re.sub(r"[.-]", "_", canonical).upper()


@dataclass(frozen=True, slots=True)
class ConfigOption(Generic[T]):
    """Typed configuration option with a canonical key and environment name."""

    key: str
    default: T | None
    value_type: type[T]
    description: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "key", normalize_config_key(self.key))
        if not isinstance(self.value_type, type):
            raise TypeError("configuration value_type must be a type")
        if self.default is not None:
            try:
                default = _validate_typed_value(self.value_type, self.default)
            except TypeError as exc:
                raise TypeError(
                    f"default value {self.default!r} must be an instance of {self.value_type.__name__}"
                ) from exc
            object.__setattr__(self, "default", default)

    @property
    def environment_variable(self) -> str:
        return environment_variable_for(self.key)
