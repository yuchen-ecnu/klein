# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import enum
import json
import math
import os
import re
import shlex
from collections.abc import Mapping
from datetime import timedelta
from typing import Any, TypeAlias, TypeVar, Union

from ray.klein._internal.duration import parse_duration
from ray.klein.config.config_option import (
    ConfigOption,
    _validate_typed_value,
    environment_variable_for,
    normalize_config_key,
)

ConfigInput: TypeAlias = Union["Configuration", Mapping[str, Any], str, None]
_MISSING = object()
_ENV_PREFIX = "RAY_KLEIN_"
T = TypeVar("T")
_INTEGER_PATTERN = re.compile(r"^[+-]?[0-9]+$")


def _convert_boolean(value: Any) -> bool:
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
    elif isinstance(value, bool):
        return value
    elif isinstance(value, int) and value in {0, 1}:
        return bool(value)
    raise ValueError(f"expected a boolean, got {value!r}")


def _convert_integer(value: Any) -> int:
    if isinstance(value, bool):
        raise ValueError(f"expected an integer, got {value!r}")
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        normalized = value.strip()
        if _INTEGER_PATTERN.fullmatch(normalized) is not None:
            return int(normalized)
    raise ValueError(f"expected an integer, got {value!r}")


def _convert_float(value: Any) -> float:
    if isinstance(value, bool):
        raise ValueError(f"expected a float, got {value!r}")
    if isinstance(value, int | float | str):
        try:
            converted = float(value)
        except (ValueError, OverflowError):
            pass
        else:
            if math.isfinite(converted):
                return converted
    raise ValueError(f"expected a float, got {value!r}")


def _convert_duration(value: Any) -> timedelta:
    if isinstance(value, bool):
        raise ValueError(f"expected a duration, got {value!r}")
    if isinstance(value, int | float):
        try:
            seconds = float(value)
            if not math.isfinite(seconds):
                raise ValueError(f"expected a finite duration, got {value!r}")
            return timedelta(seconds=seconds)
        except OverflowError as error:
            raise ValueError(f"duration is out of range, got {value!r}") from error
    if not isinstance(value, str):
        raise ValueError(f"expected a duration, got {value!r}")
    parsed = parse_duration(value)
    if not isinstance(parsed, timedelta):
        raise TypeError("duration parser returned an invalid value")
    return parsed


def _convert_collection(target: type, value: Any) -> Any:
    decoded = json.loads(value) if isinstance(value, str) else value
    if target is dict:
        if not isinstance(decoded, Mapping):
            raise ValueError(f"expected a mapping, got {decoded!r}")
    elif target in {list, tuple} and not isinstance(decoded, list | tuple):
        raise ValueError(f"expected a sequence, got {decoded!r}")
    return target(decoded)


def _convert_enum(target: type[enum.Enum], value: Any) -> enum.Enum:
    normalized = str(value).lower()
    for member in target:
        if member.name.lower() == normalized or str(member.value).lower() == normalized:
            return member
    raise ValueError(f"{value!r} is not valid for {target.__name__}")


def _convert_config_value(target: type, value: Any) -> Any:
    if target is str:
        return str(value)
    if target is int:
        return _convert_integer(value)
    if target is float:
        return _convert_float(value)
    if target is bool:
        return _convert_boolean(value)
    if target is timedelta:
        return _convert_duration(value)
    if target in {dict, list, tuple}:
        return _convert_collection(target, value)
    if isinstance(target, type) and issubclass(target, enum.Enum):
        return _convert_enum(target, value)
    raise TypeError(f"configuration type {target!r} is not supported")


class Configuration:
    """A typed Klein configuration assembled from code, strings and env vars.

    Resolution order is explicit values, a captured ``RAY_KLEIN_*`` environment
    value, then the :class:`ConfigOption` default. Explicit values can be
    supplied as a mapping or as ``key=value`` pairs separated by commas,
    semicolons or whitespace. A JSON object string is also accepted.
    """

    def __init__(
        self,
        options: ConfigInput = None,
        *,
        environment: Mapping[str, str] | None = None,
        include_environment: bool = True,
    ) -> None:
        self._values: dict[str, Any] = {}
        self._environment: dict[str, str]
        if isinstance(options, Configuration):
            self._environment = dict(options._environment)
            self._values.update(options._values)
            return
        self._environment = (
            {
                key.upper(): value
                for key, value in (environment if environment is not None else os.environ).items()
                if key.upper().startswith(_ENV_PREFIX)
            }
            if include_environment
            else {}
        )
        self.update(options)

    def get(self, option: ConfigOption[T] | str, default: Any = _MISSING) -> T | Any | None:
        if isinstance(option, str):
            value = self._raw_value(option)
            if value is not _MISSING:
                return value
            return None if default is _MISSING else default

        value = self.get_optional(option)
        if value is None:
            fallback = option.default if default is _MISSING else default
            if fallback is None:
                return None
            return _validate_typed_value(option.value_type, fallback)
        return _validate_typed_value(option.value_type, value)

    def get_optional(self, option: ConfigOption[T] | str) -> T | Any | None:
        if isinstance(option, str):
            value = self._raw_value(option)
            return None if value is _MISSING else value

        raw_value = self._raw_value(option.key)
        if raw_value is _MISSING or raw_value is None:
            return None
        try:
            return _validate_typed_value(option.value_type, raw_value)
        except TypeError:
            pass
        return self.convert_value(option, raw_value)

    def set(self, option: ConfigOption[T] | str, value: T | Any) -> Configuration:
        if value is None:
            raise ValueError("configuration values cannot be None; use unset() instead")
        if isinstance(option, str):
            self._set_value(option, value)
            return self
        typed_value = _validate_typed_value(option.value_type, value)
        self._set_value(option.key, typed_value)
        return self

    def update(self, options: ConfigInput = None) -> Configuration:
        if options is None:
            return self
        values: Mapping[str, Any]
        if isinstance(options, Configuration):
            self._environment.update(options._environment)
            values = options._values
        elif isinstance(options, str):
            values = self._parse_string(options)
        elif isinstance(options, Mapping):
            values = options
        else:
            raise TypeError("configuration must be a Configuration, mapping, string, or None")
        for key, value in values.items():
            if key is None or value is None:
                raise ValueError("configuration keys and values cannot be None")
            self._set_value(key, value)
        return self

    def unset(self, option: ConfigOption | str) -> Configuration:
        key = option.key if isinstance(option, ConfigOption) else normalize_config_key(option)
        self._values.pop(key, None)
        return self

    def _raw_value(self, key: str) -> Any:
        canonical = normalize_config_key(key)
        if canonical in self._values:
            return self._values[canonical]
        return self._environment.get(environment_variable_for(canonical), _MISSING)

    def _set_value(self, key: str, value: Any) -> None:
        self._values[normalize_config_key(key)] = value

    def to_dict(self) -> dict[str, Any]:
        return dict(self._values)

    @staticmethod
    def _parse_string(value: str) -> dict[str, Any]:
        text = value.strip()
        if not text:
            return {}
        if text.startswith("{"):
            decoded = json.loads(text)
            if not isinstance(decoded, dict):
                raise ValueError("JSON configuration must be an object")
            return decoded

        lexer = shlex.shlex(text, posix=True)
        lexer.commenters = ""
        lexer.whitespace += ",;"
        lexer.whitespace_split = True
        result: dict[str, Any] = {}
        for item in lexer:
            if "=" not in item:
                raise ValueError(f"invalid configuration token {item!r}; expected key=value")
            key, raw_value = item.split("=", 1)
            if not raw_value:
                raise ValueError(f"configuration value for {key!r} cannot be empty")
            try:
                parsed_value = json.loads(raw_value)
            except json.JSONDecodeError:
                parsed_value = raw_value
            result[key] = parsed_value
        return result

    @staticmethod
    def convert_value(option: ConfigOption[T], raw_value: Any) -> T:
        target = option.value_type
        try:
            converted = _convert_config_value(target, raw_value)
            return _validate_typed_value(target, converted)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"unable to convert {raw_value!r} for {option.key!r} to {target.__name__}") from exc
