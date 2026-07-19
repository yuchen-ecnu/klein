# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import enum
import json
import os
import shlex
from collections.abc import Mapping
from datetime import timedelta
from typing import Any, TypeAlias, TypeVar, Union, cast

from ray.klein._internal.duration import parse_duration
from ray.klein.config.config_option import ConfigOption, environment_variable_for, normalize_config_key

ConfigInput: TypeAlias = Union["Configuration", Mapping[str, Any], str, None]
_MISSING = object()
_ENV_PREFIX = "RAY_KLEIN_"
T = TypeVar("T")


def _convert_boolean(value: Any) -> bool:
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
    elif isinstance(value, int | bool):
        return bool(value)
    raise ValueError(f"expected a boolean, got {value!r}")


def _convert_duration(value: Any) -> timedelta:
    if isinstance(value, int | float):
        return timedelta(seconds=float(value))
    return parse_duration(value)


def _convert_collection(target: type, value: Any) -> Any:
    decoded = json.loads(value) if isinstance(value, str) else value
    return target(decoded)


def _convert_enum(target: type[enum.Enum], value: Any) -> enum.Enum:
    normalized = str(value).lower()
    for member in target:
        if member.name.lower() == normalized or str(member.value).lower() == normalized:
            return member
    raise ValueError(f"{value!r} is not valid for {target.__name__}")


def _convert_config_value(target: type, value: Any) -> Any:
    primitive_converters = {str: str, int: int, float: float}
    if target in primitive_converters:
        return primitive_converters[target](value)
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
            return option.default if default is _MISSING else default
        if isinstance(value, option.value_type):
            return value
        raise TypeError(f"wrong type of value {value!r}; expected {option.value_type.__name__}")

    def get_optional(self, option: ConfigOption[T] | str) -> T | Any | None:
        if isinstance(option, str):
            value = self._raw_value(option)
            return None if value is _MISSING else value

        raw_value = self._raw_value(option.key)
        if raw_value is _MISSING or raw_value is None:
            return None
        if isinstance(raw_value, option.value_type):
            return raw_value
        return self.convert_value(option, raw_value)

    def set(self, option: ConfigOption[T] | str, value: T | Any) -> Configuration:
        if value is None:
            raise ValueError("configuration values cannot be None; use unset() instead")
        if isinstance(option, str):
            self._set_value(option, value)
            return self
        if not isinstance(value, option.value_type):
            raise TypeError(f"wrong type of value {value!r}; expected {option.value_type.__name__}")
        self._set_value(option.key, value)
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
            return cast(T, _convert_config_value(target, raw_value))
        except (TypeError, ValueError) as exc:
            raise ValueError(f"unable to convert {raw_value!r} for {option.key!r} to {target.__name__}") from exc
