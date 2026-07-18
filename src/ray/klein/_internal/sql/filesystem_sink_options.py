# SPDX-License-Identifier: Apache-2.0
"""Typed decoding for Flink-style filesystem sink table options."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, ClassVar

from ray.klein._internal.duration import parse_duration
from ray.klein._internal.sql.connector_options import parse_option_value
from ray.klein.api.sql_query_error import SQLQueryError

_SIZE_PATTERN = re.compile(r"^(\d+(?:\.\d+)?)\s*([a-zA-Z]*)$")
_SIZE_MULTIPLIERS = {
    "": 1,
    "b": 1,
    "kb": 1_000,
    "mb": 1_000_000,
    "gb": 1_000_000_000,
    "tb": 1_000_000_000_000,
    "kib": 1 << 10,
    "mib": 1 << 20,
    "gib": 1 << 30,
    "tib": 1 << 40,
}


@dataclass(frozen=True, slots=True)
class FilesystemSinkOptions:
    """Validated options passed from a catalog table to ``write_files``."""

    OPTION_NAMES: ClassVar[frozenset[str]] = frozenset(
        {
            "sink.filename-prefix",
            "sink.max-rows-per-file",
            "sink.parallelism",
            "sink.ray-data-options",
            "sink.rolling-policy.file-size",
            "sink.rolling-policy.inactivity-interval",
            "sink.rolling-policy.rollover-interval",
            "sink.storage-options",
        }
    )

    filename_prefix: str = "part"
    max_rows_per_file: int | None = None
    max_bytes_per_file: int | None = None
    rollover_interval: timedelta | None = None
    inactivity_interval: timedelta | None = None
    storage_options: dict[str, Any] | None = None
    ray_data_options: dict[str, Any] | None = None
    parallelism: int | None = None

    @classmethod
    def from_mapping(cls, options: Mapping[str, str]) -> FilesystemSinkOptions:
        filename_prefix = options.get("sink.filename-prefix", "part").strip()
        if not filename_prefix:
            raise SQLQueryError("Filesystem option 'sink.filename-prefix' must not be empty")
        return cls(
            filename_prefix=filename_prefix,
            max_rows_per_file=_optional_positive_int(options, "sink.max-rows-per-file"),
            max_bytes_per_file=_optional_size(options, "sink.rolling-policy.file-size"),
            rollover_interval=_optional_duration(options, "sink.rolling-policy.rollover-interval"),
            inactivity_interval=_optional_duration(options, "sink.rolling-policy.inactivity-interval"),
            storage_options=_optional_mapping(options, "sink.storage-options"),
            ray_data_options=_optional_mapping(options, "sink.ray-data-options"),
            parallelism=_optional_positive_int(options, "sink.parallelism"),
        )


def _optional_positive_int(options: Mapping[str, str], name: str) -> int | None:
    value = options.get(name)
    if value is None:
        return None
    try:
        parsed = int(value)
    except ValueError as error:
        raise SQLQueryError(f"Filesystem option {name!r} must be a positive integer") from error
    if parsed <= 0:
        raise SQLQueryError(f"Filesystem option {name!r} must be a positive integer")
    return parsed


def _optional_size(options: Mapping[str, str], name: str) -> int | None:
    value = options.get(name)
    if value is None:
        return None
    match = _SIZE_PATTERN.fullmatch(value.strip())
    if match is None:
        raise SQLQueryError(f"Filesystem option {name!r} is not a valid byte size: {value!r}")
    amount, unit = match.groups()
    multiplier = _SIZE_MULTIPLIERS.get(unit.lower())
    if multiplier is None:
        raise SQLQueryError(f"Filesystem option {name!r} has an unsupported size unit: {unit!r}")
    result = int(float(amount) * multiplier)
    if result <= 0:
        raise SQLQueryError(f"Filesystem option {name!r} must be greater than zero")
    return result


def _optional_duration(options: Mapping[str, str], name: str) -> timedelta | None:
    value = options.get(name)
    if value is None:
        return None
    try:
        duration = parse_duration(re.sub(r"\s+", "", value))
    except ValueError as error:
        raise SQLQueryError(f"Filesystem option {name!r} is not a valid duration: {value!r}") from error
    if duration <= timedelta(0):
        raise SQLQueryError(f"Filesystem option {name!r} must be greater than zero")
    return duration


def _optional_mapping(options: Mapping[str, str], name: str) -> dict[str, Any] | None:
    value = options.get(name)
    if value is None:
        return None
    parsed = parse_option_value(value)
    if not isinstance(parsed, dict):
        raise SQLQueryError(f"Filesystem option {name!r} must be a JSON object")
    return parsed
