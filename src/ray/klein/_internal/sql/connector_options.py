# SPDX-License-Identifier: Apache-2.0
"""Validation and decoding for Flink-style connector properties."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from ray.klein.api.sql_query_error import SQLQueryError


def require_option(options: Mapping[str, str], name: str, connector: str) -> str:
    value = options.get(name)
    if value is None or not value.strip():
        raise SQLQueryError(f"Connector {connector!r} requires option {name!r}")
    return value


def prefixed_options(options: Mapping[str, str], prefix: str) -> dict[str, Any]:
    return {
        name.removeprefix(prefix): parse_option_value(value)
        for name, value in options.items()
        if name.startswith(prefix)
    }


def parse_option_value(value: str) -> Any:
    """Decode Flink-style string properties when they contain JSON scalars."""

    stripped = value.strip()
    if not stripped:
        return value
    try:
        return json.loads(stripped)
    except json.JSONDecodeError:
        return value


def reject_unknown_options(
    options: Mapping[str, str],
    *,
    connector: str,
    supported: set[str],
    prefixes: tuple[str, ...] = (),
) -> None:
    unknown = sorted(
        name for name in options if name not in supported and not any(name.startswith(prefix) for prefix in prefixes)
    )
    if unknown:
        raise SQLQueryError(f"Unsupported option(s) for connector {connector!r}: {', '.join(unknown)}")
