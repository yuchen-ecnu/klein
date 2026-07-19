# SPDX-License-Identifier: Apache-2.0
"""Decode Canal FlatMessage JSON into Klein changelog rows."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any, Literal

from ray.klein.api.changelog_row import ChangelogRow

DdlHandling = Literal["ignore", "emit", "fail"]

_DML_TYPES = frozenset({"INSERT", "UPDATE", "DELETE"})
_DDL_TYPES = frozenset({"CREATE", "ALTER", "ERASE", "TRUNCATE", "RENAME", "CINDEX", "DINDEX"})
_OPTION_NAMES = frozenset({"include_metadata", "ddl_handling"})
_METADATA_FIELDS = {
    "id": "id",
    "database": "database",
    "table": "table",
    "type": "event_type",
    "es": "execute_time",
    "ts": "build_time",
    "gtid": "gtid",
}


def decode_canal_json(
    payload: bytes | bytearray | memoryview | str,
    *,
    include_metadata: bool = True,
    ddl_handling: DdlHandling = "ignore",
) -> list[ChangelogRow]:
    """Decode one Canal ``FlatMessage`` JSON value.

    Canal represents row values as strings (or ``null``); this decoder keeps
    those values unchanged. UPDATE ``old`` entries contain only the columns
    that changed, so they are overlaid on the corresponding full ``data`` row
    to reconstruct the before image.
    """

    options = _normalize_canal_json_options({"include_metadata": include_metadata, "ddl_handling": ddl_handling})
    message = _load_message(payload)
    event_type = _event_type(message)
    is_ddl = _is_ddl(message, event_type)
    if is_ddl:
        return _decode_ddl(
            message,
            event_type,
            include_metadata=options["include_metadata"],
            handling=options["ddl_handling"],
        )

    data = _rows(message.get("data"), "data", required=event_type in _DML_TYPES)
    if event_type not in _DML_TYPES:
        if data:
            raise ValueError(f"Unsupported Canal event type {event_type!r} with row data")
        return []

    metadata = _metadata(message, event_type) if options["include_metadata"] else {}
    if event_type == "INSERT":
        return [ChangelogRow.insert(_output_row(row, metadata)) for row in data]
    if event_type == "DELETE":
        return [ChangelogRow.delete(_output_row(row, metadata)) for row in data]
    return _decode_updates(message, data, metadata)


def _normalize_canal_json_options(options: Mapping[str, Any] | None) -> dict[str, Any]:
    if options is None:
        options = {}
    if not isinstance(options, Mapping):
        raise TypeError("format_options must be a mapping")
    unknown = sorted(set(options) - _OPTION_NAMES)
    if unknown:
        raise ValueError(f"Unsupported canal-json format option(s): {', '.join(unknown)}")
    include_metadata = options.get("include_metadata", True)
    ddl_handling = options.get("ddl_handling", "ignore")
    if not isinstance(include_metadata, bool):
        raise TypeError("canal-json include_metadata must be a boolean")
    if ddl_handling not in {"ignore", "emit", "fail"}:
        raise ValueError("canal-json ddl_handling must be 'ignore', 'emit', or 'fail'")
    return {"include_metadata": include_metadata, "ddl_handling": ddl_handling}


def _load_message(payload: bytes | bytearray | memoryview | str) -> Mapping[str, Any]:
    if isinstance(payload, str):
        value = payload
    elif isinstance(payload, (bytes, bytearray, memoryview)):
        try:
            value = bytes(payload).decode("utf-8")
        except UnicodeDecodeError as error:
            raise ValueError("Canal message is not valid UTF-8") from error
    else:
        raise TypeError("Canal payload must be bytes or a string")
    try:
        message = json.loads(value)
    except json.JSONDecodeError as error:
        raise ValueError("Canal message is not valid JSON") from error
    if not isinstance(message, Mapping):
        raise ValueError("Canal message must be a JSON object")
    return message


def _event_type(message: Mapping[str, Any]) -> str:
    value = message.get("type")
    if not isinstance(value, str) or not value.strip():
        raise ValueError("Canal message requires a non-empty string 'type'")
    return value.upper()


def _is_ddl(message: Mapping[str, Any], event_type: str) -> bool:
    value = message.get("isDdl", False)
    if not isinstance(value, bool):
        raise ValueError("Canal message 'isDdl' must be a boolean")
    return value or event_type in _DDL_TYPES


def _rows(value: Any, name: str, *, required: bool) -> list[dict[str, Any]]:
    if value is None:
        if required:
            raise ValueError(f"Canal DML message requires a '{name}' row list")
        return []
    if not isinstance(value, list) or any(not isinstance(row, Mapping) for row in value):
        raise ValueError(f"Canal message '{name}' must be a list of JSON objects")
    return [dict(row) for row in value]


def _decode_updates(
    message: Mapping[str, Any],
    after_rows: list[dict[str, Any]],
    metadata: dict[str, Any],
) -> list[ChangelogRow]:
    old_rows = _rows(message.get("old"), "old", required=True)
    if len(old_rows) != len(after_rows):
        raise ValueError("Canal UPDATE 'old' and 'data' row counts must match")

    changes: list[ChangelogRow] = []
    for after, old_values in zip(after_rows, old_rows, strict=True):
        before = {**after, **old_values}
        changes.append(ChangelogRow.update_before(_output_row(before, metadata)))
        changes.append(ChangelogRow.update_after(_output_row(after, metadata)))
    return changes


def _decode_ddl(
    message: Mapping[str, Any],
    event_type: str,
    *,
    include_metadata: bool,
    handling: DdlHandling,
) -> list[ChangelogRow]:
    if handling == "ignore":
        return []
    if handling == "fail":
        raise ValueError(f"Canal DDL event {event_type!r} is not enabled")
    row = _metadata(message, event_type) if include_metadata else {"__canal_event_type": event_type}
    row["__canal_is_ddl"] = True
    row["__canal_sql"] = message.get("sql")
    return [ChangelogRow.insert(row)]


def _metadata(message: Mapping[str, Any], event_type: str) -> dict[str, Any]:
    metadata: dict[str, Any] = {}
    for source_name, output_name in _METADATA_FIELDS.items():
        value = event_type if source_name == "type" else message.get(source_name)
        if value is not None:
            metadata[f"__canal_{output_name}"] = value
    pk_names = message.get("pkNames")
    if pk_names is not None:
        if not isinstance(pk_names, list) or any(not isinstance(name, str) for name in pk_names):
            raise ValueError("Canal message 'pkNames' must be a list of strings")
        metadata["__canal_pk_names"] = list(pk_names)
    return metadata


def _output_row(data: Mapping[str, Any], metadata: Mapping[str, Any]) -> dict[str, Any]:
    # Reserved metadata wins if a source table happens to use the same column.
    return {**data, **metadata}


__all__ = ["DdlHandling", "decode_canal_json"]
