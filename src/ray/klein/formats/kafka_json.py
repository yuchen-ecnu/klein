# SPDX-License-Identifier: Apache-2.0
"""Decode ordinary JSON values from the raw Kafka record envelope."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

_OPTION_NAMES = frozenset({"include_metadata", "metadata_prefix"})
_METADATA_FIELDS = ("topic", "partition", "offset", "timestamp", "key", "headers")


def decode_kafka_json(
    record: Mapping[str, Any],
    *,
    include_metadata: bool = False,
    metadata_prefix: str = "__kafka_",
) -> dict[str, Any]:
    """Decode a Kafka record's UTF-8 JSON object value.

    When requested, Kafka envelope fields are added with ``metadata_prefix``;
    prefixed metadata wins over colliding JSON object keys.
    """

    options = _normalize_kafka_json_options(
        {
            "include_metadata": include_metadata,
            "metadata_prefix": metadata_prefix,
        }
    )
    if not isinstance(record, Mapping):
        raise TypeError("Kafka JSON input must be a record mapping")
    if "value" not in record:
        raise ValueError("Kafka JSON input requires a 'value' field")
    payload = record["value"]
    if isinstance(payload, str):
        text = payload
    elif isinstance(payload, bytes | bytearray | memoryview):
        try:
            text = bytes(payload).decode("utf-8")
        except UnicodeDecodeError as error:
            raise ValueError("Kafka value is not valid UTF-8") from error
    else:
        raise TypeError("Kafka JSON value must be bytes or a string")
    try:
        value = json.loads(text)
    except json.JSONDecodeError as error:
        raise ValueError("Kafka value is not valid JSON") from error
    if not isinstance(value, Mapping):
        raise ValueError("Kafka JSON value must be an object")

    decoded = dict(value)
    if options["include_metadata"]:
        prefix = options["metadata_prefix"]
        decoded.update({f"{prefix}{field}": record.get(field) for field in _METADATA_FIELDS})
    return decoded


def _normalize_kafka_json_options(options: Mapping[str, Any] | None) -> dict[str, Any]:
    if options is None:
        options = {}
    if not isinstance(options, Mapping):
        raise TypeError("format_options must be a mapping")
    unknown = sorted(set(options) - _OPTION_NAMES)
    if unknown:
        raise ValueError(f"Unsupported json format option(s): {', '.join(unknown)}")
    include_metadata = options.get("include_metadata", False)
    metadata_prefix = options.get("metadata_prefix", "__kafka_")
    if not isinstance(include_metadata, bool):
        raise TypeError("json include_metadata must be a boolean")
    if not isinstance(metadata_prefix, str) or not metadata_prefix:
        raise ValueError("json metadata_prefix must be a non-empty string")
    return {
        "include_metadata": include_metadata,
        "metadata_prefix": metadata_prefix,
    }


class KafkaJSONDecoder:
    """Pickle-friendly callable used by batch and streaming map operators."""

    def __init__(
        self,
        *,
        include_metadata: bool = False,
        metadata_prefix: str = "__kafka_",
    ) -> None:
        self._options = _normalize_kafka_json_options(
            {
                "include_metadata": include_metadata,
                "metadata_prefix": metadata_prefix,
            }
        )

    def __call__(self, record: Mapping[str, Any]) -> dict[str, Any]:
        return decode_kafka_json(record, **self._options)


__all__ = ["decode_kafka_json"]
