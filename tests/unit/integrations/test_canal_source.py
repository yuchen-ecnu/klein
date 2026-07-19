# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import json
from typing import Any

import pytest

from ray.klein.api.changelog_row import ChangelogRow
from ray.klein.api.row_kind import RowKind
from ray.klein.formats import decode_canal_json
from ray.klein.integrations.kafka import KafkaSource


def _payload(event_type: str, **overrides: Any) -> bytes:
    message = {
        "id": 42,
        "database": "shop",
        "table": "orders",
        "pkNames": ["id"],
        "isDdl": False,
        "type": event_type,
        "es": 1_700_000_000_000,
        "ts": 1_700_000_000_100,
        "sql": "",
        "sqlType": {"id": 4, "status": 12},
        "mysqlType": {"id": "bigint", "status": "varchar(32)"},
        "data": [],
        "old": None,
        "gtid": "gtid-1",
        **overrides,
    }
    return json.dumps(message).encode()


def _changes(rows: list[ChangelogRow]) -> list[tuple[RowKind, dict[str, Any]]]:
    return [(row.row_kind, dict(row)) for row in rows]


def test_insert_and_delete_flat_messages_become_changelog_rows() -> None:
    inserted = decode_canal_json(_payload("INSERT", data=[{"id": "1", "status": "new"}]))
    deleted = decode_canal_json(_payload("DELETE", data=[{"id": "1", "status": "paid"}]))

    assert inserted[0].row_kind is RowKind.INSERT
    assert inserted[0]["id"] == "1"
    assert inserted[0]["__canal_database"] == "shop"
    assert inserted[0]["__canal_table"] == "orders"
    assert inserted[0]["__canal_event_type"] == "INSERT"
    assert inserted[0]["__canal_pk_names"] == ["id"]
    assert deleted[0].row_kind is RowKind.DELETE


def test_update_reconstructs_before_image_from_partial_old_columns() -> None:
    rows = decode_canal_json(
        _payload(
            "UPDATE",
            data=[{"id": "1", "status": "paid", "note": None}],
            old=[{"status": "new"}],
        ),
        include_metadata=False,
    )

    assert _changes(rows) == [
        (RowKind.UPDATE_BEFORE, {"id": "1", "status": "new", "note": None}),
        (RowKind.UPDATE_AFTER, {"id": "1", "status": "paid", "note": None}),
    ]


def test_update_requires_aligned_old_images() -> None:
    with pytest.raises(ValueError, match="row counts must match"):
        decode_canal_json(
            _payload("UPDATE", data=[{"id": "1"}], old=[]),
            include_metadata=False,
        )


def test_ddl_handling_is_explicit() -> None:
    payload = _payload("ALTER", isDdl=True, sql="ALTER TABLE orders ADD note TEXT", data=None)

    assert decode_canal_json(payload) == []
    emitted = decode_canal_json(payload, ddl_handling="emit")
    assert _changes(emitted) == [
        (
            RowKind.INSERT,
            {
                "__canal_id": 42,
                "__canal_database": "shop",
                "__canal_table": "orders",
                "__canal_event_type": "ALTER",
                "__canal_execute_time": 1_700_000_000_000,
                "__canal_build_time": 1_700_000_000_100,
                "__canal_gtid": "gtid-1",
                "__canal_pk_names": ["id"],
                "__canal_is_ddl": True,
                "__canal_sql": "ALTER TABLE orders ADD note TEXT",
            },
        )
    ]
    with pytest.raises(ValueError, match="DDL event"):
        decode_canal_json(payload, ddl_handling="fail")


@pytest.mark.parametrize("payload", [b"not-json", b"[]", b'{"type": "UPDATE", "data": []}'])
def test_malformed_messages_fail_fast(payload: bytes) -> None:
    with pytest.raises(ValueError):
        decode_canal_json(payload)


class _Message:
    def __init__(self, value: bytes, *, offset: int = 5) -> None:
        self._value = value
        self._offset = offset

    def error(self):
        return None

    def topic(self) -> str:
        return "canal-orders"

    def partition(self) -> int:
        return 0

    def offset(self) -> int:
        return self._offset

    def value(self) -> bytes:
        return self._value


def test_checkpoint_resumes_inside_multirow_kafka_message() -> None:
    message = _Message(
        _payload(
            "UPDATE",
            data=[{"id": "1", "status": "paid"}],
            old=[{"status": "new"}],
        )
    )
    source = KafkaSource(
        "canal-orders",
        bootstrap_servers="broker:9092",
        value_format="canal-json",
        format_options={"include_metadata": False},
    )

    class _FirstContext:
        def __init__(self) -> None:
            self.rows: list[ChangelogRow] = []
            self.state: dict[str, Any] | None = None

        def collect(self, row: ChangelogRow) -> None:
            self.rows.append(row)
            self.state = source.snapshot_state(7)
            source.cancel()

    first = _FirstContext()
    assert source._emit_messages(first, [message]) == 1
    assert _changes(first.rows) == [(RowKind.UPDATE_BEFORE, {"id": "1", "status": "new"})]
    assert first.state == {
        "version": 1,
        "positions": {"canal-orders": {0: 5}},
        "value_format": "canal-json",
        "format_inflight": {"canal-orders": {0: {"offset": 5, "next_index": 1}}},
    }

    restored = KafkaSource(
        "canal-orders",
        bootstrap_servers="broker:9092",
        value_format="canal-json",
        format_options={"include_metadata": False},
    )
    restored.restore_state(first.state)

    class _RestoredContext:
        def __init__(self) -> None:
            self.rows: list[ChangelogRow] = []

        def collect(self, row: ChangelogRow) -> None:
            self.rows.append(row)

    second = _RestoredContext()
    assert restored._emit_messages(second, [message]) == 1
    assert _changes(second.rows) == [(RowKind.UPDATE_AFTER, {"id": "1", "status": "paid"})]
    assert restored.snapshot_state(8) == {
        "version": 1,
        "positions": {"canal-orders": {0: 6}},
        "value_format": "canal-json",
        "format_inflight": {},
    }


def test_checkpoint_rejects_a_different_configured_value_format() -> None:
    source = KafkaSource("events", bootstrap_servers="broker:9092")

    with pytest.raises(ValueError, match="does not match source format"):
        source.restore_state(
            {
                "version": 1,
                "positions": {"events": {0: 1}},
                "value_format": "canal-json",
                "format_inflight": {},
            }
        )
