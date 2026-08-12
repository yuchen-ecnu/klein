# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import timedelta
from functools import cmp_to_key
from typing import Any

from sqlglot import exp

from ray.klein._internal.sql.expression import evaluate_expression
from ray.klein.api.changelog_row import ChangelogRow, row_kind_of
from ray.klein.api.collector import Collector
from ray.klein.api.functions.logical_function import LogicalFunction
from ray.klein.api.row_kind import RowKind
from ray.klein.api.sql_query_error import SQLQueryError
from ray.klein.config.table_options import TableOptions
from ray.klein.runtime.context.runtime_context import TaskRuntimeContext
from ray.klein.runtime.message import Record
from ray.klein.runtime.operator.managed_state_operator import ManagedStateOperator
from ray.klein.runtime.operator.sql_value_semantics import compare_non_null_values, state_value_key
from ray.klein.state.keyed_state_context import KeyedStateContext
from ray.klein.state.state_ttl_config import StateTTLConfig
from ray.klein.state.value_state_descriptor import ValueStateDescriptor

_TOP_N_STATE_VERSION = 1


def global_top_n_key(_row: Mapping[str, Any]) -> str:
    return "__klein_global_top_n__"


class SQLTopNOperator(ManagedStateOperator):
    """Maintain streaming ORDER BY/LIMIT with an incrementally sorted index."""

    def __init__(
        self,
        logical_function: LogicalFunction | None = None,
        *,
        order: Sequence[exp.Ordered],
        limit: int,
        state_ttl: timedelta | None = None,
        retractable: bool = True,
    ) -> None:
        if limit < 0:
            raise ValueError("Top-N limit must be non-negative")
        self._order = tuple(order)
        self._limit = limit
        self._configured_state_ttl = state_ttl
        self._retractable = retractable
        self._rows_state = self._state_descriptor(state_ttl)
        super().__init__(logical_function, key_selector=global_top_n_key)

    @staticmethod
    def _state_descriptor(ttl: timedelta | None) -> ValueStateDescriptor[dict[str, Any]]:
        ttl_config = None if ttl is None else StateTTLConfig(ttl)
        return ValueStateDescriptor("sql-top-n-rows", ttl_config=ttl_config)

    def open(self, collector: Collector, runtime_context: TaskRuntimeContext) -> None:
        state_ttl = self._configured_state_ttl or runtime_context.config.get(TableOptions.STATE_TTL)
        self._rows_state = self._state_descriptor(state_ttl)
        super().open(collector, runtime_context)

    def process_managed_element(self, record: Record, context: KeyedStateContext) -> None:
        if record.block is None:
            return
        if self._limit == 0:
            return
        state = context.state(self._rows_state)
        rows = self._restore_rows(state.value)

        previous = list(rows[: self._limit])
        row = dict(record.block)
        if row_kind_of(record.block).is_addition:
            self._insert_sorted(rows, row)
            if not self._retractable and len(rows) > self._limit:
                rows.pop()
        else:
            if not self._retractable:
                raise SQLQueryError("received a retraction from a stream declared as insert-only")
            if not self._retract(rows, row):
                return
        current = list(rows[: self._limit])
        if rows:
            state.value = {"version": _TOP_N_STATE_VERSION, "rows": rows}
        else:
            state.clear()
        self._emit_diff(previous, current)

    def _restore_rows(self, stored: Any) -> list[dict[str, Any]]:
        if stored is None:
            rows: list[dict[str, Any]] = []
        elif isinstance(stored, list):
            rows = sorted(stored, key=cmp_to_key(self._compare))
        elif (
            isinstance(stored, dict)
            and stored.get("version") == _TOP_N_STATE_VERSION
            and isinstance(stored.get("rows"), list)
        ):
            rows = stored["rows"]
        else:
            raise SQLQueryError("Unsupported streaming SQL Top-N state format")

        if not self._retractable and len(rows) > self._limit:
            del rows[self._limit :]
        return rows

    def _insert_sorted(self, rows: list[dict[str, Any]], row: dict[str, Any]) -> None:
        low, high = 0, len(rows)
        # Insert after equal values, matching Python's stable sorted() semantics.
        while low < high:
            middle = (low + high) // 2
            if self._compare(row, rows[middle]) < 0:
                high = middle
            else:
                low = middle + 1
        rows.insert(low, row)

    @staticmethod
    def _retract(rows: list[dict[str, Any]], row: dict[str, Any]) -> bool:
        row_key = state_value_key(row)
        for index, existing in enumerate(rows):
            if state_value_key(existing) == row_key:
                del rows[index]
                return True
        # State TTL can remove a row before a late CDC retraction arrives.
        return False

    def _compare(self, left: Mapping[str, Any], right: Mapping[str, Any]) -> int:
        for ordered in self._order:
            left_value = evaluate_expression(ordered.this, left)
            right_value = evaluate_expression(ordered.this, right)
            comparison = self._compare_values(
                left_value,
                right_value,
                nulls_first=bool(ordered.args.get("nulls_first")),
            )
            if comparison:
                if left_value is None or right_value is None:
                    return comparison
                return -comparison if ordered.args.get("desc") else comparison
        return 0

    @staticmethod
    def _compare_values(left: Any, right: Any, *, nulls_first: bool) -> int:
        if left is None or right is None:
            if left is right:
                return 0
            if left is None:
                return -1 if nulls_first else 1
            return 1 if nulls_first else -1
        return compare_non_null_values(left, right)

    def _emit_diff(self, previous: list[dict[str, Any]], current: list[dict[str, Any]]) -> None:
        current_counts = SQLTopNOperator._row_counts(current)
        removed: list[dict[str, Any]] = []
        for row in previous:
            key = state_value_key(row)
            if current_counts.get(key, 0):
                current_counts[key] -= 1
            else:
                removed.append(row)
        previous_counts = SQLTopNOperator._row_counts(previous)
        added: list[dict[str, Any]] = []
        for row in current:
            key = state_value_key(row)
            if previous_counts.get(key, 0):
                previous_counts[key] -= 1
            else:
                added.append(row)
        for row in removed:
            self.collect(Record(ChangelogRow(row, row_kind=RowKind.DELETE)))
        for row in added:
            self.collect(Record(ChangelogRow(row, row_kind=RowKind.INSERT)))

    @staticmethod
    def _row_counts(rows: Sequence[dict[str, Any]]) -> dict[bytes, int]:
        counts: dict[bytes, int] = {}
        for row in rows:
            key = state_value_key(row)
            counts[key] = counts.get(key, 0) + 1
        return counts

    def _spec_parameters(self) -> dict[str, Any]:
        return {
            "order": self._order,
            "limit": self._limit,
            "state_ttl": self._configured_state_ttl,
            "retractable": self._retractable,
        }
