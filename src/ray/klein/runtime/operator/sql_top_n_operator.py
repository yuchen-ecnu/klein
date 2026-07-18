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
from ray.klein.api.row_kind import RowKind
from ray.klein.config.table_options import TableOptions
from ray.klein.runtime.context.runtime_context import TaskRuntimeContext
from ray.klein.runtime.message import Record
from ray.klein.runtime.operator.managed_state_operator import ManagedStateOperator
from ray.klein.state.keyed_state_context import KeyedStateContext
from ray.klein.state.list_state_descriptor import ListStateDescriptor
from ray.klein.state.state_ttl_config import StateTTLConfig


def global_top_n_key(_row: Mapping[str, Any]) -> str:
    return "__klein_global_top_n__"


class SQLTopNOperator(ManagedStateOperator):
    """Maintain Flink's streaming ORDER BY ... LIMIT result as a retract stream."""

    def __init__(
        self,
        logical_function=None,
        *,
        order: Sequence[exp.Ordered],
        limit: int,
        state_ttl: timedelta | None = None,
    ) -> None:
        if limit < 0:
            raise ValueError("Top-N limit must be non-negative")
        self._order = tuple(order)
        self._limit = limit
        self._configured_state_ttl = state_ttl
        self._rows_state = self._state_descriptor(state_ttl)
        super().__init__(logical_function, key_selector=global_top_n_key)

    @staticmethod
    def _state_descriptor(ttl: timedelta | None) -> ListStateDescriptor[dict[str, Any]]:
        ttl_config = None if ttl is None else StateTTLConfig(ttl)
        return ListStateDescriptor("sql-top-n-rows", ttl_config=ttl_config)

    def open(self, collector: Collector, runtime_context: TaskRuntimeContext) -> None:
        state_ttl = self._configured_state_ttl or runtime_context.config.get(TableOptions.STATE_TTL)
        self._rows_state = self._state_descriptor(state_ttl)
        super().open(collector, runtime_context)

    def process_managed_element(self, record: Record, context: KeyedStateContext) -> None:
        if record.block is None:
            return
        state = context.state(self._rows_state)
        previous = self._top_n(list(state))
        row = dict(record.block)
        if row_kind_of(record.block).is_addition:
            state.append(row)
        else:
            self._retract(state, row)
        current = self._top_n(list(state))
        self._emit_diff(previous, current)

    @staticmethod
    def _retract(state, row: dict[str, Any]) -> None:
        for index, existing in enumerate(state):
            if existing == row:
                del state[index]
                return

    def _top_n(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return sorted(rows, key=cmp_to_key(self._compare))[: self._limit]

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
        return (left > right) - (left < right)

    def _emit_diff(self, previous: list[dict[str, Any]], current: list[dict[str, Any]]) -> None:
        remaining_current = list(current)
        removed: list[dict[str, Any]] = []
        for row in previous:
            try:
                remaining_current.remove(row)
            except ValueError:
                removed.append(row)
        remaining_previous = list(previous)
        added: list[dict[str, Any]] = []
        for row in current:
            try:
                remaining_previous.remove(row)
            except ValueError:
                added.append(row)
        for row in removed:
            self.collect(Record(ChangelogRow(row, row_kind=RowKind.DELETE)))
        for row in added:
            self.collect(Record(ChangelogRow(row, row_kind=RowKind.INSERT)))

    def _spec_parameters(self) -> dict[str, Any]:
        return {
            "order": self._order,
            "limit": self._limit,
            "state_ttl": self._configured_state_ttl,
        }
