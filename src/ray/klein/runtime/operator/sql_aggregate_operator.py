# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import timedelta
from typing import Any

from sqlglot import exp

from ray.klein._internal.sql.expression import evaluate_expression
from ray.klein.api.changelog_row import ChangelogRow, row_kind_of
from ray.klein.api.collector import Collector
from ray.klein.api.row_kind import RowKind
from ray.klein.api.sql_query_error import SQLQueryError
from ray.klein.config.table_options import TableOptions
from ray.klein.runtime.context.runtime_context import TaskRuntimeContext
from ray.klein.runtime.message import Record
from ray.klein.runtime.operator.managed_state_operator import ManagedStateOperator
from ray.klein.state.keyed_state_context import KeyedStateContext
from ray.klein.state.list_state_descriptor import ListStateDescriptor
from ray.klein.state.state_ttl_config import StateTTLConfig


class SQLGroupKeySelector:
    """Pickle-safe group-key evaluator shared by routing and managed state."""

    def __init__(self, expressions: Sequence[exp.Expression]) -> None:
        self._expressions = tuple(expressions)

    def __call__(self, row: Mapping[str, Any]) -> tuple[Any, ...]:
        if not self._expressions:
            return ("__klein_global_aggregate__",)
        return tuple(evaluate_expression(expression, row) for expression in self._expressions)


class SQLAggregateOperator(ManagedStateOperator):
    """Flink-style dynamic-table aggregation with retract support."""

    def __init__(
        self,
        logical_function=None,
        *,
        group_expressions: Sequence[exp.Expression],
        projections: Sequence[exp.Expression],
        state_ttl: timedelta | None = None,
    ) -> None:
        self._group_expressions = tuple(group_expressions)
        self._projections = tuple(projections)
        self._configured_state_ttl = state_ttl
        self._rows_state = self._state_descriptor(state_ttl)
        super().__init__(logical_function, key_selector=SQLGroupKeySelector(self._group_expressions))

    @staticmethod
    def _state_descriptor(ttl: timedelta | None) -> ListStateDescriptor[dict[str, Any]]:
        ttl_config = None if ttl is None else StateTTLConfig(ttl)
        return ListStateDescriptor("sql-group-rows", ttl_config=ttl_config)

    def open(self, collector: Collector, runtime_context: TaskRuntimeContext) -> None:
        state_ttl = self._configured_state_ttl or runtime_context.config.get(TableOptions.STATE_TTL)
        self._rows_state = self._state_descriptor(state_ttl)
        super().open(collector, runtime_context)

    def process_managed_element(self, record: Record, context: KeyedStateContext) -> None:
        if record.block is None:
            return
        state = context.state(self._rows_state)
        previous_rows = list(state)
        previous_result = self._aggregate(previous_rows, context.current_key)
        row = dict(record.block)
        kind = row_kind_of(record.block)

        if kind.is_addition:
            state.append(row)
        else:
            self._retract(state, row)

        current_rows = list(state)
        current_result = self._aggregate(current_rows, context.current_key)
        self._emit_change(previous_result, current_result)

    @staticmethod
    def _retract(state, row: dict[str, Any]) -> None:
        for index, existing in enumerate(state):
            if existing == row:
                del state[index]
                return
        # State TTL can legitimately remove a row before a late CDC retraction
        # arrives. Flink documents that TTL may make query results incomplete;
        # the runtime must not fail the whole job for that expected condition.

    def _aggregate(self, rows: list[dict[str, Any]], key: tuple[Any, ...]) -> dict[str, Any] | None:
        if not rows and self._group_expressions:
            return None
        group_lookup = {expression.sql(): key[index] for index, expression in enumerate(self._group_expressions)}
        result: dict[str, Any] = {}
        for projection in self._projections:
            expression = projection.this if isinstance(projection, exp.Alias) else projection
            output_name = projection.alias_or_name or projection.sql()
            if output_name in result:
                raise SQLQueryError(f"Duplicate SQL output column {output_name!r}; add an explicit alias")
            if isinstance(expression, exp.AggFunc):
                result[output_name] = self._aggregate_value(expression, rows)
                continue
            try:
                result[output_name] = group_lookup[expression.sql()]
            except KeyError as error:
                raise SQLQueryError(f"Non-aggregate projection {expression.sql()!r} must appear in GROUP BY") from error
        return result

    @staticmethod
    def _aggregate_value(aggregate: exp.AggFunc, rows: list[dict[str, Any]]) -> Any:
        argument = aggregate.this
        if isinstance(aggregate, exp.Count):
            if argument is None or isinstance(argument, exp.Star):
                return len(rows)
            return sum(evaluate_expression(argument, row) is not None for row in rows)
        if argument is None or isinstance(argument, exp.Star):
            raise SQLQueryError(f"{aggregate.key.upper()} requires an input expression")
        values = [evaluate_expression(argument, row) for row in rows]
        non_null = [value for value in values if value is not None]
        if isinstance(aggregate, exp.Sum):
            return sum(non_null) if non_null else None
        if isinstance(aggregate, exp.Min):
            return min(non_null) if non_null else None
        if isinstance(aggregate, exp.Max):
            return max(non_null) if non_null else None
        if isinstance(aggregate, exp.Avg):
            return sum(non_null) / len(non_null) if non_null else None
        raise SQLQueryError(f"Unsupported streaming SQL aggregate {aggregate.key.upper()}")

    def _emit_change(self, previous: dict[str, Any] | None, current: dict[str, Any] | None) -> None:
        if previous == current:
            return
        if previous is None and current is not None:
            self.collect(Record(ChangelogRow(current, row_kind=RowKind.INSERT)))
            return
        if previous is not None and current is None:
            self.collect(Record(ChangelogRow(previous, row_kind=RowKind.DELETE)))
            return
        self.collect(Record(ChangelogRow(previous, row_kind=RowKind.UPDATE_BEFORE)))
        self.collect(Record(ChangelogRow(current, row_kind=RowKind.UPDATE_AFTER)))

    def _spec_parameters(self) -> dict[str, Any]:
        return {
            "group_expressions": self._group_expressions,
            "projections": self._projections,
            "state_ttl": self._configured_state_ttl,
        }
