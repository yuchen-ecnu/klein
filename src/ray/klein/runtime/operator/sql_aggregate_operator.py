# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import timedelta
from fractions import Fraction
from numbers import Rational, Real
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
from ray.klein.runtime.operator.sql_value_semantics import (
    compare_non_null_values,
    infinity_sign,
    is_nan_value,
    state_value_key,
)
from ray.klein.state.key_encoding import encode_key
from ray.klein.state.keyed_state_context import KeyedStateContext
from ray.klein.state.state_ttl_config import StateTTLConfig
from ray.klein.state.value_state_descriptor import ValueStateDescriptor

_ACCUMULATOR_VERSION = 1


@dataclass(frozen=True, slots=True)
class _SQLGroupKey:
    """Route equal SQL values together while retaining display values locally."""

    identity: bytes
    values: tuple[Any, ...] = field(compare=False, hash=False, repr=False)

    def __reduce__(self) -> tuple[Any, tuple[bytes]]:
        # Managed-state keys persist only the canonical identity. During normal
        # processing ``values`` come from the current row and remain available
        # for the GROUP BY projection.
        return (_restore_sql_group_key, (self.identity,))


def _restore_sql_group_key(identity: bytes) -> _SQLGroupKey:
    return _SQLGroupKey(identity, ())


class SQLGroupKeySelector:
    """Pickle-safe group-key evaluator shared by routing and managed state."""

    def __init__(self, expressions: Sequence[exp.Expression]) -> None:
        self._expressions = tuple(expressions)

    def __call__(self, row: Mapping[str, Any]) -> tuple[Any, ...] | _SQLGroupKey:
        if not self._expressions:
            return ("__klein_global_aggregate__",)
        values = tuple(evaluate_expression(expression, row) for expression in self._expressions)
        try:
            encode_key(values)
        except TypeError:
            # NaNs intentionally reject ordinary keyed-state encoding because
            # Python considers them unequal to themselves. SQL grouping needs
            # a stable equality class, while all previously supported keys keep
            # their legacy tuple representation for checkpoint compatibility.
            return _SQLGroupKey(state_value_key(values), values)
        return values


class SQLAggregateOperator(ManagedStateOperator):
    """Flink-style dynamic-table aggregation with incremental accumulators."""

    def __init__(
        self,
        logical_function: LogicalFunction | None = None,
        *,
        group_expressions: Sequence[exp.Expression],
        projections: Sequence[exp.Expression],
        state_ttl: timedelta | None = None,
        retractable: bool = True,
    ) -> None:
        self._group_expressions = tuple(group_expressions)
        self._projections = tuple(projections)
        self._aggregate_expressions = tuple(
            expression
            for projection in self._projections
            if isinstance(
                (expression := projection.this if isinstance(projection, exp.Alias) else projection), exp.AggFunc
            )
        )
        self._configured_state_ttl = state_ttl
        self._retractable = retractable
        self._aggregate_state = self._state_descriptor(state_ttl)
        super().__init__(logical_function, key_selector=SQLGroupKeySelector(self._group_expressions))

    @staticmethod
    def _state_descriptor(ttl: timedelta | None) -> ValueStateDescriptor[dict[str, Any]]:
        ttl_config = None if ttl is None else StateTTLConfig(ttl)
        # Preserve the durable state name used by the former row-list implementation.
        return ValueStateDescriptor("sql-group-rows", ttl_config=ttl_config)

    def open(self, collector: Collector, runtime_context: TaskRuntimeContext) -> None:
        state_ttl = self._configured_state_ttl or runtime_context.config.get(TableOptions.STATE_TTL)
        self._aggregate_state = self._state_descriptor(state_ttl)
        super().open(collector, runtime_context)

    def process_managed_element(self, record: Record, context: KeyedStateContext) -> None:
        if record.block is None:
            return
        state = context.state(self._aggregate_state)
        accumulator = self._restore_accumulator(state.value)
        previous_result = self._result(accumulator, context.current_key)
        row = dict(record.block)
        kind = row_kind_of(record.block)

        changed = self._apply_row(accumulator, row, addition=kind.is_addition)
        if not changed:
            return
        current_result = self._result(accumulator, context.current_key)
        if accumulator["row_count"]:
            state.value = accumulator
        else:
            state.clear()
        self._emit_change(previous_result, current_result)

    def _empty_accumulator(self) -> dict[str, Any]:
        aggregates: list[dict[str, Any]] = []
        for aggregate in self._aggregate_expressions:
            item: dict[str, Any] = {"count": 0}
            if isinstance(aggregate, (exp.Sum, exp.Avg)):
                item["sum"] = 0
                item["floating_sum"] = Fraction(0)
                item["floating_count"] = 0
                item["nan_count"] = 0
                item["nan_value"] = None
                item["positive_infinity_count"] = 0
                item["positive_infinity_value"] = None
                item["negative_infinity_count"] = 0
                item["negative_infinity_value"] = None
            elif isinstance(aggregate, (exp.Min, exp.Max)):
                item["values"] = {}
                item["current"] = None
                item["current_key"] = None
            aggregates.append(item)
        return {
            "version": _ACCUMULATOR_VERSION,
            "row_count": 0,
            "aggregates": aggregates,
            "row_counts": {} if self._retractable else None,
        }

    def _restore_accumulator(self, stored: Any) -> dict[str, Any]:
        if stored is None:
            return self._empty_accumulator()
        if isinstance(stored, dict) and stored.get("version") == _ACCUMULATOR_VERSION:
            self._upgrade_accumulator_fields(stored)
            return stored
        if isinstance(stored, list):
            # One-time online migration from checkpoints written by the row-list
            # implementation. Subsequent records persist the compact format.
            accumulator = self._empty_accumulator()
            for row in stored:
                self._apply_row(accumulator, dict(row), addition=True)
            return accumulator
        raise SQLQueryError("Unsupported streaming SQL aggregate state format")

    def _upgrade_accumulator_fields(self, accumulator: dict[str, Any]) -> None:
        """Fill fields added while the versioned accumulator was unreleased."""

        aggregates = accumulator.get("aggregates")
        if not isinstance(aggregates, list) or len(aggregates) != len(self._aggregate_expressions):
            raise SQLQueryError("Streaming SQL aggregate state does not match the query")
        for aggregate, item in zip(self._aggregate_expressions, aggregates, strict=True):
            if not isinstance(item, dict):
                raise SQLQueryError("Unsupported streaming SQL aggregate state format")
            if isinstance(aggregate, (exp.Sum, exp.Avg)):
                item.setdefault("floating_sum", Fraction(0))
                item.setdefault("floating_count", 0)
                for prefix in ("nan", "positive_infinity", "negative_infinity"):
                    item.setdefault(f"{prefix}_count", 0)
                    item.setdefault(f"{prefix}_value", None)
            elif isinstance(aggregate, (exp.Min, exp.Max)):
                current = item.get("current")
                item.setdefault("current_key", None if current is None else state_value_key(current))

    def _apply_row(self, accumulator: dict[str, Any], row: dict[str, Any], *, addition: bool) -> bool:
        if not self._accept_row(accumulator, row, addition=addition):
            return False
        direction = 1 if addition else -1
        accumulator["row_count"] += direction
        for aggregate, item in zip(self._aggregate_expressions, accumulator["aggregates"], strict=True):
            self._update_aggregate(aggregate, item, row, addition=addition, direction=direction)
        return True

    @staticmethod
    def _accept_row(accumulator: dict[str, Any], row: dict[str, Any], *, addition: bool) -> bool:
        row_counts: dict[bytes, int] | None = accumulator["row_counts"]
        if row_counts is None:
            if not addition:
                raise SQLQueryError("received a retraction from a stream declared as insert-only")
            return True
        row_key = state_value_key(row)
        existing = row_counts.get(row_key, 0)
        if not addition and existing == 0:
            # TTL may remove state before a late CDC retraction arrives.
            return False
        if addition:
            row_counts[row_key] = existing + 1
        elif existing == 1:
            row_counts.pop(row_key)
        else:
            row_counts[row_key] = existing - 1
        return True

    def _update_aggregate(
        self,
        aggregate: exp.AggFunc,
        item: dict[str, Any],
        row: dict[str, Any],
        *,
        addition: bool,
        direction: int,
    ) -> None:
        argument = aggregate.this
        if isinstance(aggregate, exp.Count) and (argument is None or isinstance(argument, exp.Star)):
            item["count"] += direction
            return
        if argument is None or isinstance(argument, exp.Star):
            raise SQLQueryError(f"{aggregate.key.upper()} requires an input expression")
        value = evaluate_expression(argument, row)
        if value is None:
            return
        item["count"] += direction
        if isinstance(aggregate, exp.Count):
            return
        if isinstance(aggregate, (exp.Sum, exp.Avg)):
            self._update_sum(item, value, addition=addition)
            return
        if isinstance(aggregate, (exp.Min, exp.Max)):
            self._update_extreme(item, value, addition=addition, minimum=isinstance(aggregate, exp.Min))
            return
        raise SQLQueryError(f"Unsupported streaming SQL aggregate {aggregate.key.upper()}")

    @staticmethod
    def _update_sum(item: dict[str, Any], value: Any, *, addition: bool) -> None:
        direction = 1 if addition else -1
        if is_nan_value(value):
            previous_count = item["nan_count"]
            item["nan_count"] += direction
            if addition and previous_count == 0:
                item["nan_value"] = value
            elif item["nan_count"] == 0:
                item["nan_value"] = None
            return
        sign = infinity_sign(value)
        if sign:
            prefix = "positive" if sign > 0 else "negative"
            count_key = f"{prefix}_infinity_count"
            value_key = f"{prefix}_infinity_value"
            item[count_key] += direction
            item[value_key] = value if item[count_key] else None
            return
        if isinstance(value, Real) and not isinstance(value, Rational):
            as_integer_ratio = getattr(value, "as_integer_ratio", None)
            if callable(as_integer_ratio):
                try:
                    ratio = Fraction(*as_integer_ratio())
                except (OverflowError, TypeError, ValueError):
                    pass
                else:
                    item["floating_count"] += direction
                    item["floating_sum"] += ratio * direction
                    return
        if addition:
            item["sum"] += value
        else:
            item["sum"] -= value

    @staticmethod
    def _update_extreme(item: dict[str, Any], value: Any, *, addition: bool, minimum: bool) -> None:
        values: dict[bytes, tuple[Any, int]] = item["values"]
        value_key = state_value_key(value)
        existing = values.get(value_key)
        if addition:
            values[value_key] = (value, 1 if existing is None else existing[1] + 1)
            current = item["current"]
            comparison = 0 if current is None else compare_non_null_values(value, current)
            if current is None or ((comparison < 0) if minimum else (comparison > 0)):
                item["current"] = value
                item["current_key"] = value_key
            return
        if existing is None:
            return
        if existing[1] > 1:
            values[value_key] = (existing[0], existing[1] - 1)
            return
        values.pop(value_key)
        if item["current_key"] == value_key:
            SQLAggregateOperator._replace_extreme(item, minimum=minimum)

    @staticmethod
    def _replace_extreme(item: dict[str, Any], *, minimum: bool) -> None:
        values: dict[bytes, tuple[Any, int]] = item["values"]
        if not values:
            item["current"] = None
            item["current_key"] = None
            return
        best_key, (best_value, _count) = next(iter(values.items()))
        for candidate_key, (candidate, _count) in values.items():
            comparison = compare_non_null_values(candidate, best_value)
            if (comparison < 0) if minimum else (comparison > 0):
                best_key, best_value = candidate_key, candidate
        item["current"] = best_value
        item["current_key"] = best_key

    def _result(
        self,
        accumulator: dict[str, Any],
        key: tuple[Any, ...] | _SQLGroupKey,
    ) -> dict[str, Any] | None:
        if accumulator["row_count"] == 0 and self._group_expressions:
            return None
        group_values = key.values if isinstance(key, _SQLGroupKey) else key
        group_lookup = {
            expression.sql(): group_values[index] for index, expression in enumerate(self._group_expressions)
        }
        result: dict[str, Any] = {}
        aggregate_index = 0
        for projection in self._projections:
            expression = projection.this if isinstance(projection, exp.Alias) else projection
            output_name = projection.alias_or_name or projection.sql()
            if output_name in result:
                raise SQLQueryError(f"Duplicate SQL output column {output_name!r}; add an explicit alias")
            if isinstance(expression, exp.AggFunc):
                result[output_name] = self._aggregate_value(expression, accumulator["aggregates"][aggregate_index])
                aggregate_index += 1
                continue
            try:
                result[output_name] = group_lookup[expression.sql()]
            except KeyError as error:
                raise SQLQueryError(f"Non-aggregate projection {expression.sql()!r} must appear in GROUP BY") from error
        return result

    @staticmethod
    def _aggregate_value(aggregate: exp.AggFunc, item: dict[str, Any]) -> Any:
        if isinstance(aggregate, exp.Count):
            return item["count"]
        if isinstance(aggregate, exp.Sum):
            return SQLAggregateOperator._sum_value(item) if item["count"] else None
        if isinstance(aggregate, exp.Avg):
            return SQLAggregateOperator._sum_value(item) / item["count"] if item["count"] else None
        if isinstance(aggregate, (exp.Min, exp.Max)):
            return item["current"]
        raise SQLQueryError(f"Unsupported streaming SQL aggregate {aggregate.key.upper()}")

    @staticmethod
    def _sum_value(item: dict[str, Any]) -> Any:
        if item["nan_count"]:
            return item["nan_value"]
        positive = item["positive_infinity_count"]
        negative = item["negative_infinity_count"]
        if positive and negative:
            try:
                return item["positive_infinity_value"] + item["negative_infinity_value"]
            except (ArithmeticError, TypeError, ValueError):
                return float("nan")
        if positive:
            return item["positive_infinity_value"]
        if negative:
            return item["negative_infinity_value"]
        if item["floating_count"]:
            combined = item["floating_sum"] + Fraction(item["sum"])
            try:
                return float(combined)
            except OverflowError:
                return float("inf") if combined > 0 else float("-inf")
        return item["sum"]

    def _emit_change(self, previous: dict[str, Any] | None, current: dict[str, Any] | None) -> None:
        if previous is current or (
            previous is not None and current is not None and state_value_key(previous) == state_value_key(current)
        ):
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
            "retractable": self._retractable,
        }
