# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import timedelta
from typing import Any

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


class SQLJoinKeySelector:
    """Select a regular-join key from a tagged left or right row."""

    def __init__(self, left_keys: Sequence[str], right_keys: Sequence[str]) -> None:
        self._left_keys = tuple(left_keys)
        self._right_keys = tuple(right_keys)

    def __call__(self, row: Mapping[str, Any]) -> tuple[Any, ...]:
        raise RuntimeError("SQLJoinKeySelector requires the record input tag")

    def for_record(self, record: Record) -> tuple[Any, ...]:
        keys = self._left_keys if record.input_tag == 0 else self._right_keys
        return tuple(record.block[key] for key in keys)


def _unused_row_key(row: Mapping[str, Any]) -> Mapping[str, Any]:
    return row


class SQLRegularJoinOperator(ManagedStateOperator):
    """Checkpointed Flink-style regular INNER JOIN over two dynamic tables."""

    def __init__(
        self,
        logical_function=None,
        *,
        left_keys: Sequence[str],
        right_keys: Sequence[str],
        left_state_ttl: timedelta | None = None,
        right_state_ttl: timedelta | None = None,
    ) -> None:
        self._key_selector_by_side = SQLJoinKeySelector(left_keys, right_keys)
        self._left_keys = tuple(left_keys)
        self._right_keys = tuple(right_keys)
        self._configured_left_ttl = left_state_ttl
        self._configured_right_ttl = right_state_ttl
        self._left_state = self._state_descriptor("sql-join-left", left_state_ttl)
        self._right_state = self._state_descriptor("sql-join-right", right_state_ttl)
        # ManagedStateOperator calls _key_and_timestamp below, where the input
        # tag is available. Its nominal selector is never used directly.
        super().__init__(logical_function, key_selector=_unused_row_key)

    @staticmethod
    def _state_descriptor(name: str, ttl: timedelta | None) -> ListStateDescriptor[dict[str, Any]]:
        ttl_config = None if ttl is None else StateTTLConfig(ttl)
        return ListStateDescriptor(name, ttl_config=ttl_config)

    def open(self, collector: Collector, runtime_context: TaskRuntimeContext) -> None:
        default_ttl = runtime_context.config.get(TableOptions.STATE_TTL)
        self._left_state = self._state_descriptor("sql-join-left", self._configured_left_ttl or default_ttl)
        self._right_state = self._state_descriptor("sql-join-right", self._configured_right_ttl or default_ttl)
        super().open(collector, runtime_context)

    def _key_and_timestamp(self, record: Record) -> tuple[Any, int | None]:
        if record.input_tag not in {0, 1}:
            raise ValueError("streaming SQL join record is missing its left/right input tag")
        return self._key_selector_by_side.for_record(record), record.timestamp

    def process_managed_element(self, record: Record, context: KeyedStateContext) -> None:
        if record.block is None:
            return
        is_left = record.input_tag == 0
        own = context.state(self._left_state if is_left else self._right_state)
        other = context.state(self._right_state if is_left else self._left_state)
        row = dict(record.block)
        kind = row_kind_of(record.block)
        output_kind = RowKind.INSERT if kind.is_addition else RowKind.DELETE

        for counterpart in other:
            left, right = (row, counterpart) if is_left else (counterpart, row)
            self.collect(Record(ChangelogRow({**left, **right}, row_kind=output_kind)))

        if kind.is_addition:
            own.append(row)
        else:
            self._retract(own, row)

    @staticmethod
    def _retract(state, row: dict[str, Any]) -> None:
        for index, existing in enumerate(state):
            if existing == row:
                del state[index]
                return

    def _spec_parameters(self) -> dict[str, Any]:
        return {
            "left_keys": self._left_keys,
            "right_keys": self._right_keys,
            "left_state_ttl": self._configured_left_ttl,
            "right_state_ttl": self._configured_right_ttl,
        }
