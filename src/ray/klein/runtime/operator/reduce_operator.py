# SPDX-License-Identifier: Apache-2.0
from collections.abc import Callable
from typing import Any

from ray.klein.api.functions.logical_function import LogicalFunction
from ray.klein.runtime.message import Record
from ray.klein.runtime.operator.operator import OneInputOperator, StreamOperator
from ray.klein.runtime.operator.operator_type import OperatorType
from ray.klein.runtime.operator.rank_fields import INNER_ID, INNER_RANK


class ReduceOperator(StreamOperator, OneInputOperator):
    """
    Operator to run a :class:`function.ReduceFunction`
    """

    def __init__(
        self,
        logical_function: LogicalFunction,
        key_selector: Callable[[Any], Any] | None = None,
    ) -> None:
        super().__init__(logical_function)
        self._key_selector = key_selector
        self._key_records: dict[Any, list[dict[str, Any]]] = {}

    def _spec_parameters(self) -> dict[str, Any]:
        return {"key_selector": self._key_selector}

    def process_element(self, record: Record) -> None:
        current_key = self._key_selector(record.block)
        data = record.block
        inner_id = data.pop(INNER_ID)
        key_id = f"{current_key}_{inner_id[0]}_{inner_id[1]}"
        if key_id not in self._key_records:
            self._key_records[key_id] = []
        self._key_records[key_id].append(data)
        if len(self._key_records[key_id]) == data[INNER_RANK][1]:
            records = self._key_records.pop(key_id)
            sorted_records = sorted(records, key=lambda item: item[INNER_RANK][0])
            column_keys = list(sorted_records[0].keys())
            column_keys.remove(INNER_RANK)
            process_data = {column: [item[column] for item in sorted_records] for column in column_keys}
            self.collect(Record(self.callable_function(process_data)))

    @property
    def operator_type(self) -> OperatorType:
        return OperatorType.REDUCE

    def close(self) -> None:
        super().close()
        self._key_records.clear()
