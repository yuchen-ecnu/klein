# SPDX-License-Identifier: Apache-2.0
from typing import Any

from ray.klein._internal.logging import get_logger
from ray.klein.api.collector import Collector
from ray.klein.api.functions.logical_function import LogicalFunction
from ray.klein.api.missing_data_strategy import MissingDataStrategy
from ray.klein.runtime.context.runtime_context import TaskRuntimeContext
from ray.klein.runtime.message import Record
from ray.klein.runtime.operator.operator import OneInputOperator, StreamOperator
from ray.klein.runtime.operator.rank_fields import INNER_ID, INNER_RANK

logger = get_logger(__name__)


class FlatMapWithRankOperator(StreamOperator, OneInputOperator):
    """
    FlatMap-like operator to run a :class:`function.FlatMapFunction` and add rank into result
    For example:
        1. input data: {"id": "mock-data", "values": [1, 2, 3]}
        2. func = lambda d: [{"id": d["id"], "val": value} for value in d["values"]]
        3. result data =
            [{"id": "mock-data", "val": 1, "__rank__": (0, 3), "__id__": (1, 1)},
             {"id": "mock-data", "val": 2, "__rank__": (1, 3), "__id__": (1, 1)},
             {"id": "mock-data", "val": 3, "__rank__": (2, 3), "__id__": (1, 1)}]
    """

    def __init__(
        self,
        logical_function: LogicalFunction,
        missing_data_strategy: MissingDataStrategy = MissingDataStrategy.ERROR,
    ) -> None:
        super().__init__(logical_function)
        self._missing_data_strategy = missing_data_strategy
        self._inner_id = 0
        self._task_index: int | None = None

    def _spec_parameters(self) -> dict[str, Any]:
        return {"missing_data_strategy": self._missing_data_strategy}

    def open(self, collector: Collector, runtime_context: TaskRuntimeContext) -> None:
        super().open(collector, runtime_context)
        self._task_index = runtime_context.task_index
        self._inner_id = 0

    def process_element(self, record: Record) -> None:
        processed_datas = list(self.callable_function(record.block))
        if not processed_datas:
            if self._missing_data_strategy == MissingDataStrategy.WARNING:
                logger.warning(
                    "Preprocessing produced no output; dropping the input record",
                )
            elif self._missing_data_strategy == MissingDataStrategy.ERROR:
                raise ValueError(f"record [{record}] will be dropped because no data will output after preprocess.")
        self._inner_id = (self._inner_id + 1) % 2147483648
        for rank, row in enumerate(processed_datas):
            row.update(
                {
                    INNER_RANK: (rank + 1, len(processed_datas)),
                    INNER_ID: (self._task_index, self._inner_id),
                }
            )
            self.collect(Record(row))
