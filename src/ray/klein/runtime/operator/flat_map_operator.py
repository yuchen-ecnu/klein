# SPDX-License-Identifier: Apache-2.0

from ray.klein.runtime.message import Record
from ray.klein.runtime.operator.operator import OneInputOperator, StreamOperator


class FlatMapOperator(StreamOperator, OneInputOperator):
    """
    Operator to run a :class:`function.FlatMapFunction`
    """

    def process_element(self, record: Record) -> None:
        for row in self.callable_function(record.block):
            self.collect(Record(row))

    async def process_async_element(self, record: Record) -> list[Record]:
        return [Record(row) for row in await self.callable_function(record.block)]
