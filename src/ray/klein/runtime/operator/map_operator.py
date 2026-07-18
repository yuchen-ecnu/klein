# SPDX-License-Identifier: Apache-2.0

from ray.klein.runtime.message import Record
from ray.klein.runtime.operator.operator import OneInputOperator, StreamOperator


class MapOperator(StreamOperator, OneInputOperator):
    """
    Operator to run a :class:`function.MapFunction`
    """

    def process_element(self, record: Record) -> None:
        self.collect(Record(self.callable_function(record.block)))

    async def process_async_element(self, record: Record) -> list[Record]:
        result = await self.callable_function(record.block)
        return [Record(result)]
