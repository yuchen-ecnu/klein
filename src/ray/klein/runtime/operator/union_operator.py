# SPDX-License-Identifier: Apache-2.0

from ray.klein.api.functions.logical_function import LogicalFunction
from ray.klein.runtime.message import Record
from ray.klein.runtime.operator.operator import OneInputOperator, StreamOperator


class UnionOperator(StreamOperator, OneInputOperator):
    """Operator for union operation"""

    def __init__(self, logical_function: LogicalFunction) -> None:
        super().__init__(logical_function=logical_function)

    def process_element(self, record: Record) -> None:
        self.collect(record)

    async def process_async_element(self, record: Record) -> list[Record]:
        return [record]
