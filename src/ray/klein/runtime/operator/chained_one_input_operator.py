# SPDX-License-Identifier: Apache-2.0
from ray.klein.runtime.message import Record
from ray.klein.runtime.operator.chained_operator import ChainedOperator
from ray.klein.runtime.operator.operator import OneInputOperator


class ChainedOneInputOperator(ChainedOperator, OneInputOperator):
    def process_element(self, record: Record) -> None:
        self._root_operator.invoke_process(record)
