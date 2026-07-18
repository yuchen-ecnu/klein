# SPDX-License-Identifier: Apache-2.0
from ray.klein.runtime.message import Record
from ray.klein.runtime.operator.operator import OneInputOperator, StreamOperator


class KeyByOperator(StreamOperator, OneInputOperator):
    """Internal identity node that gives a keyed branch its own partition edge."""

    def process_element(self, record: Record) -> None:
        self.collect(record)
