# SPDX-License-Identifier: Apache-2.0
from ray.klein.runtime.message import Record
from ray.klein.runtime.operator.operator import OneInputOperator, StreamOperator
from ray.klein.runtime.operator.rank_fields import INNER_ID, INNER_RANK


class BatchProcessOperator(StreamOperator, OneInputOperator):
    """
    Operator to run a :class:`function.ReduceFunction`
    """

    def process_element(self, record: Record) -> None:
        data = record.block
        ranks = data.pop(INNER_RANK)
        ids = data.pop(INNER_ID)
        infer_results = self.callable_function(data)
        infer_results.update({INNER_RANK: ranks, INNER_ID: ids})
        self.collect(Record(infer_results))
