# SPDX-License-Identifier: Apache-2.0
from typing import Any

from ray.klein._internal.block import create_possibly_ragged_ndarray
from ray.klein.api.collector import Collector
from ray.klein.observability.metrics.metric_catalog import KleinMetrics
from ray.klein.observability.metrics.metrics import Counter
from ray.klein.runtime.context.runtime_context import TaskRuntimeContext
from ray.klein.runtime.message import Record
from ray.klein.runtime.operator.operator import OneInputOperator, StreamOperator


class FilterOperator(StreamOperator, OneInputOperator):
    """
    Operator to run a :class:`function.FilterFunction`
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._filter_out_cnt_metric: Counter | None = None
        self._filter_total_cnt_metric: Counter | None = None

    def open(self, collector: Collector, runtime_context: TaskRuntimeContext) -> None:
        super().open(collector, runtime_context)
        self._filter_out_cnt_metric = self.metric_group.builtin_counter(KleinMetrics.FILTER_RECORDS_DROPPED)
        self._filter_total_cnt_metric = self.metric_group.builtin_counter(KleinMetrics.FILTER_RECORDS_IN)

    def process_element(self, record: Record) -> None:
        data = record.block
        if not self.runtime_info.batch_enabled:
            keep = self.callable_function(data)
            self._record_metrics((keep,))
            if keep:
                self.collect(Record(data))
            return
        decisions: list[bool] = self.callable_function(data)
        self._record_metrics(decisions)
        self.collect(self._filter_batch(data, decisions))

    async def process_async_element(self, record: Record) -> list[Record]:
        data = record.block
        if not self.runtime_info.batch_enabled:
            keep = await self.callable_function(data)
            self._record_metrics((keep,))
            return [Record(data)] if keep else []
        decisions: list[bool] = await self.callable_function(data)
        self._record_metrics(decisions)
        return [self._filter_batch(data, decisions)]

    def _record_metrics(self, decisions) -> None:
        if self._filter_total_cnt_metric is not None:
            self._filter_total_cnt_metric.inc(len(decisions))
        if self._filter_out_cnt_metric is not None:
            self._filter_out_cnt_metric.inc(sum(1 for keep in decisions if not keep))

    @staticmethod
    def _filter_batch(data: dict[str, Any], decisions: list[bool]) -> Record:
        filtered = {
            key: create_possibly_ragged_ndarray([item for item, keep in zip(values, decisions, strict=True) if keep])
            for key, values in data.items()
        }
        return Record(filtered)
