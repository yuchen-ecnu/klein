# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from datetime import timedelta
from typing import Any

import numpy

from ray.klein._internal.logging import get_logger
from ray.klein.api.job_status import JobStatus
from ray.klein.api.klein_context import KleinContext
from ray.klein.api.runtime_context import RuntimeContext
from ray.klein.api.sink_function import SinkFunction
from ray.klein.config.checkpoint_trigger_options import CheckpointTriggerOptions
from ray.klein.config.partitioner_options import PartitionerOptions
from ray.klein.config.pipeline_options import PipelineOptions
from ray.klein.integrations.console.console_sink import ConsoleSinkFunction
from tests.support.streaming import LoopSourceFunction, flat_map_identity

logger = get_logger(__name__)


class RecordingSink(SinkFunction):
    def __init__(self) -> None:
        self.task_index = None

    def open(self, runtime_context: RuntimeContext) -> None:
        self.task_index = runtime_context.task_index

    def flush(self) -> None:
        pass

    def write(self, value: dict[str, Any]) -> None:
        logger.debug("sink[%s] received %s", self.task_index, value)


class ThroughputSink(SinkFunction):
    def __init__(self) -> None:
        self.count = 0

    def flush(self) -> None:
        pass

    def write(self, value: Any) -> None:
        self.count += 1


class IdentityMap:
    def __init__(self, runtime_context: RuntimeContext = None):
        self.runtime_context = runtime_context

    def __call__(self, row: dict[str, Any]) -> dict[str, Any]:
        return row


def _assert_finished(client) -> None:
    client.wait()
    assert client.status == JobStatus.FINISHED


def test_streaming_pipeline_finishes() -> None:
    config = KleinContext().config
    config.set(CheckpointTriggerOptions.INTERVAL_RECORDS, 1)
    config.set(CheckpointTriggerOptions.INTERVAL_DURATION, timedelta(0))
    context = KleinContext(config)
    stream = (
        context.source(
            LoopSourceFunction,
            num_cpus=0.5,
            concurrency=2,
            bounded=False,
            fn_constructor_kwargs={"record_num": 4, "sleep_interval": 0},
        )
        .map_batches(
            lambda batch: {"idx": numpy.array(batch["idx"]) * 2},
            num_cpus=0.1,
            concurrency=4,
            batch_size=2,
        )
        .flat_map(flat_map_identity, num_cpus=0.1)
        .map(IdentityMap, num_cpus=0.1)
    )
    stream.write(RecordingSink, num_cpus=0.1, concurrency=2)

    _assert_finished(context.execute("finite-streaming-pipeline"))


def test_multiple_sinks_finish() -> None:
    context = KleinContext()
    stream = (
        context.source(
            LoopSourceFunction,
            num_cpus=0.1,
            fn_constructor_kwargs={"record_num": 5, "sleep_interval": 0},
        )
        .flat_map(flat_map_identity, num_cpus=0.1, concurrency=2)
        .map(IdentityMap, num_cpus=0.1)
    )
    stream.write(ConsoleSinkFunction, num_cpus=0.1)
    stream.write(RecordingSink, num_cpus=0.1)

    _assert_finished(context.execute("finite-multiple-sinks"))


def test_adaptive_partitioning_finishes() -> None:
    config = KleinContext().config
    config.set(PartitionerOptions.BUFFER_BUSY_THRESHOLD, 0.5)
    config.set(PartitionerOptions.UPDATE_INTERVAL, 0.1)
    config.set(PipelineOptions.INPUT_BUFFER_SIZE, 30)
    context = KleinContext(config)
    stream = context.source(
        LoopSourceFunction,
        num_cpus=0.1,
        concurrency=2,
        fn_constructor_kwargs={"record_num": 10, "sleep_interval": 0},
    ).map(IdentityMap, num_cpus=0.1, concurrency=5)
    stream.write(ThroughputSink, num_cpus=0.1)

    _assert_finished(context.execute("finite-adaptive-partitioning"))
