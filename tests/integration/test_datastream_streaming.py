# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from typing import Any

import numpy
import pytest

from ray.klein._internal.logging import get_logger
from ray.klein.api.job_status import JobStatus
from ray.klein.api.klein_context import KleinContext
from ray.klein.api.runtime_context import RuntimeContext
from ray.klein.api.sink_function import SinkFunction
from ray.klein.api.source_function import SourceFunction
from ray.klein.config.checkpoint_options import CheckpointOptions
from ray.klein.config.checkpoint_trigger_options import CheckpointTriggerOptions
from ray.klein.config.pipeline_options import PipelineOptions
from ray.klein.integrations.console.console_sink import ConsoleSinkFunction
from tests.support.streaming import LoopSourceFunction, flat_map_identity

logger = get_logger(__name__)


class CommitLifecycleSource(SourceFunction):
    def __init__(self, events_path: str) -> None:
        self.events_path = events_path
        self.closed = False
        self.offset = 0

    def run(self, context) -> None:
        for index in range(3):
            self.offset = index + 1
            context.collect({"id": index})

    def snapshot_state(self, checkpoint_id):
        return {"offset": self.offset}

    def restore_state(self, state):
        self.offset = state["offset"]

    def cancel(self):
        pass

    def notify_checkpoint_complete(self, checkpoint_id):
        assert not self.closed, "source closed before its durable checkpoint callback"
        with Path(self.events_path).open("a") as events:
            events.write("commit\n")

    def close(self):
        self.closed = True
        with Path(self.events_path).open("a") as events:
            events.write("close\n")


@pytest.mark.parametrize("chained", [False, True])
def test_eof_commits_checkpoint_before_closing_source(tmp_path: Path, chained: bool) -> None:
    context = KleinContext()
    context.config.set(PipelineOptions.OPERATOR_CHAINING, chained)
    context.config.set(CheckpointOptions.DIRECTORY, (tmp_path / "checkpoints").as_uri())
    context.config.set(CheckpointOptions.PERSISTENCE_INTERVAL, 3600)
    events_path = tmp_path / "source-events.txt"
    stream = context.source(
        CommitLifecycleSource,
        bounded=False,
        concurrency=1,
        num_cpus=0.1,
        fn_constructor_kwargs={"events_path": str(events_path)},
    )
    stream.map(lambda row: row, num_cpus=0.1).write(RecordingSink, num_cpus=0.1)
    _assert_finished(context.execute("eof-source-commit"))
    events = events_path.read_text().splitlines()
    assert events[0] == "commit"
    assert events[-1] == "close"


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
