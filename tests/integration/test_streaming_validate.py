# SPDX-License-Identifier: Apache-2.0
from collections import Counter
from typing import Any

import pytest

from ray.klein.api.collect_function import CollectFunction
from ray.klein.api.data_stream import DataStream
from ray.klein.api.job_handle import JobHandle
from ray.klein.api.klein_context import KleinContext
from ray.klein.api.node_type import NodeType
from ray.klein.api.runtime_context import RuntimeContext
from ray.klein.api.source_context import SourceContext
from ray.klein.api.source_function import SourceFunction
from ray.klein.config.configuration import Configuration
from ray.klein.config.job_manager_options import JobManagerOptions
from ray.klein.config.pipeline_options import PipelineOptions
from ray.klein.config.restart_strategy_options import RestartStrategyOptions
from ray.klein.exceptions import KleinError


@pytest.fixture(autouse=True)
def _reset_klein_debug_registry_between_tests():
    """Clear the in-process klein actor registry between debug-mode tests.

    In ``RAY_KLEIN_DEBUG=1`` mode klein actors are looked up via a plain
    in-process dict (``KLEIN_DEBUG_OBJECT_STORE``) instead of Ray's named
    actor table. That dict is module-global and survives across tests, so a
    later test can pick up a stale ``JobManager`` handle from a previous run
    — making ``wait_until_terminal`` return immediately for an already-failed
    job instead of waiting on the new one and observing its real failure.
    """
    yield
    from ray.klein._internal.ray import KLEIN_DEBUG_OBJECT_STORE

    KLEIN_DEBUG_OBJECT_STORE.clear()


def idx_generator(prefix, sub_task_id, record_num: int | None = None):
    cur_id = 0
    while record_num is None or cur_id <= record_num:
        cur_id += 1
        yield f"{prefix}-{sub_task_id}-{cur_id}", cur_id


class MockSourceFunction(SourceFunction):
    def __init__(self, prefix: str, record_num: int = -1):
        self._prefix = prefix
        self._sub_task_id: int | None = None
        self._interrupted = False
        self._record_num = record_num
        self._last_idx = 0

    def open(self, runtime_context: RuntimeContext) -> None:
        self._sub_task_id = runtime_context.task_index

    def run(self, context: SourceContext) -> None:
        for d, idx in idx_generator(self._prefix, self._sub_task_id, record_num=self._record_num):
            if idx <= self._last_idx:
                continue
            self._last_idx = idx
            context.collect({"id": idx, "idx": d})
            if self._interrupted:
                break

    def cancel(self) -> None:
        self._interrupted = True

    def snapshot_state(self, checkpoint_id: int) -> int:
        return self._last_idx

    def restore_state(self, state: int) -> None:
        self._last_idx = state


def mock_flat_map(data: dict[str, Any]):
    yield data


@pytest.mark.parametrize("chaining", [True, False])
@pytest.mark.parametrize("debug", [False, True])
def test_base_stream_job(debug: bool, chaining: bool, monkeypatch):
    monkeypatch.setenv("RAY_KLEIN_DEBUG", "1" if debug else "0")
    config: Configuration = Configuration()
    config.set(RestartStrategyOptions.MAX_ATTEMPTS, 1)
    config.set(PipelineOptions.OPERATOR_CHAINING, chaining)
    config.set(JobManagerOptions.HEALTH_CHECK_INTERVAL, 2)
    ctx = KleinContext(config)

    source1_prefix = "S1"
    source1_gen_num = 30
    source1_concurrency = 2
    source2_prefix = "S2"
    source2_gen_num = 70
    source2_concurrency = 1
    source3_prefix = "S3"
    source3_gen_num = 5
    source3_concurrency = 2

    stream1: DataStream = (
        ctx.source(
            MockSourceFunction,
            fn_constructor_args=[source1_prefix],
            fn_constructor_kwargs={"record_num": source1_gen_num},
            name="source",
            num_cpus=0.1,
            num_gpus=0,
            concurrency=source1_concurrency,
        )
        .map(lambda x: x, name="map1", num_cpus=0.1, num_gpus=0, concurrency=3)
        .filter(lambda x: True, name="filter1", num_cpus=0.1, num_gpus=0, concurrency=3)
    )

    stream2: DataStream = (
        ctx.source(
            MockSourceFunction,
            fn_constructor_args=[source2_prefix],
            fn_constructor_kwargs={"record_num": source2_gen_num},
            name="source",
            num_cpus=0.1,
            num_gpus=0,
            concurrency=source2_concurrency,
        )
        .map(lambda x: x, name="map2", num_cpus=0.1, num_gpus=0, concurrency=1)
        .flat_map(
            lambda x: mock_flat_map(x),
            name="flat_map1",
            num_cpus=0.1,
            num_gpus=0,
            concurrency=2,
        )
    )

    stream3: DataStream = ctx.source(
        MockSourceFunction,
        fn_constructor_args=[source3_prefix],
        fn_constructor_kwargs={"record_num": source3_gen_num},
        name="source",
        num_cpus=0.1,
        num_gpus=0,
        concurrency=source3_concurrency,
    )

    data_stream = stream1.union(stream2, stream3).map(lambda x: x, name="map2", num_cpus=0.1, num_gpus=0, concurrency=2)
    data_stream.show(-1, num_cpus=0.1, concurrency=2)
    data_stream = data_stream.flat_map(
        lambda x: mock_flat_map(x),
        name="flat_map1",
        num_cpus=0.1,
        num_gpus=0,
        concurrency=3,
    )
    data_stream.write(
        CollectFunction,
        fn_constructor_kwargs={"limit": None},
        concurrency=1,
        node_type=NodeType.TAKE,
        name="Take",
    )

    client: JobHandle = ctx.execute("test")
    client.wait()
    actual_result = client.get()

    source_gen_info = [
        (source1_prefix, source1_gen_num, source1_concurrency),
        (source2_prefix, source2_gen_num, source2_concurrency),
        (source3_prefix, source3_gen_num, source3_concurrency),
    ]

    def gen_expect_result():
        rs = []
        for source_prefix, source_gen_num, source_concurrency in source_gen_info:
            for i in range(source_concurrency):
                for idx, d in idx_generator(source_prefix, i, source_gen_num):
                    rs.append({"id": d, "idx": idx})
        return rs

    expect_result = gen_expect_result()
    actual_counter = Counter(tuple(sorted(d.items())) for d in actual_result)
    expect_counter = Counter(tuple(sorted(d.items())) for d in expect_result)
    assert expect_counter == actual_counter


@pytest.mark.parametrize("debug", [True, False])
def test_health_check(debug: bool, monkeypatch):
    monkeypatch.setenv("RAY_KLEIN_DEBUG", "1" if debug else "0")
    config: Configuration = Configuration()
    config.set(RestartStrategyOptions.MAX_ATTEMPTS, 1)
    config.set(JobManagerOptions.HEALTH_CHECK_INTERVAL, 2)
    ctx = KleinContext(config)
    source1_prefix = "S1"
    source1_gen_num = 30
    source1_concurrency = 2

    def map_test(data: dict[str, Any]):
        if data["id"] == 20:
            raise ValueError("custom value error")
        return data

    stream1: DataStream = (
        ctx.source(
            MockSourceFunction,
            fn_constructor_args=[source1_prefix],
            fn_constructor_kwargs={"record_num": source1_gen_num},
            name="source",
            num_cpus=0.1,
            num_gpus=0,
            concurrency=source1_concurrency,
        )
        .map(map_test, name="map1", num_cpus=0.1, num_gpus=0, concurrency=3)
        .filter(lambda x: True, name="filter1", num_cpus=0.1, num_gpus=0, concurrency=3)
    )

    stream1.show(-1, num_cpus=0.1, concurrency=2)

    with pytest.raises((KleinError, ValueError)) as exc_info:
        client: JobHandle = ctx.execute("test")
        client.wait()
    error_message = str(exc_info.value)
    assert "Job failed due to fatal error" in error_message
    assert "custom value error" in error_message
