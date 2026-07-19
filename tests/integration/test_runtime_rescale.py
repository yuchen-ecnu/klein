# SPDX-License-Identifier: Apache-2.0
"""Real-Ray coverage for barrier-aligned single-operator rescaling."""

from __future__ import annotations

import threading
import time
from queue import Empty

from ray.util.queue import Queue

import ray.klein as klein
from ray.klein.api.job_status import JobStatus
from ray.klein.api.klein_context import KleinContext
from ray.klein.api.sink_function import SinkFunction
from ray.klein.api.source_context import SourceContext
from ray.klein.api.source_function import SourceFunction
from ray.klein.config.configuration import Configuration
from ray.klein.config.pipeline_options import PipelineOptions
from ray.klein.config.state_options import StateOptions
from ray.klein.state.value_state_descriptor import ValueStateDescriptor
from tests.support.waiting import wait_until


class _QueueSink(SinkFunction):
    def __init__(self, queue: Queue) -> None:
        self._queue = queue

    def write(self, value) -> None:
        self._queue.put(value)


class _ControlledSequenceSource(SourceFunction):
    """Emit only driver-supplied ids so each rescale has an exact idle cut."""

    def __init__(self, input_queue: Queue) -> None:
        self._input_queue = input_queue
        self._interrupted = False
        self._last_emitted = 0

    def run(self, context: SourceContext) -> None:
        while not self._interrupted:
            try:
                index = self._input_queue.get(timeout=0.05)
            except Empty:
                # The rescale fence is injected from this record-boundary callback
                # even when the controlled source has no data to emit.
                context.on_idle()
                continue
            self._last_emitted = index
            context.collect({"idx": index})

    def cancel(self) -> None:
        self._interrupted = True

    def snapshot_state(self, checkpoint_id: int) -> int:
        del checkpoint_id
        return self._last_emitted

    def restore_state(self, state: int) -> None:
        self._last_emitted = state


def _operator(snapshot: dict, name: str) -> dict:
    return next(operator for operator in snapshot["operators"] if operator["name"].startswith(name))


def _unrelated_actor_ids(snapshot: dict, target_id: int) -> dict[int, tuple[str | None, ...]]:
    return {
        operator["op_id"]: tuple(subtask["actor_id"] for subtask in operator["subtasks"])
        for operator in snapshot["operators"]
        if operator["op_id"] != target_id
    }


def _collect_sequence_phase(output_queue: Queue, expected_indices: range) -> list[dict]:
    expected = set(expected_indices)
    received: dict[int, dict] = {}
    # Ray Queue performs one actor RPC per source record. Keep a fixed failure
    # budget for small phases and scale it for the intentionally overloaded
    # continuous-producer case, whose input queue may still contain thousands
    # of accepted records after the topology transaction has committed.
    deadline = time.monotonic() + max(20, len(expected) / 100)
    while set(received) != expected:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            missing = sorted(expected - set(received))
            raise TimeoutError(f"timed out waiting for source indices {missing}")
        try:
            row = output_queue.get(timeout=min(1.0, remaining))
        except Empty:
            continue
        index = row["idx"]
        assert index in expected, f"unexpected source index {index} in phase {expected_indices}"
        assert index not in received, f"duplicate source index {index}"
        received[index] = row

    # No input is admitted between phases, so a trailing row is necessarily a
    # duplicate crossing the just-completed topology cut.
    try:
        extra = output_queue.get(timeout=1.0)
    except Empty:
        pass
    else:
        raise AssertionError(f"unexpected trailing row after phase {expected_indices}: {extra}")
    return [received[index] for index in expected_indices]


def _slow_identity(row: dict) -> dict:
    time.sleep(0.01)
    return row


def test_real_ray_rescales_one_operator_without_restarting_its_neighbors(ray_cluster) -> None:
    config = Configuration()
    # Keep the direct upstream as an ordinary StreamTask. This exercises the
    # local inbox fence instead of SourceStreamTask's cooperative source pause.
    config.set(PipelineOptions.OPERATOR_CHAINING, False)
    context = KleinContext(config)
    input_queue = Queue()
    output_queue = Queue()
    upstream = context.source(
        _ControlledSequenceSource,
        fn_constructor_args=[input_queue],
        bounded=False,
        concurrency=1,
        num_cpus=0.1,
        name="RescaleSource",
    ).map(
        lambda row: row,
        concurrency=1,
        num_cpus=0.1,
        name="UpstreamMap",
    )
    upstream.round_robin()
    stream = upstream.map(
        _slow_identity,
        concurrency=2,
        num_cpus=0.1,
        name="DynamicMap",
    )
    stream.write(
        _QueueSink,
        fn_constructor_args=[output_queue],
        concurrency=1,
        num_cpus=0.1,
        name="RescaleSink",
    )
    handle = context.execute("runtime-local-rescale")
    stop_producer = threading.Event()
    produced = [0]

    def produce_during_rescale() -> None:
        for index in range(1, 5_001):
            if stop_producer.is_set():
                return
            input_queue.put(index)
            produced[0] = index
            time.sleep(0.002)

    producer = threading.Thread(target=produce_during_rescale, daemon=True)

    try:
        before = wait_until(
            lambda: klein.get_job_snapshot(handle.namespace),
            timeout=20,
            interval=0.2,
            description="the published job snapshot",
        )
        target = _operator(before, "DynamicMap")
        target_id = target["op_id"]
        assert _operator(before, "UpstreamMap")["op_id"] != target_id
        unrelated_before = _unrelated_actor_ids(before, target_id)
        assert all(actor_id is not None for actor_ids in unrelated_before.values() for actor_id in actor_ids)

        producer.start()
        wait_until(
            lambda: produced[0] >= 50,
            timeout=10,
            interval=0.01,
            description="the continuous source producer to start",
        )
        assert output_queue.qsize() < produced[0], "the rescale should begin with records still in flight"
        produced_before_scale_out = produced[0]
        scaled_out = klein.rescale_operator(handle.namespace, target_id, 3, timeout=30)
        assert scaled_out is not None
        assert scaled_out["status"] == "COMPLETED"
        assert produced[0] > produced_before_scale_out
        after_scale_out = klein.get_job_snapshot(handle.namespace)
        assert after_scale_out is not None
        assert _operator(after_scale_out, "DynamicMap")["parallelism"] == 3
        assert _unrelated_actor_ids(after_scale_out, target_id) == unrelated_before
        produced_before_scale_in = produced[0]

        scaled_in = klein.rescale_operator(handle.namespace, target_id, 2, timeout=30)
        assert scaled_in is not None
        assert scaled_in["status"] == "COMPLETED"
        assert produced[0] > produced_before_scale_in
        after_scale_in = klein.get_job_snapshot(handle.namespace)
        assert after_scale_in is not None
        assert _operator(after_scale_in, "DynamicMap")["parallelism"] == 2
        assert _unrelated_actor_ids(after_scale_in, target_id) == unrelated_before
        stop_producer.set()
        producer.join(timeout=5)
        assert not producer.is_alive()
        rows = _collect_sequence_phase(output_queue, range(1, produced[0] + 1))
        assert [row["idx"] for row in rows] == list(range(1, produced[0] + 1))
        assert handle.status == JobStatus.RUNNING
    finally:
        stop_producer.set()
        producer.join(timeout=5)
        if not handle.status.is_terminal:
            assert handle.cancel(timeout=30)
        input_queue.shutdown()
        output_queue.shutdown()

    assert handle.status == JobStatus.CANCELLED


def test_real_ray_preserves_keyed_state_across_scale_out_and_in(ray_cluster) -> None:
    descriptor = ValueStateDescriptor("runtime-rescale-count")

    def count_per_key(row, context):
        state = context.state(descriptor)
        count = (state.value or 0) + 1
        state.value = count
        return {"idx": row["idx"], "key": row["idx"] % 4, "count": count}

    config = Configuration()
    config.set(StateOptions.BACKEND, "memory")
    config.set(StateOptions.MAX_PARALLELISM, 16)
    context = KleinContext(config)
    input_queue = Queue()
    output_queue = Queue()
    stream = (
        context.source(
            _ControlledSequenceSource,
            fn_constructor_args=[input_queue],
            bounded=False,
            concurrency=1,
            num_cpus=0.1,
            name="StateSource",
        )
        .key_by(lambda row: row["idx"] % 4)
        .process(
            count_per_key,
            concurrency=2,
            num_cpus=0.1,
            name="StatefulCounter",
        )
    )
    stream.write(
        _QueueSink,
        fn_constructor_args=[output_queue],
        concurrency=1,
        num_cpus=0.1,
        name="StateSink",
    )
    handle = context.execute("runtime-stateful-rescale")

    try:
        before = wait_until(
            lambda: klein.get_job_snapshot(handle.namespace),
            timeout=20,
            interval=0.2,
            description="the stateful job snapshot",
        )
        target_id = _operator(before, "StatefulCounter")["op_id"]
        for index in range(1, 25):
            input_queue.put(index)
        rows = _collect_sequence_phase(output_queue, range(1, 25))

        assert klein.rescale_operator(handle.namespace, target_id, 3, timeout=30)["status"] == "COMPLETED"
        for index in range(25, 49):
            input_queue.put(index)
        rows.extend(_collect_sequence_phase(output_queue, range(25, 49)))

        assert klein.rescale_operator(handle.namespace, target_id, 1, timeout=30)["status"] == "COMPLETED"
        for index in range(49, 73):
            input_queue.put(index)
        rows.extend(_collect_sequence_phase(output_queue, range(49, 73)))

        counts: dict[int, int] = {}
        for expected_index, row in enumerate(rows, start=1):
            assert row["idx"] == expected_index
            expected = counts.get(row["key"], 0) + 1
            assert row["count"] == expected
            counts[row["key"]] = expected
        assert set(counts) == {0, 1, 2, 3}
        assert handle.status == JobStatus.RUNNING
    finally:
        if not handle.status.is_terminal:
            assert handle.cancel(timeout=30)
        input_queue.shutdown()
        output_queue.shutdown()
