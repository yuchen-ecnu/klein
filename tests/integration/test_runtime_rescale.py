# SPDX-License-Identifier: Apache-2.0
"""Real-Ray coverage for barrier-aligned single-operator rescaling."""

from __future__ import annotations

import threading
import time
from datetime import timedelta
from queue import Empty

from ray.util.queue import Queue

import ray
import ray.klein as klein
from ray.klein.api.job_status import JobStatus
from ray.klein.api.klein_context import KleinContext
from ray.klein.api.sink_function import SinkFunction
from ray.klein.api.source_context import SourceContext
from ray.klein.api.source_function import SourceFunction
from ray.klein.config.checkpoint_options import CheckpointOptions
from ray.klein.config.checkpoint_trigger_options import CheckpointTriggerOptions
from ray.klein.config.configuration import Configuration
from ray.klein.config.job_manager_options import JobManagerOptions
from ray.klein.config.pipeline_options import PipelineOptions
from ray.klein.config.state_options import StateOptions
from ray.klein.observability.state_api import submit_operator_rescale
from ray.klein.state.value_state_descriptor import ValueStateDescriptor
from tests.support.waiting import wait_until


class _QueueSink(SinkFunction):
    def __init__(self, queue: Queue) -> None:
        self._queue = queue

    def write(self, value) -> None:
        self._queue.put(value)


class _CpuBlocker:
    def ready(self) -> bool:
        return True


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


def _rescale_operation(snapshot: dict, operation_id: str) -> dict | None:
    return next(
        (
            operation
            for operation in snapshot.get("rescale_operations", ())
            if operation["operation_id"] == operation_id
        ),
        None,
    )


def _unrelated_actor_ids(snapshot: dict, target_id: int) -> dict[int, tuple[str | None, ...]]:
    return {
        operator["op_id"]: tuple(subtask["actor_id"] for subtask in operator["subtasks"])
        for operator in snapshot["operators"]
        if operator["op_id"] != target_id
    }


def _actor_ids_by_subtask(operator: dict) -> dict[int, str | None]:
    return {subtask["subtask_index"]: subtask["actor_id"] for subtask in operator["subtasks"]}


def _rows_by_subtask(operator: dict) -> dict[int, tuple[int, int]]:
    return {subtask["subtask_index"]: (subtask["rows_in"], subtask["rows_out"]) for subtask in operator["subtasks"]}


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


def test_real_ray_rescales_with_multiple_sources_and_an_inflight_checkpoint(ray_cluster) -> None:
    """A backpressured multi-source checkpoint must not block local rescale."""

    config = Configuration()
    config.set(PipelineOptions.OPERATOR_CHAINING, False)
    config.set(CheckpointOptions.MAX_CONCURRENT, 1)
    config.set(CheckpointTriggerOptions.INTERVAL_RECORDS, 150)
    config.set(CheckpointTriggerOptions.INTERVAL_DURATION, timedelta(0))
    context = KleinContext(config)
    input_queue = Queue()
    output_queue = Queue()
    stream = context.source(
        _ControlledSequenceSource,
        fn_constructor_args=[input_queue],
        bounded=False,
        concurrency=2,
        num_cpus=0.1,
        name="MultiSource",
    ).map(
        _slow_identity,
        concurrency=1,
        num_cpus=0.1,
        name="BackpressuredMap",
    )
    stream.write(
        _QueueSink,
        fn_constructor_args=[output_queue],
        concurrency=1,
        num_cpus=0.1,
        name="MultiSourceSink",
    )
    handle = context.execute("runtime-multi-source-inflight-rescale")
    record_count = 600

    try:
        before = wait_until(
            lambda: klein.get_job_snapshot(handle.namespace),
            timeout=20,
            interval=0.2,
            description="the multi-source job snapshot",
        )
        target_id = _operator(before, "BackpressuredMap")["op_id"]
        assert _operator(before, "MultiSource")["parallelism"] == 2

        for index in range(1, record_count + 1):
            input_queue.put(index)

        checkpoint_snapshot = wait_until(
            lambda: (
                snapshot
                if (snapshot := klein.get_job_snapshot(handle.namespace))
                and snapshot["checkpoints"]["summary"]["in_progress"] > 0
                and _operator(snapshot, "BackpressuredMap")["queued"] > 0
                else None
            ),
            timeout=20,
            interval=0.05,
            description="an in-flight checkpoint behind queued records",
        )
        assert checkpoint_snapshot["status"] == JobStatus.RUNNING.name
        assert output_queue.qsize() < record_count

        started = time.monotonic()
        result = klein.rescale_operator(handle.namespace, target_id, 2, timeout=30)
        elapsed = time.monotonic() - started

        assert result is not None
        assert result["status"] == "COMPLETED"
        assert elapsed < 30
        after = klein.get_job_snapshot(handle.namespace)
        assert after is not None
        assert after["status"] == JobStatus.RUNNING.name
        assert _operator(after, "BackpressuredMap")["parallelism"] == 2

        rows = _collect_sequence_phase(output_queue, range(1, record_count + 1))
        assert [row["idx"] for row in rows] == list(range(1, record_count + 1))
        assert handle.status == JobStatus.RUNNING
    finally:
        if not handle.status.is_terminal:
            assert handle.cancel(timeout=30)
        input_queue.shutdown()
        output_queue.shutdown()

    assert handle.status == JobStatus.CANCELLED


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
        # Two physical source subtasks exercise the shared post-rescale
        # checkpoint epoch and direct-input barrier alignment in real Ray.
        concurrency=2,
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
        # Two rescale RPCs each have a 30-second client budget. Keep the
        # producer alive for longer than both budgets so a slower CI runner
        # cannot exhaust the synthetic input before the scale-in begins.
        for index in range(1, 50_001):
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
        target_before = _actor_ids_by_subtask(target)
        assert set(target_before) == {0, 1}
        assert all(target_before.values())

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
        target_after_scale_out = _operator(after_scale_out, "DynamicMap")
        assert target_after_scale_out["parallelism"] == 3
        scale_out_actor_ids = _actor_ids_by_subtask(target_after_scale_out)
        assert set(scale_out_actor_ids) == {0, 1, 2}
        assert {index: scale_out_actor_ids[index] for index in target_before} == target_before
        assert scale_out_actor_ids[2] not in set(target_before.values())
        assert _unrelated_actor_ids(after_scale_out, target_id) == unrelated_before
        scale_out_rows = _rows_by_subtask(target_after_scale_out)
        produced_before_scale_in = produced[0]

        scaled_in = klein.rescale_operator(handle.namespace, target_id, 2, timeout=30)
        assert scaled_in is not None
        assert scaled_in["status"] == "COMPLETED"
        assert produced[0] > produced_before_scale_in
        after_scale_in = klein.get_job_snapshot(handle.namespace)
        assert after_scale_in is not None
        target_after_scale_in = _operator(after_scale_in, "DynamicMap")
        assert target_after_scale_in["parallelism"] == 2
        scale_in_actor_ids = _actor_ids_by_subtask(target_after_scale_in)
        assert set(scale_in_actor_ids) == {0, 1}
        assert scale_in_actor_ids == {index: scale_out_actor_ids[index] for index in scale_in_actor_ids}
        assert scale_in_actor_ids == target_before
        scale_in_rows = _rows_by_subtask(target_after_scale_in)
        for index, previous_counts in scale_out_rows.items():
            if index in scale_in_rows:
                assert all(
                    current >= previous for current, previous in zip(scale_in_rows[index], previous_counts, strict=True)
                )
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

    assert handle.status == JobStatus.CANCELLED


def test_real_ray_async_rescale_survives_the_submitter_and_serializes_scale_out_and_in(ray_cluster) -> None:
    """The submit RPC owns no runtime work; snapshots remain the source of truth."""

    config = Configuration()
    config.set(PipelineOptions.OPERATOR_CHAINING, False)
    config.set(CheckpointOptions.PERSISTENCE_INTERVAL, 2)
    config.set(CheckpointOptions.MAX_CONCURRENT, 1)
    config.set(CheckpointTriggerOptions.INTERVAL_RECORDS, 10_000)
    config.set(CheckpointTriggerOptions.INTERVAL_DURATION, timedelta(milliseconds=250))
    context = KleinContext(config)
    input_queue = Queue()
    output_queue = Queue()
    stream = context.source(
        _ControlledSequenceSource,
        fn_constructor_args=[input_queue],
        bounded=False,
        concurrency=1,
        num_cpus=0.1,
        name="AsyncRescaleSource",
    ).map(
        _slow_identity,
        concurrency=2,
        num_cpus=0.1,
        name="AsyncDynamicMap",
    )
    stream.write(
        _QueueSink,
        fn_constructor_args=[output_queue],
        concurrency=1,
        num_cpus=0.1,
        name="AsyncRescaleSink",
    )
    handle = context.execute("runtime-async-rescale")

    try:
        before = wait_until(
            lambda: (
                snapshot
                if (snapshot := klein.get_job_snapshot(handle.namespace))
                and _operator(snapshot, "AsyncDynamicMap")["can_rescale"]
                else None
            ),
            timeout=20,
            interval=0.1,
            description="the async-rescale operator to become ready",
        )
        target_id = _operator(before, "AsyncDynamicMap")["op_id"]

        for index in range(1, 13):
            input_queue.put(index)
        rows = _collect_sequence_phase(output_queue, range(1, 13))

        started = time.monotonic()
        accepted = submit_operator_rescale(handle.namespace, target_id, 3, timeout=10)
        submit_elapsed = time.monotonic() - started
        assert accepted is not None
        assert accepted["status"] == "ACCEPTED"
        assert accepted["phase"] == "QUEUED"
        assert accepted["previous_parallelism"] == 2
        assert accepted["target_parallelism"] == 3
        assert submit_elapsed < 5
        scale_out_id = accepted["operation_id"]

        # This is a distinct client call after the submit response returned.
        # The JobManager-owned task must still hold the single-operation slot.
        rejected = submit_operator_rescale(handle.namespace, target_id, 4, timeout=10)
        assert rejected is not None
        assert rejected["status"] == "REJECTED"
        assert rejected["active_operation_id"] == scale_out_id

        active = wait_until(
            lambda: (
                snapshot
                if (snapshot := klein.get_job_snapshot(handle.namespace))
                and (operation := _rescale_operation(snapshot, scale_out_id)) is not None
                and operation["status"] in {"ACCEPTED", "RUNNING", "STABILIZING"}
                else None
            ),
            timeout=20,
            interval=0.05,
            description="the background scale-out operation in a fresh snapshot",
        )
        active_operation = _rescale_operation(active, scale_out_id)
        assert active_operation is not None
        active_target = _operator(active, "AsyncDynamicMap")
        assert active_target["rescale_operation"]["operation_id"] == scale_out_id
        assert active_target["can_rescale"] is False

        after_scale_out = wait_until(
            lambda: (
                snapshot
                if (snapshot := klein.get_job_snapshot(handle.namespace))
                and (operation := _rescale_operation(snapshot, scale_out_id)) is not None
                and operation["status"] == "COMPLETED"
                else None
            ),
            timeout=30,
            interval=0.1,
            description="the background scale-out stabilization checkpoint",
        )
        scale_out_operation = _rescale_operation(after_scale_out, scale_out_id)
        assert scale_out_operation is not None
        assert scale_out_operation["phase"] == "COMPLETED"
        assert scale_out_operation["ended_at_ms"] is not None
        target_after_scale_out = _operator(after_scale_out, "AsyncDynamicMap")
        assert target_after_scale_out["parallelism"] == 3
        assert len(target_after_scale_out["subtasks"]) == 3
        assert target_after_scale_out["can_rescale"] is True

        for index in range(13, 25):
            input_queue.put(index)
        rows.extend(_collect_sequence_phase(output_queue, range(13, 25)))

        accepted_scale_in = submit_operator_rescale(handle.namespace, target_id, 1, timeout=10)
        assert accepted_scale_in is not None
        assert accepted_scale_in["status"] == "ACCEPTED"
        assert accepted_scale_in["previous_parallelism"] == 3
        assert accepted_scale_in["target_parallelism"] == 1
        scale_in_id = accepted_scale_in["operation_id"]

        after_scale_in = wait_until(
            lambda: (
                snapshot
                if (snapshot := klein.get_job_snapshot(handle.namespace))
                and (operation := _rescale_operation(snapshot, scale_in_id)) is not None
                and operation["status"] == "COMPLETED"
                else None
            ),
            timeout=30,
            interval=0.1,
            description="the background scale-in stabilization checkpoint",
        )
        target_after_scale_in = _operator(after_scale_in, "AsyncDynamicMap")
        assert target_after_scale_in["parallelism"] == 1
        assert len(target_after_scale_in["subtasks"]) == 1
        assert target_after_scale_in["can_rescale"] is True
        assert target_after_scale_in["rescale_operation"]["operation_id"] == scale_in_id
        assert {operation["operation_id"] for operation in after_scale_in["rescale_operations"]} >= {
            scale_out_id,
            scale_in_id,
        }

        for index in range(25, 37):
            input_queue.put(index)
        rows.extend(_collect_sequence_phase(output_queue, range(25, 37)))
        assert [row["idx"] for row in rows] == list(range(1, 37))
        assert handle.status == JobStatus.RUNNING
    finally:
        if not handle.status.is_terminal:
            assert handle.cancel(timeout=30)
        input_queue.shutdown()
        output_queue.shutdown()

    assert handle.status == JobStatus.CANCELLED


def test_real_ray_async_scale_out_fails_cleanly_when_cluster_cpus_are_exhausted(ray_cluster) -> None:
    """An unschedulable candidate times out before fencing the live data path."""

    config = Configuration()
    config.set(PipelineOptions.OPERATOR_CHAINING, False)
    # Ten seconds is short relative to the 300-second production default, but
    # still leaves enough headroom for the initial three-actor deployment on CI.
    config.set(JobManagerOptions.SCHEDULER_START_TIMEOUT, 10)
    config.set(CheckpointOptions.PERSISTENCE_INTERVAL, 1)
    config.set(CheckpointOptions.MAX_CONCURRENT, 1)
    config.set(CheckpointTriggerOptions.INTERVAL_RECORDS, 10_000)
    config.set(CheckpointTriggerOptions.INTERVAL_DURATION, timedelta(milliseconds=250))
    context = KleinContext(config)
    input_queue = Queue()
    output_queue = Queue()
    stream = context.source(
        _ControlledSequenceSource,
        fn_constructor_args=[input_queue],
        bounded=False,
        concurrency=1,
        num_cpus=0.1,
        name="ResourceFailureSource",
    ).map(
        _slow_identity,
        concurrency=1,
        num_cpus=0.1,
        name="ResourceFailureMap",
    )
    stream.write(
        _QueueSink,
        fn_constructor_args=[output_queue],
        concurrency=1,
        num_cpus=0.1,
        name="ResourceFailureSink",
    )
    handle = context.execute("runtime-resource-starved-async-rescale")
    blocker = None

    def named_actors() -> set[str]:
        return {
            actor["name"]
            for actor in ray.util.list_named_actors(all_namespaces=True)
            if actor["namespace"] == handle.namespace
        }

    try:
        before = wait_until(
            lambda: (
                snapshot
                if (snapshot := klein.get_job_snapshot(handle.namespace))
                and _operator(snapshot, "ResourceFailureMap")["can_rescale"]
                else None
            ),
            timeout=20,
            interval=0.1,
            description="the resource-failure operator to become ready",
        )
        target_before = _operator(before, "ResourceFailureMap")
        target_id = target_before["op_id"]
        target_actors_before = _actor_ids_by_subtask(target_before)
        unrelated_before = _unrelated_actor_ids(before, target_id)

        for index in range(1, 13):
            input_queue.put(index)
        rows = _collect_sequence_phase(output_queue, range(1, 13))

        available_cpus = float(ray.available_resources().get("CPU", 0.0))
        assert available_cpus > 0.15
        # Leave less than one 0.1-CPU candidate free. The existing stream tasks
        # remain runnable in their already-reserved placement-group bundles.
        blocker_cpus = round(available_cpus - 0.05, 4)
        blocker = ray.remote(_CpuBlocker).options(num_cpus=blocker_cpus).remote()
        assert ray.get(blocker.ready.remote(), timeout=10) is True
        wait_until(
            lambda: float(ray.available_resources().get("CPU", 0.0)) < 0.1,
            timeout=10,
            interval=0.05,
            description="the CPU blocker to exhaust schedulable CPU",
        )
        named_actors_before_failure = named_actors()

        accepted = submit_operator_rescale(handle.namespace, target_id, 3, timeout=10)
        assert accepted is not None
        assert accepted["status"] == "ACCEPTED"
        assert accepted["phase"] == "QUEUED"
        assert accepted["previous_parallelism"] == 1
        assert accepted["target_parallelism"] == 3
        operation_id = accepted["operation_id"]

        pending = wait_until(
            lambda: (
                snapshot
                if (snapshot := klein.get_job_snapshot(handle.namespace))
                and (operation := _rescale_operation(snapshot, operation_id)) is not None
                and operation["status"] in {"ACCEPTED", "RUNNING", "FAILED"}
                else None
            ),
            timeout=15,
            interval=0.05,
            description="the resource-starved operation to become observable",
        )
        pending_operation = _rescale_operation(pending, operation_id)
        assert pending_operation is not None
        pending_target = _operator(pending, "ResourceFailureMap")
        assert pending["status"] == JobStatus.RUNNING.name
        assert pending_target["parallelism"] == 1
        assert _actor_ids_by_subtask(pending_target) == target_actors_before
        assert _unrelated_actor_ids(pending, target_id) == unrelated_before
        assert pending_target["can_rescale"] is (pending_operation["status"] == "FAILED")

        # Whether candidate placement is still queued/running or has already
        # failed, the readiness fence precedes the data-plane fence, so the
        # original topology must continue processing.
        for index in range(13, 25):
            input_queue.put(index)
        rows.extend(_collect_sequence_phase(output_queue, range(13, 25)))
        pending_after_data = klein.get_job_snapshot(handle.namespace)
        assert pending_after_data is not None
        pending_operation = _rescale_operation(pending_after_data, operation_id)
        assert pending_operation is not None
        assert pending_operation["status"] in {"ACCEPTED", "RUNNING", "FAILED"}
        if pending_operation["status"] == "FAILED":
            assert pending_operation["phase"] == "COMPLETED"
        else:
            assert pending_operation["phase"] in {"QUEUED", "COORDINATING"}
        pending_target_after_data = _operator(pending_after_data, "ResourceFailureMap")
        assert pending_after_data["status"] == JobStatus.RUNNING.name
        assert pending_target_after_data["parallelism"] == 1
        assert _actor_ids_by_subtask(pending_target_after_data) == target_actors_before
        assert _unrelated_actor_ids(pending_after_data, target_id) == unrelated_before
        assert pending_target_after_data["can_rescale"] is (pending_operation["status"] == "FAILED")

        failed = wait_until(
            lambda: (
                snapshot
                if (snapshot := klein.get_job_snapshot(handle.namespace))
                and (operation := _rescale_operation(snapshot, operation_id)) is not None
                and operation["status"] == "FAILED"
                else None
            ),
            timeout=25,
            interval=0.1,
            description="the resource-starved scale-out to fail",
        )
        failed_operation = _rescale_operation(failed, operation_id)
        assert failed_operation is not None
        assert failed_operation["phase"] == "COMPLETED"
        assert failed_operation["ended_at_ms"] is not None
        assert "timeout" in failed_operation["error"].lower()
        target_after_failure = _operator(failed, "ResourceFailureMap")
        assert failed["status"] == JobStatus.RUNNING.name
        assert target_after_failure["parallelism"] == 1
        assert _actor_ids_by_subtask(target_after_failure) == target_actors_before
        assert _unrelated_actor_ids(failed, target_id) == unrelated_before
        assert target_after_failure["can_rescale"] is True
        assert target_after_failure["rescale_operation"]["operation_id"] == operation_id
        wait_until(
            lambda: named_actors() == named_actors_before_failure,
            timeout=10,
            interval=0.1,
            description="the failed scale-out candidates to be removed",
        )

        for index in range(25, 37):
            input_queue.put(index)
        rows.extend(_collect_sequence_phase(output_queue, range(25, 37)))

        ray.kill(blocker, no_restart=True)
        blocker = None
        wait_until(
            lambda: float(ray.available_resources().get("CPU", 0.0)) >= 0.2,
            timeout=10,
            interval=0.05,
            description="the blocked CPU resources to be released",
        )

        retry = submit_operator_rescale(handle.namespace, target_id, 3, timeout=10)
        assert retry is not None
        assert retry["status"] == "ACCEPTED"
        retry_id = retry["operation_id"]
        after_retry = wait_until(
            lambda: (
                snapshot
                if (snapshot := klein.get_job_snapshot(handle.namespace))
                and (operation := _rescale_operation(snapshot, retry_id)) is not None
                and operation["status"] == "COMPLETED"
                else None
            ),
            timeout=30,
            interval=0.1,
            description="the retried scale-out to stabilize",
        )
        target_after_retry = _operator(after_retry, "ResourceFailureMap")
        retry_actor_ids = _actor_ids_by_subtask(target_after_retry)
        assert target_after_retry["parallelism"] == 3
        assert retry_actor_ids[0] == target_actors_before[0]
        assert len(set(retry_actor_ids.values())) == 3
        assert _unrelated_actor_ids(after_retry, target_id) == unrelated_before
        assert target_after_retry["can_rescale"] is True

        for index in range(37, 49):
            input_queue.put(index)
        rows.extend(_collect_sequence_phase(output_queue, range(37, 49)))
        assert [row["idx"] for row in rows] == list(range(1, 49))
        assert handle.status == JobStatus.RUNNING
    finally:
        if blocker is not None:
            ray.kill(blocker, no_restart=True)
        if not handle.status.is_terminal:
            assert handle.cancel(timeout=30)
        input_queue.shutdown()
        output_queue.shutdown()

    assert handle.status == JobStatus.CANCELLED
