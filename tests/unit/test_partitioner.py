# SPDX-License-Identifier: Apache-2.0
import queue
from collections.abc import Callable
from typing import Any
from unittest import TestCase

import ray.klein as klein
from ray.klein.api.runtime_info import RuntimeInfo
from ray.klein.config.configuration import Configuration
from ray.klein.config.partitioner_options import PartitionerOptions
from ray.klein.observability.metrics.metric_group import (
    JobMetricGroup,
    OperatorMetricGroup,
    TaskMetricGroup,
)
from ray.klein.runtime.actor import KleinActorHandle, create_remote_actor
from ray.klein.runtime.collector.collector import OutputCollector
from ray.klein.runtime.context.runtime_context import TaskRuntimeContext
from ray.klein.runtime.coordinator.checkpoint_strategy import CheckpointStrategy
from ray.klein.runtime.message import Barrier, EndOfData, PutAck, Record
from ray.klein.runtime.partitioning import (
    AdaptivePartitioner,
    KeyPartitioner,
    RescalePartitioner,
)
from ray.klein.state.key_group_range import key_group_for_key, key_group_owner


class ConsumerFunction:
    def __init__(self, name: str, qsize: int = 100):
        self.name = name
        self._input_buffer = queue.Queue(maxsize=qsize)
        self.input_datas = []

    def put(self, record: Record, timeout=None, sender_vertex_id=None, batch_sequence=None) -> PutAck:
        try:
            self._input_buffer.put(record, timeout=timeout)
            self.input_datas.append(record)
        except queue.Full:
            return PutAck(False, self.get_input_buffer_size())
        return PutAck(True, self.get_input_buffer_size())

    def get_input_buffer_size(self):
        return self._input_buffer.qsize()

    def get_input_datas(self) -> list[Record]:
        return self.input_datas


class MockCheckpointStrategy(CheckpointStrategy):
    def __init__(self):
        super().__init__()

    def on_barrier_received(self, barrier: Barrier, on_barrier_aligned: Callable | None = None) -> bool:
        return False

    def on_eof_received(self, barrier: EndOfData) -> bool:
        return False

    def restore_source_state(self) -> Any:
        return None

    def should_trigger(self, record_emitted: bool) -> bool:
        return False

    def generate_next_barrier(self, is_eof: bool) -> Barrier | None:
        return None

    def register_operator_state(self, barrier_id, reference) -> bool:
        return False

    async def restore_operator_states_async(self) -> tuple:
        return ()

    async def restore_durable_operator_states_async(self) -> tuple:
        return ()


def get_mock_configuration() -> Configuration:
    configuration = Configuration()
    configuration.set(
        PartitionerOptions.BUSY_RATIO,
        0.5,
        # PartitionerOptions.BUSY_RATIO.default_value
    )
    configuration.set(
        PartitionerOptions.BUFFER_BUSY_THRESHOLD,
        0.5,
        # PartitionerOptions.BUFFER_BUSY_THRESHOLD.default_value
    )
    return configuration


def create_mock_metric_group(job_name: str, task_index: str, subtask_index: int) -> OperatorMetricGroup:
    job_metric_group = JobMetricGroup(job_name)
    task_metric_group = TaskMetricGroup(job_metric_group, task_index, "Map", subtask_index)
    return OperatorMetricGroup(task_metric_group, task_index, "Map", subtask_index)


def get_mock_runtime_info() -> RuntimeInfo:
    return RuntimeInfo(batch_size=None, batch_timeout=300, batch_format="default")


def get_mock_runtime_context(
    mock_task_name: str, task_index: str, subtask_index: int, parallelism: int
) -> TaskRuntimeContext:
    mock_configuration = get_mock_configuration()
    mock_metric_group = create_mock_metric_group("mock_job", task_index, subtask_index)

    return TaskRuntimeContext(
        mock_task_name,
        subtask_index,
        parallelism,
        mock_configuration,
        mock_metric_group,
        MockCheckpointStrategy(),
        get_mock_runtime_info(),
    )


def get_mock_adaptive_partitioner(subtask_index: int, target_tasks: list[KleinActorHandle], parallelism: int):
    mock_runtime_context = get_mock_runtime_context(f"partition_{subtask_index}", "2", subtask_index, parallelism)
    adaptive_partitioner = AdaptivePartitioner()
    adaptive_partitioner.open(mock_runtime_context, target_tasks)
    return adaptive_partitioner


def get_mock_rescale_partitioner(subtask_index: int, target_tasks: list[KleinActorHandle], parallelism: int):
    mock_runtime_context = get_mock_runtime_context(f"partition_{subtask_index}", "2", subtask_index, parallelism)
    adaptive_partitioner = RescalePartitioner()
    adaptive_partitioner.open(mock_runtime_context, target_tasks)
    return adaptive_partitioner


def get_local_klein_handler_list(num: int, qsize: int) -> list[KleinActorHandle]:
    return [
        create_remote_actor(
            ConsumerFunction,
            construct_args={"name": f"consumer_{i}", "qsize": qsize},
            local_mode=True,
        )
        for i in range(1, num + 1)
    ]


class CheckpointStatusTest(TestCase):
    def test_rescale(self) -> None:
        """
        test for rescale partitioner
        """
        expected_results = {0: [0, 3], 1: [1, 4], 2: [2]}
        for i in range(3):
            res: list[int] = RescalePartitioner.distribute_tasks(3, 5, i)
            self.assertEqual(res, expected_results[i])

        expected_results = {0: [0], 1: [1], 2: [2], 3: [3], 4: [4]}
        for i in range(3):
            res: list[int] = RescalePartitioner.distribute_tasks(5, 5, i)
            self.assertEqual(res, expected_results[i])

        expected_results = {0: [0], 1: [1], 2: [2], 3: [0], 4: [1]}
        for i in range(3):
            res: list[int] = RescalePartitioner.distribute_tasks(5, 3, i)
            self.assertEqual(res, expected_results[i])

    def test_adaptive_data_sequence_from_1_to_4(self) -> None:
        handlers = get_local_klein_handler_list(4, 100)
        adaptive_partition = get_mock_adaptive_partitioner(0, handlers, 1)
        collector = OutputCollector(
            handlers,
            adaptive_partition,
            100,
            ["1", "2", "3", "4"],
            5,
        )

        records = [Record({"id": i}) for i in range(10)]
        for record in records:
            collector._collect(record)

        self.assertEqual(
            klein.get(handlers[0].get_input_datas()),
            [Record({"id": 0}), Record({"id": 4}), Record({"id": 8})],
        )
        self.assertEqual(
            klein.get(handlers[1].get_input_datas()),
            [Record({"id": 1}), Record({"id": 5}), Record({"id": 9})],
        )
        self.assertEqual(
            klein.get(handlers[2].get_input_datas()),
            [Record({"id": 2}), Record({"id": 6})],
        )
        self.assertEqual(
            klein.get(handlers[3].get_input_datas()),
            [Record({"id": 3}), Record({"id": 7})],
        )

    def test_rescale_data_sequence_from_2_to_4(self) -> None:
        handlers = get_local_klein_handler_list(4, 100)
        collectors = []
        for i in range(2):
            rescale_partition = get_mock_rescale_partitioner(i, handlers, 2)
            collector = OutputCollector(
                handlers,
                rescale_partition,
                100,
                ["1", "2", "3", "4"],
                5,
            )
            collectors.append(collector)

        records = [Record({"id": i}) for i in range(10)]
        for i, record in enumerate(records):
            collectors[i % len(collectors)]._collect(record)

        self.assertEqual(
            klein.get(handlers[0].get_input_datas()),
            [Record({"id": 0}), Record({"id": 4}), Record({"id": 8})],
        )
        self.assertEqual(
            klein.get(handlers[1].get_input_datas()),
            [Record({"id": 1}), Record({"id": 5}), Record({"id": 9})],
        )
        self.assertEqual(
            klein.get(handlers[2].get_input_datas()),
            [Record({"id": 2}), Record({"id": 6})],
        )
        self.assertEqual(
            klein.get(handlers[3].get_input_datas()),
            [Record({"id": 3}), Record({"id": 7})],
        )

    def test_rescale_data_sequence_from_4_to_2(self) -> None:
        handlers = get_local_klein_handler_list(2, 100)
        collectors = []
        for i in range(4):
            rescale_partition = get_mock_rescale_partitioner(i, handlers, 4)
            collector = OutputCollector(
                handlers,
                rescale_partition,
                100,
                ["1", "2", "3", "4"],
                5,
            )
            collectors.append(collector)

        records = [Record({"id": i}) for i in range(10)]
        for i, record in enumerate(records):
            collectors[i % len(collectors)]._collect(record)

        self.assertEqual(
            klein.get(handlers[0].get_input_datas()),
            [
                Record({"id": 0}),
                Record({"id": 2}),
                Record({"id": 4}),
                Record({"id": 6}),
                Record({"id": 8}),
            ],
        )
        self.assertEqual(
            klein.get(handlers[1].get_input_datas()),
            [
                Record({"id": 1}),
                Record({"id": 3}),
                Record({"id": 5}),
                Record({"id": 7}),
                Record({"id": 9}),
            ],
        )

    def test_key_partitioner(self) -> None:
        downstream_num = 4
        handlers = get_local_klein_handler_list(downstream_num, 100)

        def key_selector(data: Any) -> str:
            return data["id"]

        records = [Record({"id": i}) for i in range(10)]
        expect = [[] for i in range(downstream_num)]
        for r in records:
            key_group = key_group_for_key(key_selector(r.block), 128)
            expect[key_group_owner(key_group, 128, downstream_num)].append(r)

        key_partitioner = KeyPartitioner(key_selector=key_selector)
        key_partitioner.open(get_mock_runtime_context("key_partitioner_test", "1", 1, 1), handlers)
        collector = OutputCollector(
            handlers,
            key_partitioner,
            100,
            ["1", "2", "3", "4"],
            5,
        )
        for record in records:
            collector._collect(record)

        for i in range(downstream_num):
            self.assertEqual(klein.get(handlers[i].get_input_datas()), expect[i])
