# SPDX-License-Identifier: Apache-2.0
import random
import unittest
from typing import Any

from ray.klein.api.collector import Collector
from ray.klein.api.functions.logical_function import LogicalFunction
from ray.klein.api.missing_data_strategy import MissingDataStrategy
from ray.klein.api.runtime_info import RuntimeInfo
from ray.klein.observability.metrics.metric_group import TaskMetricGroup
from ray.klein.runtime.context.runtime_context import TaskRuntimeContext
from ray.klein.runtime.message import KeyRecord, Record
from ray.klein.runtime.operator.flat_map_with_rank_operator import FlatMapWithRankOperator
from ray.klein.runtime.operator.reduce_operator import ReduceOperator


def preprocess(data: Any) -> Any:
    for comment in data["comment_list"]:
        yield {
            "note_id": data["note_id"],
            "comment_input_id": [i for i, _ in enumerate(comment)],
        }


def pre_processor(data):
    for x in data["comment_list"][0]:
        data = {
            "note_id": data["note_id"],
            "comment_input_id": [random.randint(1, 1000) for _ in range(len(x))],
        }
        yield data


def batch_inference(data):
    return {
        "note_id": data["note_id"],
        "comment_embeddings": [[i * 2 for i in ids] for ids in data["comment_input_id"]],
    }


def key_selector(data: Any) -> Any:
    return data.get("note_id")


def get_key_record(data: Any) -> KeyRecord:
    return KeyRecord(key_selector(data), data)


class MockCollector(Collector):
    def __init__(self):
        super().__init__()
        self.records = []

    def collect(self, record: Record) -> None:
        self.records.append(record)


class DatastreamBatchTests(unittest.TestCase):
    def test_map_reduce_flatmap(self) -> None:
        origin_datas = [
            {
                "note_id": "111",
                "comment_list": ["可以的", "小猫好乖啊", "好可爱的小猫", "给姨姨吸吸"],
            },
            {"note_id": "222", "comment_list": ["好漂亮的花瓶", "哪里买的呀"]},
        ]

        flat_map_operator = FlatMapWithRankOperator(
            LogicalFunction(
                preprocess,
                batch_size=None,
            ),
            missing_data_strategy=MissingDataStrategy.ERROR,
        )

        collector = MockCollector()
        flat_map_operator.open(
            collector,
            TaskRuntimeContext(
                "1",
                1,
                1,
                {},
                TaskMetricGroup(None, "1", "1", 1),
                None,
                RuntimeInfo(batch_size=None, batch_timeout=300, batch_format="default"),
            ),
        )
        for data in origin_datas:
            flat_map_operator.process_element(Record(data))

        # Remove dynamically changing timestamps from results
        flat_map_res_no_id = []
        for record in collector.records:
            record.block.pop("__id__")
            flat_map_res_no_id.append(record)

        expect = [
            Record({"note_id": "111", "comment_input_id": [0, 1, 2], "__rank__": (1, 4)}),
            Record(
                {
                    "note_id": "111",
                    "comment_input_id": [0, 1, 2, 3, 4],
                    "__rank__": (2, 4),
                }
            ),
            Record(
                {
                    "note_id": "111",
                    "comment_input_id": [0, 1, 2, 3, 4, 5],
                    "__rank__": (3, 4),
                }
            ),
            Record(
                {
                    "note_id": "111",
                    "comment_input_id": [0, 1, 2, 3, 4],
                    "__rank__": (4, 4),
                }
            ),
            Record(
                {
                    "note_id": "222",
                    "comment_input_id": [0, 1, 2, 3, 4, 5],
                    "__rank__": (1, 2),
                }
            ),
            Record(
                {
                    "note_id": "222",
                    "comment_input_id": [0, 1, 2, 3, 4],
                    "__rank__": (2, 2),
                }
            ),
        ]

        self.assertEqual(flat_map_res_no_id, expect)

    def test_map_reduce_reduce(self):
        input_records = [
            Record(
                block={
                    "note_id": "111",
                    "comment_input_id": [0, 1, 2, 3, 4, 5],
                    "__rank__": (3, 4),
                    "__id__": (1, 1),
                },
            ),
            Record(
                block={
                    "note_id": "111",
                    "comment_input_id": [0, 1, 2],
                    "__rank__": (1, 4),
                    "__id__": (1, 1),
                },
            ),
            Record(
                block={
                    "note_id": "222",
                    "comment_input_id": [0, 1, 2, 3, 4],
                    "__rank__": (2, 2),
                    "__id__": (1, 2),
                },
            ),
            Record(
                block={
                    "note_id": "111",
                    "comment_input_id": [0, 1, 2, 3, 4],
                    "__rank__": (4, 4),
                    "__id__": (1, 1),
                },
            ),
            Record(
                block={
                    "note_id": "111",
                    "comment_input_id": [0, 1, 2, 3, 4],
                    "__rank__": (2, 4),
                    "__id__": (1, 1),
                },
            ),
            Record(
                block={
                    "note_id": "222",
                    "comment_input_id": [0, 1, 2, 3, 4, 5],
                    "__rank__": (1, 2),
                    "__id__": (1, 2),
                },
            ),
        ]

        flat_map_operator = ReduceOperator(
            LogicalFunction(
                lambda x: x,
                batch_size=None,
            ),
            key_selector=key_selector,
        )

        collector = MockCollector()
        flat_map_operator.open(
            collector,
            TaskRuntimeContext(
                "1",
                1,
                1,
                {},
                TaskMetricGroup(None, "1", "1", 1),
                None,
                RuntimeInfo(batch_size=None, batch_timeout=300, batch_format="default"),
            ),
        )
        for record in input_records:
            flat_map_operator.process_element(record)

        expect = [
            Record(
                {
                    "note_id": ["111", "111", "111", "111"],
                    "comment_input_id": [
                        [0, 1, 2],
                        [0, 1, 2, 3, 4],
                        [0, 1, 2, 3, 4, 5],
                        [0, 1, 2, 3, 4],
                    ],
                }
            ),
            Record(
                {
                    "note_id": ["222", "222"],
                    "comment_input_id": [[0, 1, 2, 3, 4, 5], [0, 1, 2, 3, 4]],
                }
            ),
        ]
        self.assertEqual(collector.records, expect)
