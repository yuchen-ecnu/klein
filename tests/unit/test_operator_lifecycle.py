# SPDX-License-Identifier: Apache-2.0
from types import SimpleNamespace
from typing import ClassVar

from ray.klein.api.collect_function import CollectFunction
from ray.klein.api.functions.logical_function import LogicalFunction
from ray.klein.api.runtime_info import RuntimeInfo
from ray.klein.api.sink_committable import SinkCommittable
from ray.klein.config.configuration import Configuration
from ray.klein.observability.metrics.metric_group import JobMetricGroup
from ray.klein.runtime.context.runtime_context import TaskRuntimeContext
from ray.klein.runtime.message import Record
from ray.klein.runtime.operator.chained_one_input_operator import ChainedOneInputOperator
from ray.klein.runtime.operator.composite_sink_committable import CompositeSinkCommittable
from ray.klein.runtime.operator.operator import OneInputOperator, OperatorType, StreamOperator
from ray.klein.runtime.operator.sink import CollectOperator


class _CountingCollect(CollectFunction):
    instances: ClassVar[list["_CountingCollect"]] = []

    def __init__(self, output_queue) -> None:
        super().__init__(output_queue)
        self.open_count = 0
        self.close_count = 0
        self.instances.append(self)

    def open(self, runtime_context) -> None:
        self.open_count += 1

    def close(self) -> None:
        self.close_count += 1


def test_collect_operator_materializes_one_lifecycle_instance() -> None:
    _CountingCollect.instances.clear()
    operator = CollectOperator(LogicalFunction(_CountingCollect))
    operator.id, operator.name = 1, "collect"
    operator.assign_output_queue(SimpleNamespace())
    context = TaskRuntimeContext(
        "collect",
        0,
        1,
        Configuration(),
        JobMetricGroup("test").add_task_group("1:0", "collect", 0),
        SimpleNamespace(),
        RuntimeInfo(),
        "test",
    )

    operator.open(None, context)
    operator.close()

    assert len(_CountingCollect.instances) == 1
    assert (_CountingCollect.instances[0].open_count, _CountingCollect.instances[0].close_count) == (1, 1)


class _LifecycleOperator(StreamOperator, OneInputOperator):
    def __init__(self, *, fail_open: bool = False) -> None:
        super().__init__()
        self.fail_open = fail_open
        self.open_count = 0
        self.close_count = 0

    def open(self, collector, runtime_context) -> None:
        self.open_count += 1
        if self.fail_open:
            raise RuntimeError("open failed")

    def close(self) -> None:
        self.close_count += 1

    def process_element(self, record: Record) -> None:
        return None

    @property
    def operator_type(self) -> OperatorType:
        return OperatorType.ONE_INPUT


def test_chained_operator_rolls_back_every_opened_operator() -> None:
    root = _LifecycleOperator()
    first = _LifecycleOperator()
    failing = _LifecycleOperator(fail_open=True)
    chain = ChainedOneInputOperator(root, [first, failing])

    try:
        chain.open(None, SimpleNamespace())
    except RuntimeError as error:
        assert str(error) == "open failed"
    else:
        raise AssertionError("chain.open() must propagate the operator failure")

    assert root.open_count == root.close_count == 0
    assert (first.open_count, first.close_count) == (1, 1)
    assert (failing.open_count, failing.close_count) == (1, 1)


class _TestCommittable(SinkCommittable):
    def __init__(self, transaction_id: str = "transaction-1") -> None:
        self._transaction_id = transaction_id

    @property
    def transaction_id(self) -> str:
        return self._transaction_id

    def commit(self) -> None:
        return None

    def abort(self) -> None:
        return None


class _TransactionalLifecycleOperator(_LifecycleOperator):
    def __init__(self, transaction_id: str = "transaction-1") -> None:
        super().__init__()
        self.transaction_id = transaction_id
        self.flush_count = 0
        self.prepare_count = 0

    def flush(self) -> None:
        self.flush_count += 1

    def prepare_checkpoint(self, checkpoint_id: int) -> SinkCommittable:
        assert checkpoint_id == 7
        self.prepare_count += 1
        return _TestCommittable(self.transaction_id)


def test_nested_chained_operator_forwards_checkpoint_lifecycle() -> None:
    transactional = _TransactionalLifecycleOperator()
    inner = ChainedOneInputOperator(_LifecycleOperator(), [transactional])
    outer = ChainedOneInputOperator(_LifecycleOperator(), [inner])

    outer.flush()
    committable = outer.prepare_checkpoint(7)

    assert transactional.flush_count == 1
    assert transactional.prepare_count == 1
    assert committable is not None and committable.transaction_id == "transaction-1"


def test_chained_fan_out_composes_multiple_sink_transactions() -> None:
    first = _TransactionalLifecycleOperator("first")
    second = _TransactionalLifecycleOperator("second")
    chain = ChainedOneInputOperator(_LifecycleOperator(), [first, second])

    committable = chain.prepare_checkpoint(7)

    assert isinstance(committable, CompositeSinkCommittable)
    assert [item.transaction_id for item in committable.committables] == ["first", "second"]
