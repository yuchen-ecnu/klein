# SPDX-License-Identifier: Apache-2.0
from types import SimpleNamespace
from typing import Any, ClassVar

import pytest

from ray.klein.api.functions.logical_function import LogicalFunction
from ray.klein.api.runtime_info import RuntimeInfo
from ray.klein.api.source_context import SourceContext
from ray.klein.api.source_function import SourceFunction
from ray.klein.config.configuration import Configuration
from ray.klein.observability.metrics.metric_group import JobMetricGroup
from ray.klein.runtime.context.runtime_context import TaskRuntimeContext
from ray.klein.runtime.operator.chained_source_operator import ChainedSourceOperator
from ray.klein.runtime.operator.map_operator import MapOperator
from ray.klein.runtime.operator.source import SourceFunctionOperator


class _MutableStateSource(SourceFunction):
    def __init__(self) -> None:
        self.state: Any = {"offset": 1}
        self.completed: list[int] = []

    def run(self, context: SourceContext) -> None:
        return None

    def cancel(self) -> None:
        return None

    def snapshot_state(self, checkpoint_id: int) -> Any:
        return self.state

    def restore_state(self, state: Any) -> None:
        self.state = state

    def notify_checkpoint_complete(self, checkpoint_id: int) -> None:
        self.completed.append(checkpoint_id)


def _opened_operator(source: _MutableStateSource) -> SourceFunctionOperator:
    operator = SourceFunctionOperator(LogicalFunction(_MutableStateSource))
    operator._function = source
    operator.source_context = object()  # type: ignore[assignment]
    return operator


def test_source_checkpoint_state_is_frozen_at_the_barrier() -> None:
    source = _MutableStateSource()
    operator = _opened_operator(source)

    snapshot = operator.snapshot_state(3)
    source.state["offset"] = 2

    assert snapshot == {"offset": 1}


def test_source_checkpoint_state_must_be_pickleable() -> None:
    source = _MutableStateSource()
    source.state = {"callback": lambda: None}
    operator = _opened_operator(source)

    with pytest.raises(TypeError, match="must be pickleable"):
        operator.snapshot_state(3)


def test_source_completion_callback_receives_only_the_durable_checkpoint_id() -> None:
    source = _MutableStateSource()
    operator = _opened_operator(source)

    operator.notify_checkpoint_complete(3)

    assert source.completed == [3]


class _LifecycleSource(SourceFunction):
    instances: ClassVar[list["_LifecycleSource"]] = []

    def __init__(self) -> None:
        self.open_count = 0
        self.run_count = 0
        self.cancel_count = 0
        self.close_count = 0
        self.instances.append(self)

    def open(self, runtime_context) -> None:
        self.open_count += 1

    def run(self, context: SourceContext) -> None:
        self.run_count += 1

    def cancel(self) -> None:
        self.cancel_count += 1

    def close(self) -> None:
        self.close_count += 1

    def snapshot_state(self, checkpoint_id: int) -> None:
        return None

    def restore_state(self, state: Any) -> None:
        return None


def test_chained_source_owns_exactly_one_source_function_lifecycle() -> None:
    _LifecycleSource.instances.clear()
    root = SourceFunctionOperator(LogicalFunction(_LifecycleSource))
    root.id, root.name = 1, "source"
    downstream = MapOperator(LogicalFunction(lambda value: value))
    downstream.id, downstream.name = 2, "map"
    chained = ChainedSourceOperator(root, [downstream])
    context = TaskRuntimeContext(
        "source",
        0,
        1,
        Configuration(),
        JobMetricGroup("test").add_task_group("1:0", "source", 0),
        SimpleNamespace(),
        RuntimeInfo(),
        "test",
    )

    chained.open(None, context)
    chained.run()
    chained.interrupt()
    chained.close()

    assert len(_LifecycleSource.instances) == 1
    source = _LifecycleSource.instances[0]
    assert root._function is source
    assert chained._function is None
    assert (source.open_count, source.run_count, source.cancel_count, source.close_count) == (1, 1, 1, 1)
