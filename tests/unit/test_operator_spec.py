# SPDX-License-Identifier: Apache-2.0
from dataclasses import FrozenInstanceError

import pytest

from ray.klein._internal.frozen_mapping import FrozenMapping
from ray.klein.api.functions.logical_function import LogicalFunction
from ray.klein.api.sink_function import SinkFunction
from ray.klein.api.two_phase_commit_sink_function import TwoPhaseCommitSinkFunction
from ray.klein.runtime.operator.chained_one_input_operator import ChainedOneInputOperator
from ray.klein.runtime.operator.chained_source_operator import ChainedSourceOperator
from ray.klein.runtime.operator.operator import OneInputOperator, StreamOperator
from ray.klein.runtime.operator.operator_spec import OperatorSpec
from ray.klein.runtime.operator.operator_type import OperatorType
from ray.klein.runtime.operator.sink import CollectOperator
from ray.klein.runtime.operator.source import SourceOperator


class _OneInput(StreamOperator, OneInputOperator):
    def __init__(self, logical_function=None, *, marker=None) -> None:
        super().__init__(logical_function)
        self.marker = marker

    def process_element(self, record) -> None:
        del record


class _Source(SourceOperator):
    def run(self) -> None:
        pass

    def interrupt(self) -> None:
        pass

    def restore_state(self, state) -> None:
        del state

    def snapshot_state(self, checkpoint_id: int):
        return checkpoint_id

    def bind_record_emitter(self, on_record_emitted) -> None:
        del on_record_emitted


class _OrdinarySink(SinkFunction):
    def write(self, value) -> None:
        del value


class _TransactionalSink(TwoPhaseCommitSinkFunction):
    def write(self, value) -> None:
        del value

    def prepare_commit(self, checkpoint_id: int):
        del checkpoint_id

    def abort_current_transaction(self) -> None:
        pass


def _spec(
    operator_class=_OneInput,
    logical_function=None,
    *,
    id: int = 7,
    name: str = "operator",
    operator_type: OperatorType = OperatorType.ONE_INPUT,
    parameters=None,
    children=(),
    owns_state: bool = False,
) -> OperatorSpec:
    return OperatorSpec(
        operator_class=operator_class,
        logical_function=logical_function,
        id=id,
        name=name,
        operator_type=operator_type,
        parameters={} if parameters is None else parameters,
        children=children,
        owns_state=owns_state,
    )


def test_parameters_are_an_immutable_defensive_copy_and_spec_is_frozen() -> None:
    parameters = {"marker": "original"}

    spec = _spec(parameters=parameters)
    parameters["marker"] = "changed"

    assert isinstance(spec.parameters, FrozenMapping)
    assert dict(spec.parameters) == {"marker": "original"}
    with pytest.raises(TypeError):
        spec.parameters["marker"] = "changed"
    with pytest.raises(FrozenInstanceError):
        spec.name = "changed"


def test_build_returns_fresh_configured_operators_with_identity() -> None:
    logical = LogicalFunction(lambda row: row)
    spec = _spec(_OneInput, logical, id=11, name="configured", parameters={"marker": object()})

    first = spec.build()
    second = spec.build()

    assert isinstance(first, _OneInput)
    assert first is not second
    assert first.logical_function is logical
    assert first.marker is spec.parameters["marker"]
    assert (first.id, first.name) == (11, "configured")


def test_build_only_assigns_output_queue_to_collect_operator() -> None:
    queue = object()
    collect = _spec(
        CollectOperator,
        LogicalFunction(lambda row: row),
        operator_type=OperatorType.COLLECT,
    ).build(queue)
    ordinary = _spec().build(queue)
    without_queue = _spec(CollectOperator, operator_type=OperatorType.COLLECT).build()

    assert collect.output_queue is queue
    assert not hasattr(ordinary, "output_queue")
    assert without_queue.output_queue is None


def test_simple_classification_properties_cover_true_and_false_cases() -> None:
    source = _spec(_Source, operator_type=OperatorType.SOURCE)
    collect = _spec(CollectOperator, operator_type=OperatorType.COLLECT)
    ordinary = _spec()
    malformed_class = _spec(object())

    assert source.source
    assert not ordinary.source
    assert collect.collecting
    assert not ordinary.collecting
    assert not malformed_class.collecting
    assert not source.chained


def test_chained_classification_recurses_across_all_children() -> None:
    ordinary = _spec()
    collect = _spec(CollectOperator, operator_type=OperatorType.COLLECT)
    stateful = _spec(owns_state=True)

    assert _spec(children=(ordinary, collect)).collecting
    assert not _spec(children=(ordinary,)).collecting
    assert _spec(children=(ordinary, stateful)).stateful
    assert not _spec(children=(ordinary,)).stateful


def test_stateful_includes_direct_ownership_and_short_circuits_children() -> None:
    direct = _spec(owns_state=True)
    direct_with_children = _spec(children=(_spec(),), owns_state=True)

    assert direct.stateful
    assert direct_with_children.stateful
    assert not _spec().stateful


@pytest.mark.parametrize(
    ("function", "expected"),
    [
        (None, False),
        (lambda row: row, False),
        (_OrdinarySink, False),
        (_TransactionalSink, True),
    ],
)
def test_transactional_sink_detects_only_two_phase_commit_classes(function, expected: bool) -> None:
    logical = None if function is None else LogicalFunction(function)

    assert _spec(logical_function=logical).transactional_sink is expected


def test_transactional_sink_recurses_through_chained_children() -> None:
    ordinary = _spec(logical_function=LogicalFunction(_OrdinarySink))
    transactional = _spec(logical_function=LogicalFunction(_TransactionalSink))

    assert _spec(children=(ordinary, transactional)).transactional_sink
    assert not _spec(children=(ordinary,)).transactional_sink


def test_runtime_info_uses_default_or_logical_function_configuration() -> None:
    default = _spec().runtime_info
    logical = LogicalFunction(
        lambda rows: rows,
        batch_size=8,
        batch_timeout=2,
        batch_format="numpy",
        async_buffer_size=3,
    )

    assert not default.batch_enabled
    assert _spec(logical_function=logical).runtime_info is logical.runtime_info


@pytest.mark.parametrize(
    ("root", "expected_class"),
    [
        (_spec(_Source, operator_type=OperatorType.SOURCE, parameters={"bounded": True}), ChainedSourceOperator),
        (_spec(), ChainedOneInputOperator),
    ],
)
def test_chain_builds_the_matching_runtime_operator(root: OperatorSpec, expected_class) -> None:
    child = _spec(id=8, name="child")

    spec = OperatorSpec.chain(root, (child,), "fused")
    built = spec.build()

    assert isinstance(built, expected_class)
    assert spec.chained
    assert spec.children == (root, child)
    assert spec.operator_class is expected_class
    assert (spec.id, spec.name, spec.operator_type) == (root.id, "fused", root.operator_type)
    assert (built.id, built.name) == (root.id, "fused")
    assert (built._root_operator.id, built._root_operator.name) == (root.id, root.name)
    assert [(operator.id, operator.name) for operator in built.operators] == [(child.id, child.name)]


def test_chain_propagates_state_owned_by_root_or_successor() -> None:
    root_state = OperatorSpec.chain(_spec(owns_state=True), (_spec(),), "root-state")
    child_state = OperatorSpec.chain(_spec(), (_spec(owns_state=True),), "child-state")
    stateless = OperatorSpec.chain(_spec(), (_spec(),), "stateless")

    assert root_state.owns_state
    assert child_state.owns_state
    assert not stateless.owns_state


@pytest.mark.parametrize("operator_type", [OperatorType.TWO_INPUT, OperatorType.SINK, OperatorType.COLLECT])
def test_chain_rejects_unsupported_root_operator_types(operator_type: OperatorType) -> None:
    with pytest.raises(ValueError, match=f"Operator type `{operator_type}` cannot be chained"):
        OperatorSpec.chain(_spec(operator_type=operator_type), (_spec(),), "invalid")
