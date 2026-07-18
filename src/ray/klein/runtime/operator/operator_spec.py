# SPDX-License-Identifier: Apache-2.0
"""Immutable operator recipes carried by the logical graph."""

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from ray.klein._internal.frozen_mapping import FrozenMapping
from ray.klein.api.functions.logical_function import LogicalFunction
from ray.klein.api.runtime_info import RuntimeInfo
from ray.klein.runtime.operator.operator import StreamOperator
from ray.klein.runtime.operator.operator_type import OperatorType


@dataclass(frozen=True, slots=True)
class OperatorSpec:
    """Recipe to (re)build one operator. Immutable, picklable, shareable.

    ``parameters`` carries the few non-function constructor arguments (``key_selector``,
    ``missing_data_strategy``, ``bounded``). ``children`` is non-empty only for a
    chained operator, where ``operator_class`` is a ChainedOperator subclass and the
    children are the (root, *succeeding) specs to assemble.
    """

    operator_class: type[StreamOperator]
    logical_function: LogicalFunction | None
    id: int
    name: str
    operator_type: OperatorType
    parameters: Mapping[str, Any] = field(default_factory=FrozenMapping)
    children: tuple["OperatorSpec", ...] = ()
    owns_state: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "parameters", FrozenMapping(self.parameters))

    @property
    def chained(self) -> bool:
        return bool(self.children)

    @property
    def collecting(self) -> bool:
        from ray.klein.runtime.operator.sink import CollectOperator

        if self.chained:
            return any(child.collecting for child in self.children)
        return isinstance(self.operator_class, type) and issubclass(self.operator_class, CollectOperator)

    @property
    def source(self) -> bool:
        return self.operator_type == OperatorType.SOURCE

    @property
    def stateful(self) -> bool:
        return self.owns_state or any(child.stateful for child in self.children)

    @property
    def transactional_sink(self) -> bool:
        """Whether this operator or one of its chained children is a 2PC sink."""

        from ray.klein.api.two_phase_commit_sink_function import TwoPhaseCommitSinkFunction

        if self.chained:
            return any(child.transactional_sink for child in self.children)
        function = None if self.logical_function is None else self.logical_function.function
        return isinstance(function, type) and issubclass(function, TwoPhaseCommitSinkFunction)

    @property
    def runtime_info(self) -> "RuntimeInfo":
        return RuntimeInfo() if self.logical_function is None else self.logical_function.runtime_info

    def build(self, output_queue=None) -> StreamOperator:
        """Construct a fresh runtime operator. Never opened here."""
        if self.chained:
            operator = self._build_chained(output_queue)
        else:
            operator = self.operator_class(self.logical_function, **dict(self.parameters))
            self._assign_output_queue(operator, output_queue)
        operator.id = self.id
        operator.name = self.name
        return operator

    def _build_chained(self, output_queue) -> StreamOperator:
        from ray.klein.runtime.operator.chained_operator import ChainedOperator

        root = self.children[0].build(output_queue)
        succeeding = [child.build(output_queue) for child in self.children[1:]]
        return ChainedOperator.compose(root, succeeding)

    @staticmethod
    def _assign_output_queue(operator: StreamOperator, output_queue) -> None:
        from ray.klein.runtime.operator.sink import CollectOperator

        if output_queue is not None and isinstance(operator, CollectOperator):
            operator.assign_output_queue(output_queue)

    @staticmethod
    def chain(root: "OperatorSpec", succeeding: tuple["OperatorSpec", ...], name: str) -> "OperatorSpec":
        """Build a chained spec from a root spec and its succeeding specs.

        Mirrors ``ChainedOperator.compose``: a source root yields a
        chained-source operator, a one-input root a chained-one-input operator.
        The chained spec keeps the root's id and operator_type; ``name`` is the
        fused name produced by the chaining rule.
        """
        from ray.klein.runtime.operator.chained_one_input_operator import ChainedOneInputOperator
        from ray.klein.runtime.operator.chained_source_operator import ChainedSourceOperator

        if root.operator_type == OperatorType.SOURCE:
            chained_class: type[StreamOperator] = ChainedSourceOperator
        elif root.operator_type == OperatorType.ONE_INPUT:
            chained_class = ChainedOneInputOperator
        else:
            raise ValueError(f"Operator type `{root.operator_type}` cannot be chained.")
        return OperatorSpec(
            operator_class=chained_class,
            logical_function=root.logical_function,
            id=root.id,
            name=name,
            operator_type=root.operator_type,
            children=(root, *succeeding),
            owns_state=root.stateful or any(child.stateful for child in succeeding),
        )
