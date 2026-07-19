# SPDX-License-Identifier: Apache-2.0
"""Logical graph optimizer."""

from ray.klein._internal.logging import get_logger
from ray.klein.config.configuration import Configuration
from ray.klein.config.pipeline_options import PipelineOptions
from ray.klein.runtime.graph.logical_graph import LogicalGraph
from ray.klein.runtime.graph.rules import ChainingRule, LogicalRule, UnionRule

DEFAULT_RULES: tuple[type[LogicalRule], ...] = (UnionRule, ChainingRule)

logger = get_logger(__name__)


class LogicalOptimizer:
    """Apply pure rewrite rules to a logical dataflow graph."""

    def __init__(self, config: Configuration) -> None:
        self._config = config

    @property
    def rules(self) -> tuple[LogicalRule, ...]:
        rule_types = DEFAULT_RULES
        if not self._config.get(PipelineOptions.OPERATOR_CHAINING):
            rule_types = tuple(rule_type for rule_type in rule_types if rule_type is not ChainingRule)
        return tuple(rule_type() for rule_type in rule_types)

    def optimize(self, graph: LogicalGraph) -> LogicalGraph:
        """Return an optimized copy of ``graph``."""

        rules = self.rules
        for rule in rules:
            graph = rule.apply(graph)
            logger.debug("Applied rule %s:\n%s", type(rule).__name__, graph)
        logger.info("Optimized logical graph with %d rules", len(rules))
        logger.debug("Optimized logical graph:\n%s", graph)
        return graph
