# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import numpy
import pytest

from ray.klein.api.klein_context import KleinContext
from ray.klein.config.configuration import Configuration
from ray.klein.config.execution_options import ExecutionOptions
from ray.klein.config.pipeline_options import PipelineOptions
from ray.klein.config.runtime_execution_mode import RuntimeExecutionMode
from tests.support.assertions import assert_rows_equal
from tests.support.terminal import execute_terminal


class _MultiplyField:
    def __init__(self, factor: int, *, field: str) -> None:
        self._factor = factor
        self._field = field

    def __call__(self, row: dict) -> dict:
        return {**row, self._field: row[self._field] * self._factor}


class _ExpandField:
    def __init__(self, deltas: tuple[int, ...], *, field: str) -> None:
        self._deltas = deltas
        self._field = field

    def __call__(self, row: dict):
        for delta in self._deltas:
            yield {**row, self._field: row[self._field] + delta}


class _DivisibleField:
    def __init__(self, divisor: int, *, field: str) -> None:
        self._divisor = divisor
        self._field = field

    def __call__(self, row: dict) -> bool:
        return row[self._field] % self._divisor == 0


def _add_batch_offset(batch: dict[str, numpy.ndarray]) -> dict[str, numpy.ndarray]:
    return {"id": batch["id"], "value": batch["value"] + 5}


def _source(context: KleinContext, source_api: str, rows: list[dict]):
    if source_api == "from_items":
        return context.from_items(rows)
    return context.from_values(*rows)


@pytest.mark.parametrize("source_api", ["from_items", "from_values"])
@pytest.mark.parametrize("mode", [RuntimeExecutionMode.BATCH, RuntimeExecutionMode.STREAMING])
@pytest.mark.parametrize("operator_chaining", [False, True])
def test_python_operator_composition_matches_across_sources_modes_and_runtime_options(
    source_api: str,
    mode: RuntimeExecutionMode,
    operator_chaining: bool,
) -> None:
    config = Configuration()
    config.set(ExecutionOptions.MODE, mode)
    config.set(PipelineOptions.OPERATOR_CHAINING, operator_chaining)
    config.set(PipelineOptions.COLUMNAR_PASSTHROUGH_ENABLED, not operator_chaining)
    context = KleinContext(config)
    concurrency = 2 if operator_chaining else 1

    primary = _source(
        context,
        source_api,
        [
            {"id": 1, "value": 1},
            {"id": 2, "value": 2},
            {"id": 3, "value": 3},
        ],
    )
    extra = _source(context, source_api, [{"id": 99, "value": 40}])
    transformed = (
        primary.map(
            _MultiplyField,
            fn_constructor_args=[10],
            fn_constructor_kwargs={"field": "value"},
            num_cpus=0.1,
            concurrency=concurrency,
        )
        .flat_map(
            _ExpandField,
            fn_constructor_args=[(0, 1)],
            fn_constructor_kwargs={"field": "value"},
            num_cpus=0.1,
            concurrency=concurrency,
        )
        .filter(
            _DivisibleField,
            fn_constructor_args=[2],
            fn_constructor_kwargs={"field": "value"},
            num_cpus=0.1,
            concurrency=concurrency,
        )
    )
    transformed.round_robin()

    sink = (
        transformed.union(extra)
        .map_batches(
            _add_batch_offset,
            batch_size=2,
            batch_format="numpy",
            num_cpus=0.1,
            concurrency=concurrency,
        )
        .take_all()
    )
    actual = execute_terminal(sink, job_name=f"composition-{source_api}-{mode.value}-{operator_chaining}")

    assert_rows_equal(
        actual,
        [
            {"id": 1, "value": 15},
            {"id": 2, "value": 25},
            {"id": 3, "value": 35},
            {"id": 99, "value": 45},
        ],
        order_sensitive=False,
    )
