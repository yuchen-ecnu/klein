# SPDX-License-Identifier: Apache-2.0
import pytest

from ray.klein.api.functions.logical_function import LogicalFunction
from ray.klein.api.klein_context import KleinContext
from ray.klein.runtime.serve import instantiate_logical_functions
from ray.klein.runtime.serve_extract import run_extraction


class ConstructorRaises:
    def __init__(self):
        raise RuntimeError("constructor failed")

    def __call__(self, data):
        return data


class ClosableOperator:
    close_count = 0

    def __call__(self, data):
        return data

    def close(self) -> None:
        type(self).close_count += 1


class _ConstructorInterrupts:
    def __init__(self) -> None:
        raise KeyboardInterrupt


class _CloseInterrupts:
    def __call__(self, data):
        return data

    def close(self) -> None:
        raise SystemExit


def test_instantiation_error_names_the_operator() -> None:
    ClosableOperator.close_count = 0
    functions = [
        LogicalFunction(ClosableOperator),
        LogicalFunction(ConstructorRaises),
        LogicalFunction(lambda value: value * 2),
    ]

    with pytest.raises(RuntimeError, match=r"Failed to instantiate operator.*ConstructorRaises"):
        instantiate_logical_functions(functions)
    assert ClosableOperator.close_count == 1


def test_instantiation_base_exception_rolls_back_partial_chain() -> None:
    ClosableOperator.close_count = 0

    with pytest.raises(KeyboardInterrupt):
        instantiate_logical_functions([LogicalFunction(ClosableOperator), LogicalFunction(_ConstructorInterrupts)])

    assert ClosableOperator.close_count == 1


def test_close_base_exception_does_not_abort_remaining_cleanup() -> None:
    ClosableOperator.close_count = 0

    operators = instantiate_logical_functions([LogicalFunction(ClosableOperator), LogicalFunction(_CloseInterrupts)])
    from ray.klein.runtime.serve_functions import close_operators

    close_operators(operators)
    assert ClosableOperator.close_count == 1


def test_workflow_without_serve_region_fails(tmp_path) -> None:
    workflow = tmp_path / "workflow.py"
    workflow.write_text(
        """
from ray.klein.api.klein_context import KleinContext

context = KleinContext()
context.from_items([{"x": 1}]).map_batches(lambda batch: batch).show()
context.execute("no-serve")
""",
        encoding="utf-8",
    )

    with pytest.raises(RuntimeError, match="no ray_serve_enabled region"):
        run_extraction(str(workflow))


def test_workflow_without_execute_fails(tmp_path) -> None:
    workflow = tmp_path / "workflow.py"
    workflow.write_text(
        "from ray.klein.api.klein_context import KleinContext\ncontext = KleinContext()\n",
        encoding="utf-8",
    )

    with pytest.raises(RuntimeError, match="finished without calling execute"):
        run_extraction(str(workflow))


def test_missing_workflow_file_fails(tmp_path) -> None:
    with pytest.raises(FileNotFoundError):
        run_extraction(str(tmp_path / "missing.py"))


def test_repeated_top_level_workflow_extraction_is_isolated(tmp_path) -> None:
    workflow = tmp_path / "workflow.py"
    workflow.write_text(
        """
import ray.klein

def identity(batch):
    return batch

ray.klein.from_items([{"value": 1}]).map_batches(
    identity,
    ray_serve_enabled=True,
).show()
ray.klein.execute("serve-top-level")
""",
        encoding="utf-8",
    )
    previous = KleinContext.current()
    original = KleinContext.install(KleinContext())
    try:
        assert len(run_extraction(str(workflow))) == 1
        assert len(run_extraction(str(workflow))) == 1
        assert KleinContext.current() is original
        assert original.sinks == ()
    finally:
        KleinContext.install(previous)


@pytest.mark.parametrize(
    "exit_code, error",
    [
        ("finally:\n    raise RuntimeError('override extraction')", RuntimeError),
        ("except BaseException:\n    pass", RuntimeError),
    ],
)
def test_abandoned_extracted_chain_is_closed(tmp_path, exit_code: str, error: type[BaseException]) -> None:
    closed = tmp_path / "closed"
    workflow = tmp_path / "workflow.py"
    workflow.write_text(
        f"""
from pathlib import Path
from ray.klein.api.klein_context import KleinContext

class Closable:
    def __call__(self, batch):
        return batch

    def close(self):
        Path({str(closed)!r}).write_text("closed", encoding="utf-8")

ctx = KleinContext()
ctx.from_items([{{"value": 1}}]).map_batches(Closable, ray_serve_enabled=True).show()
try:
    ctx.execute("abandoned-extraction")
{exit_code}
""",
        encoding="utf-8",
    )

    with pytest.raises(error):
        run_extraction(str(workflow))
    assert closed.read_text(encoding="utf-8") == "closed"
