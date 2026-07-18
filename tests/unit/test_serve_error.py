# SPDX-License-Identifier: Apache-2.0
import pytest

from ray.klein.api.functions.logical_function import LogicalFunction
from ray.klein.runtime.serve import instantiate_logical_functions
from ray.klein.runtime.serve_extract import run_extraction


class ConstructorRaises:
    def __init__(self):
        raise RuntimeError("constructor failed")

    def __call__(self, data):
        return data


def test_instantiation_error_names_the_operator() -> None:
    functions = [
        LogicalFunction(lambda value: value + 1),
        LogicalFunction(ConstructorRaises),
        LogicalFunction(lambda value: value * 2),
    ]

    with pytest.raises(RuntimeError, match=r"Failed to instantiate operator.*ConstructorRaises"):
        instantiate_logical_functions(functions)


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
