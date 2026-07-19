# SPDX-License-Identifier: Apache-2.0
"""Guardrails for the standalone package's Ray compatibility contract."""

import ast
from pathlib import Path

from ray.data import Dataset

import ray

SOURCE_ROOT = Path(__file__).parents[2] / "src" / "ray" / "klein"
COMPAT_ROOT = SOURCE_ROOT / "_compat"
PRIVATE_RAY_PREFIXES = ("ray._private", "ray.data._internal", "ray.air")
ALLOWED_PRIVATE_IMPORTS = {
    (
        "_compat/ray_data_expression.py",
        "ray.data._internal.execution.interfaces.task_context",
        "TaskContext",
    ),
    (
        "_compat/ray_data_expression.py",
        "ray.data._internal.planner.plan_expression.expression_evaluator",
        "eval_expr",
    ),
    (
        "_compat/ray_data_expression.py",
        "ray.data._internal.planner.plan_expression.expression_visitors",
        "_CallableClassUDFCollector",
    ),
    (
        "_compat/ray_data_expression.py",
        "ray.data._internal.util",
        "RetryingPyFileSystem",
    ),
    (
        "_compat/ray_data_expression.py",
        "ray.data.datasource.path_util",
        "_resolve_paths_and_filesystem",
    ),
    (
        "_compat/ray_data_expression.py",
        "ray.data.datasource.path_util",
        "_validate_and_wrap_filesystem",
    ),
}


def _python_trees():
    assert SOURCE_ROOT.is_dir(), f"source root does not exist: {SOURCE_ROOT}"
    for path in SOURCE_ROOT.rglob("*.py"):
        yield path, ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def test_compatibility_guard_scans_the_real_package_tree():
    paths = [path for path, _tree in _python_trees()]
    assert len(paths) >= 300
    assert SOURCE_ROOT / "api" / "data_stream.py" in paths
    assert COMPAT_ROOT / "ray_data_expression.py" in paths


def test_private_ray_imports_are_isolated_and_inventory_is_exact():
    discovered = set()
    for path, tree in _python_trees():
        relative_path = path.relative_to(SOURCE_ROOT).as_posix()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name.startswith(PRIVATE_RAY_PREFIXES):
                        discovered.add((relative_path, alias.name, ""))
            elif isinstance(node, ast.ImportFrom) and node.module:
                for alias in node.names:
                    if node.module.startswith(PRIVATE_RAY_PREFIXES) or (
                        node.module.startswith("ray.")
                        and alias.name.startswith("_")
                        and not node.module.startswith("ray.klein")
                    ):
                        discovered.add((relative_path, node.module, alias.name))
    assert discovered == ALLOWED_PRIVATE_IMPORTS


def test_referenced_ray_data_symbols_exist():
    missing = []
    for path, tree in _python_trees():
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Attribute)
                and isinstance(node.value, ast.Attribute)
                and isinstance(node.value.value, ast.Name)
                and node.value.value.id == "ray"
                and node.value.attr == "data"
                and not hasattr(ray.data, node.attr)
            ):
                missing.append(f"{path.relative_to(SOURCE_ROOT)}:{node.lineno}: ray.data.{node.attr}")
            if (
                isinstance(node, ast.Attribute)
                and isinstance(node.value, ast.Name)
                and node.value.id == "Dataset"
                and not hasattr(Dataset, node.attr)
            ):
                missing.append(f"{path.relative_to(SOURCE_ROOT)}:{node.lineno}: Dataset.{node.attr}")
    assert not missing, "Ray Data symbols are unavailable:\n" + "\n".join(missing)
