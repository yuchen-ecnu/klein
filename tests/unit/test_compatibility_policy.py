# SPDX-License-Identifier: Apache-2.0
"""Guardrails for the standalone package's Ray compatibility contract."""

import ast
from pathlib import Path

from ray.data import Dataset

import ray

SOURCE_ROOT = Path(__file__).parents[2] / "src" / "ray.klein"
PRIVATE_RAY_PREFIXES = ("ray._private", "ray.data._internal", "ray.air")


def _python_trees():
    for path in SOURCE_ROOT.rglob("*.py"):
        yield path, ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def test_package_does_not_import_ray_private_modules():
    violations = []
    for path, tree in _python_trees():
        for node in ast.walk(tree):
            modules = []
            if isinstance(node, ast.Import):
                modules.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                modules.append(node.module)
            violations.extend(
                f"{path.relative_to(SOURCE_ROOT)}:{node.lineno}: {module}"
                for module in modules
                if module.startswith(PRIVATE_RAY_PREFIXES)
            )
    assert not violations, "Ray private imports are forbidden:\n" + "\n".join(violations)


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
