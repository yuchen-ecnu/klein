# SPDX-License-Identifier: Apache-2.0
"""Resolve local imports for compile-only graph generation."""

import ast
import importlib.util
from pathlib import Path
from unittest.mock import Mock


class DependencyResolver:
    """Find missing imports reachable from a local Python entrypoint."""

    def __init__(self, working_directory: str | Path) -> None:
        self._root = Path(working_directory).resolve()

    def mock_modules(self, entrypoint: str | Path) -> dict[str, Mock]:
        entrypoint_path = self._entrypoint_path(entrypoint)
        missing: set[str] = set()
        self._visit_file(entrypoint_path, set(), missing)
        modules: dict[str, Mock] = {}
        for module_name in missing:
            for depth in range(1, len(module_name.split(".")) + 1):
                qualified_name = ".".join(module_name.split(".")[:depth])
                module = modules.setdefault(qualified_name, Mock())
                module.__path__ = []
        return modules

    def _visit_file(self, path: Path, visited: set[Path], missing: set[str]) -> None:
        if path in visited:
            return
        visited.add(path)
        for module_name in self._imports(path):
            local_path = self._local_module_path(module_name)
            if local_path is not None:
                self._visit_file(local_path, visited, missing)
            elif not self._module_exists(module_name):
                missing.add(module_name)

    @staticmethod
    def _imports(path: Path) -> tuple[str, ...]:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        imports: list[str] = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imports.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imports.append(node.module)
        return tuple(imports)

    def _local_module_path(self, module_name: str) -> Path | None:
        module_path = self._root.joinpath(*module_name.split("."))
        candidates = (module_path.with_suffix(".py"), module_path / "__init__.py")
        return next((candidate for candidate in candidates if candidate.is_file()), None)

    @staticmethod
    def _module_exists(module_name: str) -> bool:
        try:
            return importlib.util.find_spec(module_name.split(".", 1)[0]) is not None
        except (ImportError, ValueError):
            return False

    def _entrypoint_path(self, entrypoint: str | Path) -> Path:
        path = Path(entrypoint)
        return path.resolve() if path.is_absolute() else (self._root / path).resolve()
