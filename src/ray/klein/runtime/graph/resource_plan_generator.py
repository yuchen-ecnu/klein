# SPDX-License-Identifier: Apache-2.0
"""Compile a pipeline entrypoint into a resource plan without submitting a job."""

import os
import runpy
import sys
from pathlib import Path
from unittest.mock import patch

from ray.klein.config.environment_variables import EnvironmentVariables
from ray.klein.runtime.graph.dependency_resolver import DependencyResolver


def generate_resource_plan(
    working_directory: str,
    entrypoint: str,
    output_path: str,
) -> None:
    """Execute ``entrypoint`` in compile-only mode and write its resource plan."""

    root = Path(working_directory).resolve()
    entrypoint_path = Path(entrypoint)
    if not entrypoint_path.is_absolute():
        entrypoint_path = root / entrypoint_path
    destination = Path(output_path).resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    environment = {
        EnvironmentVariables.DEBUG: "1",
        EnvironmentVariables.COMPILE_ONLY: "1",
        EnvironmentVariables.RESOURCE_PLAN_OUTPUT: str(destination),
    }
    mock_modules = DependencyResolver(root).mock_modules(entrypoint_path)
    original_argv = sys.argv[:]
    original_directory = Path.cwd()
    sys.path.insert(0, str(root))
    try:
        os.chdir(root)
        sys.argv = [str(entrypoint_path)]
        with patch.dict(os.environ, environment, clear=False), patch.dict(sys.modules, mock_modules, clear=False):
            runpy.run_path(str(entrypoint_path), run_name="__main__")
    finally:
        sys.argv = original_argv
        os.chdir(original_directory)
        sys.path.pop(0)
