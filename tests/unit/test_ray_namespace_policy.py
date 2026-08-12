# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import runpy
from pathlib import Path, PurePosixPath

import pytest

PROJECT_ROOT = Path(__file__).parents[2]
POLICY = runpy.run_path(str(PROJECT_ROOT / "scripts" / "check_ray_namespace.py"))


class _Distribution:
    def __init__(self, files: list[str] | None) -> None:
        self.files = None if files is None else [PurePosixPath(path) for path in files]
        self.version = "2.56.1"


def test_ray_namespace_policy_accepts_an_unowned_namespace() -> None:
    POLICY["check_ray_distribution"](_Distribution(["ray/__init__.py", "ray/data/__init__.py"]))


@pytest.mark.parametrize("path", ["ray/klein/__init__.py", "ray/klein.py", "prefix/ray/klein/api.py"])
def test_ray_namespace_policy_rejects_files_owned_by_ray(path: str) -> None:
    with pytest.raises(ValueError, match=r"already owns the ray\.klein namespace"):
        POLICY["check_ray_distribution"](_Distribution([path]))


def test_ray_namespace_policy_fails_closed_without_a_record() -> None:
    with pytest.raises(ValueError, match="installed-file metadata"):
        POLICY["check_ray_distribution"](_Distribution(None))
