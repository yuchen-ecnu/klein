# SPDX-License-Identifier: Apache-2.0
"""Validate the public contents of Klein wheel and source distributions."""

from __future__ import annotations

import argparse
import tarfile
import zipfile
from pathlib import Path

WHEEL_REQUIRED_SUFFIXES = {
    "ray/klein/__init__.py",
    "ray/klein/py.typed",
    "ray/klein/_internal/logging.yaml",
}
SDIST_REQUIRED_SUFFIXES = {
    "LICENSE",
    "NOTICE",
    "README.md",
    "pyproject.toml",
    "src/ray/klein/__init__.py",
}
FORBIDDEN_PARTS = {".github", "__pycache__", "docs/_build"}


def _assert_required(names: set[str], required_suffixes: set[str], artifact: Path) -> None:
    missing = sorted(suffix for suffix in required_suffixes if not any(name.endswith(suffix) for name in names))
    if missing:
        raise ValueError(f"{artifact.name} is missing required files: {', '.join(missing)}")


def _assert_clean(names: set[str], artifact: Path) -> None:
    forbidden = sorted(
        name
        for name in names
        if any(part in name or part in Path(name).parts for part in FORBIDDEN_PARTS)
        or name.endswith((".pyc", ".pyo", ".DS_Store"))
    )
    if forbidden:
        raise ValueError(f"{artifact.name} contains forbidden files: {', '.join(forbidden[:10])}")


def check_wheel(path: Path) -> None:
    with zipfile.ZipFile(path) as archive:
        names = set(archive.namelist())
        _assert_required(names, WHEEL_REQUIRED_SUFFIXES, path)
        _assert_clean(names, path)
        if any(name.startswith(("tests/", "docs/")) for name in names):
            raise ValueError(f"{path.name} contains development-only files")
        metadata_name = next(name for name in names if name.endswith(".dist-info/METADATA"))
        metadata = archive.read(metadata_name).decode("utf-8")
        if "License-Expression: Apache-2.0" not in metadata:
            raise ValueError(f"{path.name} has incorrect license metadata")


def check_sdist(path: Path) -> None:
    with tarfile.open(path, mode="r:gz") as archive:
        names = set(archive.getnames())
        _assert_required(names, SDIST_REQUIRED_SUFFIXES, path)
        _assert_clean(names, path)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifacts", nargs="+", type=Path)
    args = parser.parse_args()
    wheel_count = 0
    sdist_count = 0
    for artifact in args.artifacts:
        if artifact.suffix == ".whl":
            check_wheel(artifact)
            wheel_count += 1
        elif artifact.name.endswith(".tar.gz"):
            check_sdist(artifact)
            sdist_count += 1
        else:
            raise ValueError(f"unsupported distribution artifact: {artifact}")
    if wheel_count != 1 or sdist_count != 1:
        raise ValueError(f"expected one wheel and one sdist, got {wheel_count} wheel(s), {sdist_count} sdist(s)")


if __name__ == "__main__":
    main()
