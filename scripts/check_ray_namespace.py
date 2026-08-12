# SPDX-License-Identifier: Apache-2.0
"""Reject a target Ray distribution that already owns Klein's namespace."""

from __future__ import annotations

from collections.abc import Iterable
from importlib.metadata import Distribution, PackageNotFoundError, distribution
from pathlib import PurePosixPath


def _conflicting_paths(files: Iterable[object]) -> list[str]:
    conflicts: set[str] = set()
    for file in files:
        name = str(file).replace("\\", "/")
        parts = PurePosixPath(name).parts
        if any(
            part == "ray" and following in {"klein", "klein.py"}
            for part, following in zip(parts, parts[1:], strict=False)
        ):
            conflicts.add(name)
    return sorted(conflicts)


def check_ray_distribution(ray_distribution: Distribution | None = None) -> None:
    if ray_distribution is None:
        try:
            ray_distribution = distribution("ray")
        except PackageNotFoundError as error:
            raise ValueError("Ray is not installed; its namespace ownership cannot be verified") from error

    files = ray_distribution.files
    if files is None:
        raise ValueError("Ray does not expose installed-file metadata; its namespace ownership cannot be verified")
    conflicts = _conflicting_paths(files)
    if conflicts:
        preview = ", ".join(conflicts[:5])
        raise ValueError(
            f"Ray {ray_distribution.version} already owns the ray.klein namespace ({preview}); "
            "installing ray-klein would overwrite or merge files owned by another distribution"
        )


def main() -> None:
    try:
        check_ray_distribution()
    except ValueError as error:
        raise SystemExit(str(error)) from error
    print("Ray namespace ownership check passed")


if __name__ == "__main__":
    main()
