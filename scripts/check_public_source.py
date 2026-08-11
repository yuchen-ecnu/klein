# SPDX-License-Identifier: Apache-2.0
"""Reject secrets, private package endpoints, and organization-only markers."""

from __future__ import annotations

import runpy
import subprocess
from pathlib import Path

_DISTRIBUTION_POLICY = runpy.run_path(str(Path(__file__).with_name("check_distribution.py")))
MAX_SCANNED_FILE_BYTES = _DISTRIBUTION_POLICY["MAX_SCANNED_FILE_BYTES"]
SENSITIVE_BASENAMES = _DISTRIBUTION_POLICY["SENSITIVE_BASENAMES"]
SENSITIVE_SUFFIXES = _DISTRIBUTION_POLICY["SENSITIVE_SUFFIXES"]
_assert_public_contents = _DISTRIBUTION_POLICY["_assert_public_contents"]

_ORGANIZATION_ONLY_MARKERS = (
    b"xiao" + b"hongshu",
    b"npm." + b"devops",
    b"@xhs" + b":registry",
    b"@xhs" + b"/",
    b"@xhs" + b".",
)


def check_paths(paths: list[Path], *, root: Path) -> None:
    names = {path.relative_to(root).as_posix() for path in paths}
    sensitive_names = sorted(
        name
        for name in names
        if Path(name).name.lower() in SENSITIVE_BASENAMES or Path(name).name.lower().endswith(SENSITIVE_SUFFIXES)
    )
    if sensitive_names:
        raise ValueError(f"public source contains sensitive files: {', '.join(sensitive_names[:10])}")
    contents: dict[str, bytes] = {}
    for path in paths:
        if not path.is_file() or path.stat().st_size > MAX_SCANNED_FILE_BYTES:
            continue
        payload = path.read_bytes()
        relative = path.relative_to(root).as_posix()
        lowered = payload.lower()
        if any(marker in lowered for marker in _ORGANIZATION_ONLY_MARKERS):
            raise ValueError(f"public source contains organization-only metadata in {relative}")
        contents[relative] = payload
    _assert_public_contents(contents, root / "public-source-tree")


def tracked_and_unignored_paths(root: Path) -> list[Path]:
    result = subprocess.run(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"],
        cwd=root,
        check=True,
        capture_output=True,
    )
    return [root / raw.decode("utf-8") for raw in result.stdout.split(b"\0") if raw]


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    check_paths(tracked_and_unignored_paths(root), root=root)
    print("Public-source policy passed")


if __name__ == "__main__":
    main()
