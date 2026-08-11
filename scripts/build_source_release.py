# SPDX-License-Identifier: Apache-2.0
"""Build the canonical, reproducible source archive from a Git revision."""

from __future__ import annotations

import argparse
import gzip
import re
import subprocess
from pathlib import Path

VERSION_PATTERN = re.compile(rb'^version\s*=\s*"([^"]+)"', re.MULTILINE)


def _git(*arguments: str) -> bytes:
    return subprocess.run(
        ["git", *arguments],
        check=True,
        stdout=subprocess.PIPE,
    ).stdout


def build_source_release(ref: str, output_dir: Path) -> Path:
    pyproject = _git("show", f"{ref}:pyproject.toml")
    match = VERSION_PATTERN.search(pyproject)
    if match is None:
        raise ValueError(f"cannot determine project version at {ref}")
    version = match.group(1).decode("ascii")
    output_dir.mkdir(parents=True, exist_ok=True)
    output = output_dir / f"ray-klein-{version}-src.tar.gz"
    archive = _git(
        "archive",
        "--format=tar",
        "--mtime=1970-01-01T00:00:00Z",
        f"--prefix=ray-klein-{version}/",
        ref,
    )
    with (
        output.open("wb") as raw_output,
        gzip.GzipFile(fileobj=raw_output, mode="wb", filename="", mtime=0) as compressed,
    ):
        compressed.write(archive)
    return output


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ref", default="HEAD", help="Git revision to archive")
    parser.add_argument("--output-dir", type=Path, default=Path("dist"))
    arguments = parser.parse_args()
    print(build_source_release(arguments.ref, arguments.output_dir))


if __name__ == "__main__":
    main()
