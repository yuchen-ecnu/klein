# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import runpy
import subprocess
import tarfile
from pathlib import Path

PROJECT_ROOT = Path(__file__).parents[2]
SOURCE_RELEASE = runpy.run_path(str(PROJECT_ROOT / "scripts" / "build_source_release.py"))


def _git(repository: Path, *arguments: str) -> None:
    subprocess.run(["git", *arguments], cwd=repository, check=True, capture_output=True)


def test_source_release_is_reproducible_and_honors_export_ignore(
    tmp_path: Path,
    monkeypatch,
) -> None:
    repository = tmp_path / "repository"
    repository.mkdir()
    (repository / "pyproject.toml").write_text('[project]\nversion = "1.2.3"\n', encoding="utf-8")
    (repository / "README.md").write_text("public source\n", encoding="utf-8")
    (repository / "ignored.txt").write_text("generated\n", encoding="utf-8")
    (repository / ".gitattributes").write_text("ignored.txt export-ignore\n", encoding="utf-8")
    _git(repository, "init", "--quiet")
    _git(repository, "add", ".")
    _git(
        repository,
        "-c",
        "user.name=Release Test",
        "-c",
        "user.email=release@example.test",
        "commit",
        "--quiet",
        "-m",
        "source",
    )

    monkeypatch.chdir(repository)
    output = SOURCE_RELEASE["build_source_release"]("HEAD", tmp_path / "dist")
    first_bytes = output.read_bytes()
    assert int.from_bytes(first_bytes[4:8], "little") == 0

    with tarfile.open(output, mode="r:gz") as archive:
        members = archive.getmembers()
        names = {member.name for member in members}
    assert {member.mtime for member in members} == {0}
    assert "ray-klein-1.2.3/README.md" in names
    assert "ray-klein-1.2.3/ignored.txt" not in names

    assert SOURCE_RELEASE["build_source_release"]("HEAD", tmp_path / "dist").read_bytes() == first_bytes
