# SPDX-License-Identifier: Apache-2.0
"""Build latest and tagged English/Chinese documentation into one Pages tree."""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from packaging.version import InvalidVersion, Version

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python 3.10 compatibility
    import tomli as tomllib

_RELEASE_TAG = re.compile(r"^v(?P<version>[0-9]+(?:\.[0-9A-Za-z]+)+)$")
_OUTPUT_MARKER = ".klein-versioned-docs"
_OUTPUT_MARKER_CONTENT = "managed by scripts/build_versioned_docs.py\n"


@dataclass(frozen=True, slots=True)
class DocumentationRef:
    label: str
    ref: str
    output_name: str
    version_match: str


def select_documentation_refs(tags: list[str], *, max_releases: int) -> list[DocumentationRef]:
    """Return latest plus the newest unique PEP 440 release tags."""

    if isinstance(max_releases, bool) or not isinstance(max_releases, int):
        raise TypeError("max_releases must be an integer")
    if max_releases < 0:
        raise ValueError("max_releases must be non-negative")
    parsed: dict[Version, str] = {}
    for tag in tags:
        match = _RELEASE_TAG.fullmatch(tag)
        if match is None:
            continue
        try:
            release = Version(match.group("version"))
        except InvalidVersion:
            continue
        parsed.setdefault(release, tag)
    releases = [
        DocumentationRef(str(release), parsed[release], str(release), str(release))
        for release in sorted(parsed, reverse=True)[:max_releases]
    ]
    return [DocumentationRef("latest", "HEAD", "latest", "latest"), *releases]


def version_entries(
    refs: list[DocumentationRef],
    *,
    base_url: str,
    language: str,
) -> list[dict[str, str | bool]]:
    """Create pydata-sphinx-theme switcher entries for one language."""

    if not refs:
        raise ValueError("at least one documentation ref is required")
    base = base_url.rstrip("/")
    suffix = "/zh_CN/" if language == "zh_CN" else "/"
    preferred = refs[1] if len(refs) > 1 else refs[0]
    return [
        {
            "name": ref.label,
            "version": ref.version_match,
            "url": f"{base}/{ref.output_name}{suffix}",
            "preferred": ref is preferred,
        }
        for ref in refs
    ]


def _git_tags(repository: Path) -> list[str]:
    result = subprocess.run(
        ["git", "tag", "--list", "v*"],
        cwd=repository,
        check=True,
        capture_output=True,
        text=True,
    )
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def validate_documentation_ref(repository: Path, ref: DocumentationRef) -> None:
    """Accept only signed release tags on the checked-out main lineage."""

    if ref.ref == "HEAD":
        return
    subprocess.run(["git", "verify-tag", ref.ref], cwd=repository, check=True)
    subprocess.run(["git", "merge-base", "--is-ancestor", f"{ref.ref}^{{commit}}", "HEAD"], cwd=repository, check=True)
    project_file = subprocess.run(
        ["git", "show", f"{ref.ref}:pyproject.toml"],
        cwd=repository,
        check=True,
        capture_output=True,
    ).stdout
    tagged_version = tomllib.loads(project_file.decode("utf-8"))["project"]["version"]
    if tagged_version != ref.version_match:
        raise ValueError(f"documentation tag {ref.ref!r} does not match pyproject.toml version {tagged_version!r}")


@contextmanager
def _source_tree(repository: Path, ref: DocumentationRef) -> Iterator[Path]:
    if ref.ref == "HEAD":
        yield repository
        return
    with tempfile.TemporaryDirectory(prefix="klein-docs-") as directory:
        source = Path(directory) / "source"
        subprocess.run(
            ["git", "worktree", "add", "--detach", str(source), ref.ref],
            cwd=repository,
            check=True,
        )
        try:
            yield source
        finally:
            subprocess.run(
                ["git", "worktree", "remove", "--force", str(source)],
                cwd=repository,
                check=True,
            )


def _run(command: list[str], *, source: Path, environment: dict[str, str]) -> None:
    subprocess.run(command, cwd=source, env=environment, check=True)


def _reset_output_directory(repository: Path, output: Path) -> None:
    """Create a managed output directory without deleting unrelated data."""

    if output == Path(output.anchor):
        raise ValueError("versioned documentation output cannot be a filesystem root")
    if output == repository or repository in output.parents or output in repository.parents:
        raise ValueError("versioned documentation output must not overlap the repository")
    if output.exists():
        if not output.is_dir():
            raise ValueError("versioned documentation output must be a directory")
        marker = output / _OUTPUT_MARKER
        try:
            marker_content = marker.read_text(encoding="utf-8")
        except (OSError, UnicodeError) as error:
            raise ValueError(
                "refusing to replace an output directory not created by build_versioned_docs.py"
            ) from error
        if marker_content != _OUTPUT_MARKER_CONTENT:
            raise ValueError("refusing to replace an output directory not created by build_versioned_docs.py")
        shutil.rmtree(output)
    output.mkdir(parents=True)
    (output / _OUTPUT_MARKER).write_text(_OUTPUT_MARKER_CONTENT, encoding="utf-8")


@contextmanager
def _documentation_environment(
    source: Path,
    ref: DocumentationRef,
    *,
    base_url: str,
) -> Iterator[dict[str, str]]:
    """Load ``ray.klein`` from one ref without installing over the active environment."""

    with tempfile.TemporaryDirectory(prefix="klein-docs-python-") as bootstrap_directory:
        bootstrap = Path(bootstrap_directory)
        (bootstrap / "sitecustomize.py").write_text(
            "import os\n"
            "import ray\n"
            "source_ray = os.environ['KLEIN_DOCS_SOURCE_RAY']\n"
            "if source_ray not in ray.__path__:\n"
            "    ray.__path__.insert(0, source_ray)\n",
            encoding="utf-8",
        )
        python_paths = [str(bootstrap), str(source / "src")]
        if existing_python_path := os.environ.get("PYTHONPATH"):
            python_paths.append(existing_python_path)
        yield {
            **os.environ,
            "PYTHONPATH": os.pathsep.join(python_paths),
            "KLEIN_DOCS_SOURCE_RAY": str(source / "src" / "ray"),
            "KLEIN_DOCS_OFFLINE": "1",
            "KLEIN_DOCS_VERSION": ref.version_match,
            "KLEIN_DOCS_GITHUB_REF": "main" if ref.ref == "HEAD" else ref.ref,
            "KLEIN_DOCS_BASE_URL": base_url.rstrip("/"),
        }


def _build_ref(
    source: Path,
    destination: Path,
    ref: DocumentationRef,
    *,
    base_url: str,
    translation_checker: Path,
) -> None:
    with (
        tempfile.TemporaryDirectory(prefix="klein-docs-build-") as build_directory,
        _documentation_environment(source, ref, base_url=base_url) as environment,
    ):
        build = Path(build_directory)
        gettext = build / "gettext"
        _run(
            [sys.executable, "-m", "sphinx", "-W", "--keep-going", "-b", "gettext", "docs", str(gettext)],
            source=source,
            environment={**environment, "KLEIN_DOCS_LANGUAGE": "en"},
        )
        _run(
            [
                sys.executable,
                str(translation_checker),
                str(gettext),
                "docs/locales/zh_CN/LC_MESSAGES",
            ],
            source=source,
            environment=environment,
        )
        _run(
            [sys.executable, "-m", "sphinx", "-W", "--keep-going", "-b", "html", "docs", str(destination)],
            source=source,
            environment={**environment, "KLEIN_DOCS_LANGUAGE": "en"},
        )
        _run(
            [
                sys.executable,
                "-m",
                "sphinx",
                "-W",
                "--keep-going",
                "-b",
                "html",
                "docs",
                str(destination / "zh_CN"),
            ],
            source=source,
            environment={**environment, "KLEIN_DOCS_LANGUAGE": "zh_CN"},
        )


def build_versioned_docs(
    repository: Path,
    output: Path,
    *,
    max_releases: int,
    base_url: str,
) -> list[DocumentationRef]:
    repository = repository.resolve()
    output = output.resolve()
    refs = select_documentation_refs(_git_tags(repository), max_releases=max_releases)
    for ref in refs:
        validate_documentation_ref(repository, ref)
    _reset_output_directory(repository, output)

    for ref in reversed(refs):
        with _source_tree(repository, ref) as source:
            _build_ref(
                source,
                output / ref.output_name,
                ref,
                base_url=base_url,
                translation_checker=repository / "scripts" / "check_doc_translations.py",
            )

    default_ref = refs[1] if len(refs) > 1 else refs[0]
    shutil.copytree(output / default_ref.output_name, output, dirs_exist_ok=True)
    for language, filename in (("en", "versions.json"), ("zh_CN", "versions-zh_CN.json")):
        entries = version_entries(refs, base_url=base_url, language=language)
        (output / filename).write_text(json.dumps(entries, indent=2) + "\n", encoding="utf-8")
    return refs


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", type=Path, default=Path.cwd())
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--max-releases", type=int, default=5)
    parser.add_argument("--base-url", default="https://yuchen-ecnu.github.io/klein")
    arguments = parser.parse_args()
    refs = build_versioned_docs(
        arguments.repository,
        arguments.output,
        max_releases=arguments.max_releases,
        base_url=arguments.base_url,
    )
    print("Built documentation versions: " + ", ".join(ref.label for ref in refs))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
