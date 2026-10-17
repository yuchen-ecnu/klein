# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import runpy
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

PROJECT_ROOT = Path(__file__).parents[2]
SCRIPT = runpy.run_path(str(PROJECT_ROOT / "scripts" / "build_versioned_docs.py"))
DocumentationRef = SCRIPT["DocumentationRef"]
select_documentation_refs = SCRIPT["select_documentation_refs"]
version_entries = SCRIPT["version_entries"]
reset_output_directory = SCRIPT["_reset_output_directory"]
documentation_environment = SCRIPT["_documentation_environment"]
validate_documentation_ref = SCRIPT["validate_documentation_ref"]


def test_versioned_docs_select_latest_unique_pep440_release_tags() -> None:
    refs = select_documentation_refs(
        ["not-a-release", "v1.2.0", "v1.10.0", "v2.0.0rc1", "v2.0.0rc1"],
        max_releases=2,
    )

    assert [(ref.label, ref.ref, ref.output_name) for ref in refs] == [
        ("latest", "HEAD", "latest"),
        ("2.0.0rc1", "v2.0.0rc1", "2.0.0rc1"),
        ("1.10.0", "v1.10.0", "1.10.0"),
    ]


@pytest.mark.parametrize("max_releases", [-1, True])
def test_versioned_docs_reject_invalid_release_retention(max_releases: object) -> None:
    error = TypeError if max_releases is True else ValueError
    with pytest.raises(error):
        select_documentation_refs([], max_releases=max_releases)


def test_version_switcher_keeps_each_language_in_its_version_tree() -> None:
    refs = [
        DocumentationRef("latest", "HEAD", "latest", "latest"),
        DocumentationRef("1.2.3", "v1.2.3", "1.2.3", "1.2.3"),
    ]

    assert version_entries(refs, base_url="https://example.test/docs/", language="en") == [
        {
            "name": "latest",
            "version": "latest",
            "url": "https://example.test/docs/latest/",
            "preferred": False,
        },
        {
            "name": "1.2.3",
            "version": "1.2.3",
            "url": "https://example.test/docs/1.2.3/",
            "preferred": True,
        },
    ]
    assert version_entries(refs, base_url="https://example.test/docs", language="zh_CN")[1]["url"] == (
        "https://example.test/docs/1.2.3/zh_CN/"
    )


def test_version_switcher_requires_a_documentation_ref() -> None:
    with pytest.raises(ValueError, match="at least one"):
        version_entries([], base_url="https://example.test/docs", language="en")


def test_versioned_docs_refuses_to_delete_an_unmanaged_output(tmp_path: Path) -> None:
    repository = tmp_path / "repository"
    repository.mkdir()
    output = tmp_path / "site"
    output.mkdir()
    sentinel = output / "keep-me"
    sentinel.write_text("user data", encoding="utf-8")

    with pytest.raises(ValueError, match="refusing to replace"):
        reset_output_directory(repository, output)

    assert sentinel.read_text(encoding="utf-8") == "user data"


def test_versioned_docs_only_replaces_its_own_managed_output(tmp_path: Path) -> None:
    repository = tmp_path / "repository"
    repository.mkdir()
    output = tmp_path / "site"

    reset_output_directory(repository, output)
    stale = output / "stale.html"
    stale.write_text("stale", encoding="utf-8")
    reset_output_directory(repository, output)

    assert not stale.exists()
    assert (output / ".klein-versioned-docs").is_file()


@pytest.mark.parametrize("relative_output", [".", "parent", "child"])
def test_versioned_docs_rejects_output_that_overlaps_repository(
    tmp_path: Path,
    relative_output: str,
) -> None:
    repository = tmp_path / "parent" / "repository"
    repository.mkdir(parents=True)
    outputs = {
        ".": repository,
        "parent": repository.parent,
        "child": repository / "site",
    }

    with pytest.raises(ValueError, match="must not overlap"):
        reset_output_directory(repository, outputs[relative_output])


def test_versioned_docs_rejects_a_filesystem_root() -> None:
    with pytest.raises(ValueError, match="filesystem root"):
        reset_output_directory(Path("/tmp/repository"), Path("/"))


def test_versioned_docs_loads_klein_from_the_selected_source_tree(tmp_path: Path) -> None:
    source = tmp_path / "tag"
    package = source / "src" / "ray" / "klein"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("ORIGIN = 'selected-tag'\n", encoding="utf-8")
    ref = DocumentationRef("1.2.3", "v1.2.3", "1.2.3", "1.2.3")

    with documentation_environment(source, ref, base_url="https://example.test/docs") as environment:
        result = subprocess.run(
            [sys.executable, "-c", "import ray.klein; print(ray.klein.ORIGIN)"],
            env=environment,
            check=True,
            capture_output=True,
            text=True,
        )

    assert result.stdout.strip() == "selected-tag"


def _git(repository: Path, *arguments: str) -> None:
    subprocess.run(["git", *arguments], cwd=repository, check=True, capture_output=True)


@pytest.fixture()
def tagged_repository(tmp_path: Path) -> Path:
    _git(tmp_path, "init", "--quiet")
    _git(tmp_path, "config", "user.name", "Release Test")
    _git(tmp_path, "config", "user.email", "release@example.test")
    (tmp_path / "pyproject.toml").write_text('[project]\nversion = "1.2.3"\n')
    _git(tmp_path, "add", "pyproject.toml")
    _git(tmp_path, "commit", "--quiet", "-m", "source")
    _git(tmp_path, "tag", "-a", "v1.2.3", "-m", "Release 1.2.3")
    return tmp_path


def test_versioned_docs_accepts_annotated_version_matched_tags(tagged_repository: Path) -> None:
    validate_documentation_ref(tagged_repository, DocumentationRef("1.2.3", "v1.2.3", "1.2.3", "1.2.3"))


def test_versioned_docs_rejects_a_tag_with_mismatched_package_version(tagged_repository: Path) -> None:
    with pytest.raises(ValueError, match="does not match"):
        validate_documentation_ref(tagged_repository, DocumentationRef("1.2.3", "v1.2.3", "1.2.3", "1.2.4"))


def test_versioned_docs_rejects_lightweight_tags(tagged_repository: Path) -> None:
    _git(tagged_repository, "tag", "v1.2.4")
    with pytest.raises(ValueError, match="must be annotated"):
        validate_documentation_ref(tagged_repository, DocumentationRef("1.2.4", "v1.2.4", "1.2.4", "1.2.4"))


def test_versioned_docs_rejects_a_tag_outside_the_current_lineage(tagged_repository: Path) -> None:
    _git(tagged_repository, "checkout", "--orphan", "unrelated")
    _git(tagged_repository, "commit", "--quiet", "-m", "unrelated history")
    with pytest.raises(subprocess.CalledProcessError):
        validate_documentation_ref(tagged_repository, DocumentationRef("1.2.3", "v1.2.3", "1.2.3", "1.2.3"))


def test_documentation_workflow_keeps_tag_builds_separate_from_deploy_privileges() -> None:
    workflow_path = PROJECT_ROOT / ".github" / "workflows" / "docs.yml"
    workflow = yaml.load(workflow_path.read_text(encoding="utf-8"), Loader=yaml.BaseLoader)
    build = workflow["jobs"]["build"]
    deploy = workflow["jobs"]["deploy"]
    build_source = str(build)

    assert build["permissions"] == {"contents": "read", "pages": "read"}
    assert deploy["needs"] == "build"
    assert deploy["permissions"] == {"pages": "write", "id-token": "write"}
    assert "RELEASE_GPG_PUBLIC_KEY" not in build_source
    assert "steps.pages.outputs.base_url" in build_source
