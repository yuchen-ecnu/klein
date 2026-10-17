# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import runpy
from copy import deepcopy
from pathlib import Path

import pytest
import yaml

PROJECT_ROOT = Path(__file__).parents[2]
POLICY = runpy.run_path(str(PROJECT_ROOT / "scripts" / "check_release_evidence.py"))
COMMIT = "a" * 40


def _run() -> dict:
    return {
        "id": 7,
        "html_url": "https://github.com/example/project/actions/runs/7",
        "event": "workflow_dispatch",
        "path": ".github/workflows/release.yml",
        "head_sha": COMMIT,
        "head_branch": "main",
        "repository": {"full_name": "example/project"},
        "actor": {"login": "alice"},
        "triggering_actor": {"login": "alice"},
    }


def _validate_dispatch(run: dict) -> dict:
    return POLICY["_validate_dispatch"](
        run, repository="example/project", commit=COMMIT, maintainers={"alice"}, default_branch="main"
    )


def test_one_current_maintainer_can_dispatch_a_release() -> None:
    assert _validate_dispatch(_run())["actor"] == "alice"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("event", "pull_request"),
        ("path", ".github/workflows/other.yml"),
        ("head_sha", "b" * 40),
        ("head_branch", "candidate"),
        ("repository", {"full_name": "other/project"}),
        ("actor", {"login": "mallory"}),
        ("triggering_actor", {"login": "mallory"}),
        ("triggering_actor", {}),
    ],
)
def test_release_rejects_untrusted_dispatch_or_rerun(field: str, value: object) -> None:
    run = _run()
    run[field] = value
    with pytest.raises(ValueError):
        _validate_dispatch(run)


def _check(identifier: int, *, status: str = "completed", conclusion: str | None = "success") -> dict:
    return {
        "id": identifier,
        "name": "CI / required gate",
        "status": status,
        "conclusion": conclusion,
        "head_sha": COMMIT,
        "app": {"slug": "github-actions"},
        "completed_at": "2026-10-17T08:00:00Z" if status == "completed" else None,
    }


def _validate_checks(runs: list[dict]) -> list[dict]:
    return POLICY["_validate_checks"](runs, ["CI / required gate"], COMMIT)


def test_required_checks_accept_success_for_the_exact_commit() -> None:
    assert _validate_checks([_check(1)])[0]["id"] == 1


@pytest.mark.parametrize("newer", [_check(2, status="in_progress", conclusion=None), _check(2, conclusion="failure")])
def test_new_check_reruns_supersede_an_older_success(newer: dict) -> None:
    with pytest.raises(ValueError, match="did not succeed"):
        _validate_checks([_check(1), deepcopy(newer)])


@pytest.mark.parametrize("field,value", [("head_sha", "b" * 40), ("app", {"slug": "other-app"})])
def test_checks_must_belong_to_the_commit_and_github_actions(field: str, value: object) -> None:
    run = _check(1)
    run[field] = value
    with pytest.raises(ValueError, match="unexpected commit or publisher"):
        _validate_checks([run])


def test_missing_check_blocks_release() -> None:
    with pytest.raises(ValueError, match="missing"):
        _validate_checks([])


def test_clearance_requires_an_explicit_current_maintainer_attestation(tmp_path: Path) -> None:
    path = tmp_path / "IP_CLEARANCE.md"
    path.write_text("**Status: CLEARED**\n\n**Authorized by:** @Alice\n", encoding="utf-8")
    assert POLICY["_validate_clearance"](path, {"alice"}) == "alice"
    with pytest.raises(ValueError, match="current maintainer"):
        POLICY["_validate_clearance"](path, {"bob"})
    path.write_text("**Status: PENDING**\n", encoding="utf-8")
    with pytest.raises(ValueError, match="CLEARED"):
        POLICY["_validate_clearance"](path, {"alice"})


def test_maintainer_roster_excludes_other_roles(tmp_path: Path) -> None:
    path = tmp_path / "COMMUNITY.md"
    path.write_text("### Maintainers\n- @alice\n### Committers\n- @bob\n", encoding="utf-8")
    assert POLICY["_maintainers"](path) == {"alice"}


def test_release_workflow_separates_build_and_publish_privileges() -> None:
    workflow = yaml.load((PROJECT_ROOT / ".github/workflows/release.yml").read_text(), Loader=yaml.BaseLoader)
    jobs = workflow["jobs"]
    assert list(workflow["on"]) == ["workflow_dispatch"]
    assert workflow["permissions"] == {"contents": "read"}
    assert "id-token" not in jobs["verify"]["permissions"]
    assert "permissions" not in jobs["build"]
    assert jobs["publish"]["needs"] == "build"
    assert jobs["publish"]["environment"]["name"] == "pypi"
    assert jobs["publish"]["permissions"]["id-token"] == "write"
    assert jobs["github-release"]["needs"] == "publish"
    assert all(not step.get("uses", "").startswith("actions/checkout@") for step in jobs["publish"]["steps"])
