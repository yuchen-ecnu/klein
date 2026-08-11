# SPDX-License-Identifier: Apache-2.0
import json
import runpy
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).parents[2]
POLICY = runpy.run_path(str(PROJECT_ROOT / "scripts" / "check_distribution.py"))
PUBLIC_SOURCE_POLICY = runpy.run_path(str(PROJECT_ROOT / "scripts" / "check_public_source.py"))


def test_distribution_policy_rejects_private_npm_registry() -> None:
    lock = {
        "packages": {
            "node_modules/example": {
                "resolved": "https://" + "artifactory" + ".example.test/npm/example.tgz",
                "integrity": "sha512-example",
            }
        }
    }

    with pytest.raises(ValueError, match="non-public npm resolution"):
        POLICY["_assert_public_npm_lock"](
            json.dumps(lock).encode(),
            Path("example.tar.gz"),
        )


@pytest.mark.parametrize(
    "name",
    [
        "project/.env",
        "project/private.pem",
        "project/messages.mo",
        "project/frontend/coverage/coverage-summary.json",
        "project/docs/_build/index.html",
    ],
)
def test_distribution_policy_rejects_sensitive_or_compiled_files(name: str) -> None:
    with pytest.raises(ValueError, match="forbidden files"):
        POLICY["_assert_clean"]({name}, Path("example.tar.gz"))


def test_distribution_policy_rejects_embedded_credentials() -> None:
    contents = {
        "project/THIRD_PARTY_NOTICES": b"Klein Dashboard third-party notices",
        "project/config.txt": b"https://" + b"build-user" + b":" + b"secret@example.test/simple",
    }

    with pytest.raises(ValueError, match="embedded credentials"):
        POLICY["_assert_public_contents"](contents, Path("example.tar.gz"))


@pytest.mark.parametrize(
    "secret",
    [
        b"AKIA" + b"A" * 16,
        b"ghp_" + b"a" * 30,
        b"glpat-" + b"a" * 20,
        b"xoxb-" + b"1" * 20,
        b"AIza" + b"a" * 35,
        b"sk_live_" + b"a" * 20,
        b"ya29." + b"a" * 20,
    ],
)
def test_distribution_policy_rejects_strong_token_patterns(secret: bytes) -> None:
    contents = {
        "project/THIRD_PARTY_NOTICES": b"Klein Dashboard third-party notices",
        "project/config.txt": secret,
    }

    with pytest.raises(ValueError, match="secret-like material"):
        POLICY["_assert_public_contents"](contents, Path("example.tar.gz"))


def test_public_source_policy_rejects_organization_only_markers(tmp_path: Path) -> None:
    notices = tmp_path / "THIRD_PARTY_NOTICES"
    notices.write_text("Klein Dashboard third-party notices", encoding="utf-8")
    metadata = tmp_path / "metadata.txt"
    metadata.write_bytes(b"registry=" + b"npm." + b"devops" + b".example.test")

    with pytest.raises(ValueError, match="organization-only metadata"):
        PUBLIC_SOURCE_POLICY["check_paths"]([notices, metadata], root=tmp_path)
