# SPDX-License-Identifier: Apache-2.0
"""Validate the public contents of Klein release artifacts."""

from __future__ import annotations

import argparse
import json
import re
import tarfile
import zipfile
from collections.abc import Mapping
from pathlib import Path, PurePosixPath
from urllib.parse import urlsplit

COMMON_REQUIRED_SUFFIXES = {
    "LICENSE",
    "NOTICE",
    "THIRD_PARTY_NOTICES",
}
WHEEL_REQUIRED_SUFFIXES = COMMON_REQUIRED_SUFFIXES | {
    "ray/klein/__init__.py",
    "ray/klein/py.typed",
    "ray/klein/_internal/logging.yaml",
    "ray/klein/observability/dashboard/static/index.html",
}
SDIST_REQUIRED_SUFFIXES = COMMON_REQUIRED_SUFFIXES | {
    "README.md",
    "pyproject.toml",
    "frontend/package-lock.json",
    "frontend/package.json",
    "src/ray/klein/__init__.py",
    "src/ray/klein/observability/dashboard/static/index.html",
}
SOURCE_REQUIRED_SUFFIXES = COMMON_REQUIRED_SUFFIXES | {
    "README.md",
    "pyproject.toml",
    "frontend/package-lock.json",
    "frontend/package.json",
    "frontend/src/main.tsx",
    "src/ray/klein/__init__.py",
}
FORBIDDEN_PARTS = {
    ".git",
    ".github",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    "__pycache__",
    "_build",
    "coverage",
    "node_modules",
}
SENSITIVE_BASENAMES = {".env", ".npmrc", ".pypirc", "id_rsa", "id_ed25519"}
SENSITIVE_SUFFIXES = (".key", ".p12", ".pem", ".pfx", ".pyc", ".pyo", ".mo", ".ds_store")
PUBLIC_PACKAGE_HOSTS = {"codeload.github.com", "github.com", "registry.npmjs.org"}
PRIVATE_ARTIFACT_LABELS = {"artifactory", "harbor", "nexus"}
MAX_SCANNED_FILE_BYTES = 8 * 1024 * 1024
URL_PATTERN = re.compile(rb"(?:https?|git\+https|redis)://[^\s\"'<>\\]+")
URL_CREDENTIAL_PATTERN = re.compile(rb"://[^/\s:@]+:[^/\s@]+@")
SECRET_PATTERNS = (
    re.compile(rb"-----BEGIN " + rb"(?:EC |OPENSSH |RSA )?PRIVATE KEY-----"),
    re.compile(rb"\bAKIA[0-9A-Z]{16}\b"),
    re.compile(rb"\bgh[oprsu]_[A-Za-z0-9_]{30,}\b"),
    re.compile(rb"\bglpat-[A-Za-z0-9_-]{20,}\b"),
    re.compile(rb"\bxox[baprs]-[A-Za-z0-9-]{10,}\b"),
    re.compile(rb"\bAIza[0-9A-Za-z_-]{35}\b"),
    re.compile(rb"\bsk_(?:live|test)_[A-Za-z0-9]{16,}\b"),
    re.compile(rb"\bya29\.[0-9A-Za-z_-]{20,}\b"),
)


def _assert_required(names: set[str], required_suffixes: set[str], artifact: Path) -> None:
    missing = sorted(suffix for suffix in required_suffixes if not any(name.endswith(suffix) for name in names))
    if missing:
        raise ValueError(f"{artifact.name} is missing required files: {', '.join(missing)}")


def _assert_clean(names: set[str], artifact: Path) -> None:
    forbidden = []
    for name in names:
        path = PurePosixPath(name)
        lowered_parts = {part.lower() for part in path.parts}
        basename = path.name.lower()
        if (
            path.is_absolute()
            or ".." in path.parts
            or lowered_parts.intersection(FORBIDDEN_PARTS)
            or basename in SENSITIVE_BASENAMES
            or basename.endswith(SENSITIVE_SUFFIXES)
        ):
            forbidden.append(name)
    if forbidden:
        raise ValueError(f"{artifact.name} contains forbidden files: {', '.join(sorted(forbidden)[:10])}")


def _assert_public_contents(contents: Mapping[str, bytes], artifact: Path) -> None:
    for name, payload in contents.items():
        if name.endswith("frontend/package-lock.json"):
            _assert_public_npm_lock(payload, artifact)
        if len(payload) > MAX_SCANNED_FILE_BYTES or b"\x00" in payload:
            continue
        if URL_CREDENTIAL_PATTERN.search(payload):
            raise ValueError(f"{artifact.name} contains a URL with embedded credentials in {name}")
        if any(pattern.search(payload) for pattern in SECRET_PATTERNS):
            raise ValueError(f"{artifact.name} contains secret-like material in {name}")
        for raw_url in URL_PATTERN.findall(payload):
            _assert_public_url(raw_url.decode("utf-8", errors="replace"), artifact, name)

    notices = next(
        (payload for name, payload in contents.items() if name.endswith("THIRD_PARTY_NOTICES")),
        None,
    )
    if notices is None or b"Klein Dashboard third-party notices" not in notices:
        raise ValueError(f"{artifact.name} has missing or stale third-party notices")


def _assert_public_npm_lock(payload: bytes, artifact: Path) -> None:
    try:
        lock = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"{artifact.name} contains an invalid frontend/package-lock.json") from error
    for package_path, metadata in lock.get("packages", {}).items():
        resolved = metadata.get("resolved")
        if not resolved:
            continue
        parsed = urlsplit(resolved)
        if parsed.scheme != "https" or parsed.hostname not in PUBLIC_PACKAGE_HOSTS:
            raise ValueError(
                f"{artifact.name} contains non-public npm resolution for {package_path or '<root>'}: {resolved}"
            )
        if not metadata.get("integrity"):
            raise ValueError(f"{artifact.name} has an npm dependency without integrity metadata: {package_path}")


def _assert_public_url(url: str, artifact: Path, member_name: str) -> None:
    hostname = (urlsplit(url).hostname or "").lower()
    if not hostname:
        return
    labels = set(hostname.split("."))
    if labels.intersection(PRIVATE_ARTIFACT_LABELS) and hostname not in PUBLIC_PACKAGE_HOSTS:
        raise ValueError(f"{artifact.name} contains a private artifact endpoint in {member_name}: {hostname}")


def _zip_contents(archive: zipfile.ZipFile, artifact: Path) -> dict[str, bytes]:
    contents: dict[str, bytes] = {}
    for info in archive.infolist():
        if info.is_dir():
            continue
        unix_mode = info.external_attr >> 16
        if unix_mode & 0o170000 == 0o120000:
            raise ValueError(f"{artifact.name} contains a symbolic link: {info.filename}")
        contents[info.filename] = archive.read(info)
    return contents


def _tar_contents(archive: tarfile.TarFile, artifact: Path) -> dict[str, bytes]:
    contents: dict[str, bytes] = {}
    for member in archive.getmembers():
        if member.isdir():
            continue
        if not member.isfile():
            raise ValueError(f"{artifact.name} contains a non-regular member: {member.name}")
        stream = archive.extractfile(member)
        if stream is None:
            raise ValueError(f"{artifact.name} contains an unreadable member: {member.name}")
        with stream:
            contents[member.name] = stream.read()
    return contents


def check_wheel(path: Path) -> None:
    with zipfile.ZipFile(path) as archive:
        contents = _zip_contents(archive, path)
    names = set(contents)
    _assert_required(names, WHEEL_REQUIRED_SUFFIXES, path)
    _assert_dashboard_assets(names, path)
    _assert_clean(names, path)
    _assert_public_contents(contents, path)
    if any(name.startswith(("tests/", "docs/")) for name in names):
        raise ValueError(f"{path.name} contains development-only files")
    metadata_name = next(name for name in names if name.endswith(".dist-info/METADATA"))
    metadata = contents[metadata_name].decode("utf-8")
    if "License-Expression: Apache-2.0" not in metadata:
        raise ValueError(f"{path.name} has incorrect license metadata")


def check_sdist(path: Path) -> None:
    with tarfile.open(path, mode="r:gz") as archive:
        contents = _tar_contents(archive, path)
    names = set(contents)
    _assert_required(names, SDIST_REQUIRED_SUFFIXES, path)
    _assert_dashboard_assets(names, path)
    _assert_clean(names, path)
    _assert_public_contents(contents, path)


def check_source_release(path: Path) -> None:
    with tarfile.open(path, mode="r:gz") as archive:
        contents = _tar_contents(archive, path)
    names = set(contents)
    _assert_required(names, SOURCE_REQUIRED_SUFFIXES, path)
    _assert_clean(names, path)
    _assert_public_contents(contents, path)
    generated = sorted(name for name in names if "/observability/dashboard/static/" in f"/{name}")
    if generated:
        raise ValueError(f"{path.name} canonical source contains compiled Dashboard assets")


def _assert_dashboard_assets(names: set[str], artifact: Path) -> None:
    dashboard_assets = {name for name in names if "/ray/klein/observability/dashboard/static/assets/" in f"/{name}"}
    missing_types = [
        suffix for suffix in (".css", ".js") if not any(name.endswith(suffix) for name in dashboard_assets)
    ]
    if missing_types:
        raise ValueError(f"{artifact.name} is missing Klein Dashboard assets: {', '.join(missing_types)}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifacts", nargs="+", type=Path)
    args = parser.parse_args()
    wheel_count = 0
    sdist_count = 0
    source_count = 0
    for artifact in args.artifacts:
        if artifact.suffix == ".whl":
            check_wheel(artifact)
            wheel_count += 1
        elif artifact.name.endswith("-src.tar.gz"):
            check_source_release(artifact)
            source_count += 1
        elif artifact.name.endswith(".tar.gz"):
            check_sdist(artifact)
            sdist_count += 1
        else:
            raise ValueError(f"unsupported distribution artifact: {artifact}")
    if wheel_count != 1 or sdist_count != 1 or source_count > 1:
        raise ValueError(
            "expected one wheel, one Python sdist, and at most one canonical source archive; "
            f"got {wheel_count} wheel(s), {sdist_count} sdist(s), {source_count} source archive(s)"
        )


if __name__ == "__main__":
    main()
