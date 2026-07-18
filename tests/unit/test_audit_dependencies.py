# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import importlib.util
from pathlib import Path


def _load_audit_module():
    script = Path(__file__).parents[2] / "scripts" / "audit_dependencies.py"
    spec = importlib.util.spec_from_file_location("audit_dependencies", script)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_dependency_audit_adds_every_upstream_advisory() -> None:
    module = _load_audit_module()

    command = module.build_command(("--local",))

    assert command[:4] == [module.sys.executable, "-m", "pip_audit", "--local"]
    for advisory in module.UPSTREAM_RAY_ADVISORIES:
        assert command.count(advisory) == 1


def test_dependency_audit_allowlist_matches_security_policy() -> None:
    module = _load_audit_module()
    security_policy = (Path(__file__).parents[2] / "SECURITY.md").read_text()

    assert all(advisory in security_policy for advisory in module.UPSTREAM_RAY_ADVISORIES)
