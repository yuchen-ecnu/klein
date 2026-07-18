# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import ast
from pathlib import Path

TEST_ROOT = Path(__file__).resolve().parents[1]
UNIT_ROOT = TEST_ROOT / "unit"
TEST_TIERS = ("architecture", "integration", "state", "unit")


def _test_modules(root: Path = TEST_ROOT) -> list[Path]:
    return sorted(root.rglob("test_*.py"))


def _is_main_guard(node: ast.If) -> bool:
    test = node.test
    return (
        isinstance(test, ast.Compare)
        and isinstance(test.left, ast.Name)
        and test.left.id == "__name__"
        and len(test.ops) == 1
        and isinstance(test.ops[0], ast.Eq)
        and len(test.comparators) == 1
        and isinstance(test.comparators[0], ast.Constant)
        and test.comparators[0].value == "__main__"
    )


def test_test_modules_live_in_explicit_tiers() -> None:
    misplaced = [
        path.relative_to(TEST_ROOT)
        for path in _test_modules()
        if path.relative_to(TEST_ROOT).parts[0] not in TEST_TIERS
    ]
    assert misplaced == []


def test_test_modules_do_not_embed_a_pytest_runner() -> None:
    offenders = []
    for path in _test_modules():
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        if any(isinstance(node, ast.If) and _is_main_guard(node) for node in ast.walk(tree)):
            offenders.append(path.relative_to(TEST_ROOT))
    assert offenders == []


def test_unit_tests_do_not_import_integration_tests() -> None:
    offenders: list[Path] = []
    for path in _test_modules(UNIT_ROOT):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        imported_modules = [
            node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) and node.module is not None
        ]
        imported_modules.extend(
            alias.name for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names
        )
        if any(name == "tests.integration" or name.startswith("tests.integration.") for name in imported_modules):
            offenders.append(path.relative_to(TEST_ROOT))
    assert offenders == []


def test_unit_tests_use_bounded_waits_and_pytest_temp_paths() -> None:
    forbidden_calls = {
        "asyncio.new_event_loop",
        "time.sleep",
        "tempfile.mkdtemp",
        "tempfile.NamedTemporaryFile",
    }
    offenders: list[str] = []
    for path in _test_modules(UNIT_ROOT):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
                continue
            if not isinstance(node.func.value, ast.Name):
                continue
            call_name = f"{node.func.value.id}.{node.func.attr}"
            if call_name in forbidden_calls:
                offenders.append(f"{path.relative_to(TEST_ROOT)}:{node.lineno} ({call_name})")
    assert offenders == []
