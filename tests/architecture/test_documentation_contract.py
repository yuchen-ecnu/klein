# SPDX-License-Identifier: Apache-2.0
import ast
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
PACKAGE_ROOT = PROJECT_ROOT / "src" / "ray" / "klein"
DOCS_ROOT = PROJECT_ROOT / "docs"


def _export_names(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        if any(isinstance(target, ast.Name) and target.id == "_EXPORTS" for target in node.targets):
            return set(ast.literal_eval(node.value))
    return set()


def _api_reference_text() -> str:
    return "\n".join(path.read_text(encoding="utf-8") for path in sorted((DOCS_ROOT / "api").glob("*.rst")))


def test_top_level_exports_are_accounted_for_in_api_reference() -> None:
    missing = sorted(name for name in _export_names(PACKAGE_ROOT / "__init__.py") if name not in _api_reference_text())
    assert not missing, f"Top-level exports missing from API reference: {missing}"


def test_datastream_reference_lists_every_public_member() -> None:
    source_path = PACKAGE_ROOT / "api" / "data_stream.py"
    tree = ast.parse(source_path.read_text(encoding="utf-8"), filename=str(source_path))
    data_stream = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == "DataStream")
    members = {
        node.name
        for node in data_stream.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and not node.name.startswith("_")
    }
    for node in data_stream.body:
        if not isinstance(node, ast.Assign):
            continue
        members.update(
            target.id for target in node.targets if isinstance(target, ast.Name) and not target.id.startswith("_")
        )

    reference = (DOCS_ROOT / "api" / "datastream.rst").read_text(encoding="utf-8")
    missing = sorted(member for member in members if f"DataStream.{member}" not in reference)
    assert not missing, f"DataStream members missing from API reference: {missing}"


def test_configuration_reference_lists_every_declared_option() -> None:
    keys: set[str] = set()
    for path in (PACKAGE_ROOT / "config").glob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == "ConfigOption"
                and node.args
            ):
                continue
            value = ast.literal_eval(node.args[0])
            if isinstance(value, str):
                keys.add(value)

    reference = (DOCS_ROOT / "configuration-reference.md").read_text(encoding="utf-8")
    missing = sorted(key for key in keys if f"`{key}`" not in reference)
    assert not missing, f"Configuration options missing from reference: {missing}"


def test_standalone_examples_are_valid_python() -> None:
    for path in sorted((PROJECT_ROOT / "examples").glob("*.py")):
        compile(path.read_text(encoding="utf-8"), str(path), "exec")


def test_documented_cli_covers_operations_commands() -> None:
    observability = (DOCS_ROOT / "observability.md").read_text(encoding="utf-8")
    assert "ray-klein stop" in observability
    assert "ray-klein cancel" in observability
    assert "`/#/klein`" in observability


def test_restore_guide_uses_the_canonical_option() -> None:
    recovery = (DOCS_ROOT / "checkpoint-recovery.md").read_text(encoding="utf-8")
    driver_fault_tolerance = (DOCS_ROOT / "driver-fault-tolerance.md").read_text(encoding="utf-8")
    assert "execution.savepoint.path" in recovery
    assert "execution.savepoint.path" in driver_fault_tolerance
    assert "`execution.checkpointing.restore-path`" not in driver_fault_tolerance
