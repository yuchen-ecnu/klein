# SPDX-License-Identifier: Apache-2.0
import ast
import re
from pathlib import Path

PACKAGE_ROOT = Path(__file__).parents[2] / "src" / "ray" / "klein"
STABLE_PACKAGES = ("api", "config", "integrations", "state")
FORBIDDEN_NAMESPACES = (
    "ray.klein.core",
    "ray.klein.common",
    "ray.klein._private",
    "ray.klein._internal.utils",
    "ray.klein.runtime.common",
    "ray.klein.runtime.function",
    "ray.klein.runtime.interface",
    "ray.klein.runtime.rules",
    "ray.klein.runtime.utils",
    "ray.klein.connectors",
    "ray.klein.builtin",
)
FORBIDDEN_PACKAGE_PATHS = (
    "_internal/utils",
    "connectors",
    "builtin",
    "runtime/common",
    "runtime/function",
    "runtime/interface",
    "runtime/rules",
    "runtime/utils",
)
STDOUT_BOUNDARIES = {
    Path("integrations/console/console_output.py"),
    Path("observability/progress_view.py"),
}


def _python_modules():
    return PACKAGE_ROOT.rglob("*.py")


def _defined_public_classes(path: Path) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    return [node.name for node in tree.body if isinstance(node, ast.ClassDef) and not node.name.startswith("_")]


def _snake_case(name: str) -> str:
    words = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", words).lower()


def test_generic_layers_are_forbidden():
    for directory in ("core", "common", "_private", *FORBIDDEN_PACKAGE_PATHS):
        path = PACKAGE_ROOT / directory
        assert not path.exists() or not any(path.rglob("*.py")), directory


def test_distribution_uses_the_ray_klein_namespace():
    assert not (PACKAGE_ROOT.parents[1] / "ray_klein").exists()

    violations = []
    for path in _python_modules():
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        imported_names = []
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module:
                imported_names.append(node.module)
            elif isinstance(node, ast.Import):
                imported_names.extend(alias.name for alias in node.names)
        if any(name == "ray_klein" or name.startswith("ray_klein.") for name in imported_names):
            violations.append(str(path.relative_to(PACKAGE_ROOT)))
    assert violations == []


def test_source_uses_the_public_ray_runtime_context_api():
    source = "\n".join(path.read_text(encoding="utf-8") for path in _python_modules())
    assert "create_runtime_context" not in source


def test_source_respects_forbidden_namespace_boundaries():
    violations = []
    for path in _python_modules():
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        imported_names = []
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module:
                imported_names.append(node.module)
            elif isinstance(node, ast.Import):
                imported_names.extend(alias.name for alias in node.names)
        violations.extend(
            f"{path.relative_to(PACKAGE_ROOT)}: {imported_name}"
            for imported_name in imported_names
            if any(
                imported_name == namespace or imported_name.startswith(f"{namespace}.")
                for namespace in FORBIDDEN_NAMESPACES
            )
        )
    assert violations == []


def test_stable_modules_define_at_most_one_public_class():
    violations = []
    for package in STABLE_PACKAGES:
        for path in (PACKAGE_ROOT / package).rglob("*.py"):
            classes = _defined_public_classes(path)
            if len(classes) > 1:
                violations.append(f"{path.relative_to(PACKAGE_ROOT)}: {classes}")
    assert violations == []


def test_top_level_stable_class_matches_its_module_name():
    violations = []
    for package in STABLE_PACKAGES:
        for path in (PACKAGE_ROOT / package).glob("*.py"):
            classes = _defined_public_classes(path)
            if len(classes) == 1 and path.stem != _snake_case(classes[0]):
                violations.append(f"{path.relative_to(PACKAGE_ROOT)} defines {classes[0]}")
    assert violations == []


def test_operational_modules_use_component_scoped_klein_loggers():
    violations = []
    for path in _python_modules():
        relative = path.relative_to(PACKAGE_ROOT)
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.ImportFrom)
                and node.module == "ray.klein._internal.logging"
                and any(alias.name == "logger" for alias in node.names)
            ):
                violations.append(f"{relative}:{node.lineno}: imports the shared logger")
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                if (
                    isinstance(node.func.value, ast.Name)
                    and node.func.value.id == "logging"
                    and node.func.attr == "getLogger"
                    and relative != Path("_internal/logging.py")
                ):
                    violations.append(f"{relative}:{node.lineno}: bypasses get_logger()")
                if (
                    isinstance(node.func.value, ast.Name)
                    and node.func.value.id == "logger"
                    and node.func.attr in {"debug", "info", "warning", "error", "exception", "critical", "log"}
                    and node.args
                    and isinstance(node.args[0], ast.JoinedStr)
                ):
                    violations.append(f"{relative}:{node.lineno}: eagerly formats a log message")
    assert violations == []


def test_stdout_is_reserved_for_explicit_user_facing_boundaries():
    violations = []
    for path in _python_modules():
        relative = path.relative_to(PACKAGE_ROOT)
        if relative in STDOUT_BOUNDARIES:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "print":
                violations.append(f"{relative}:{node.lineno}: print()")
            if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
                continue
            stream = node.func.value
            if (
                node.func.attr in {"write", "writelines"}
                and isinstance(stream, ast.Attribute)
                and isinstance(stream.value, ast.Name)
                and stream.value.id == "sys"
                and stream.attr in {"stdout", "stderr"}
            ):
                violations.append(f"{relative}:{node.lineno}: sys.{stream.attr}.{node.func.attr}()")
    assert violations == []


def test_sensitive_integration_payloads_are_not_embedded_in_logs():
    source = "\n".join(path.read_text(encoding="utf-8") for path in _python_modules())
    assert "pipeline.command_stack" not in source
    assert "Commands:" not in source
