# SPDX-License-Identifier: Apache-2.0
import ast
from contextlib import suppress
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
PACKAGE_ROOT = PROJECT_ROOT / "src" / "ray" / "klein"
DOCS_ROOT = PROJECT_ROOT / "docs"


def _export_names(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    names: set[str] = set()
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        target_names = {target.id for target in node.targets if isinstance(target, ast.Name)}
        if "_EXPORTS" in target_names:
            names.update(ast.literal_eval(node.value))
        elif "__all__" in target_names:
            with suppress(TypeError, ValueError):
                names.update(ast.literal_eval(node.value))
    return names


def _api_reference_text() -> str:
    return "\n".join(path.read_text(encoding="utf-8") for path in sorted((DOCS_ROOT / "api").glob("*.rst")))


def test_top_level_exports_are_accounted_for_in_api_reference() -> None:
    missing = sorted(name for name in _export_names(PACKAGE_ROOT / "__init__.py") if name not in _api_reference_text())
    assert not missing, f"Top-level exports missing from API reference: {missing}"


def test_public_domain_package_exports_are_accounted_for() -> None:
    package_initializers = [
        PACKAGE_ROOT / "api" / "__init__.py",
        PACKAGE_ROOT / "api" / "ray_data" / "__init__.py",
        PACKAGE_ROOT / "config" / "__init__.py",
        PACKAGE_ROOT / "formats" / "__init__.py",
        PACKAGE_ROOT / "integrations" / "console" / "__init__.py",
        PACKAGE_ROOT / "integrations" / "filesystem" / "__init__.py",
        PACKAGE_ROOT / "integrations" / "iceberg" / "__init__.py",
        PACKAGE_ROOT / "integrations" / "kafka" / "__init__.py",
        PACKAGE_ROOT / "integrations" / "redis" / "__init__.py",
        PACKAGE_ROOT / "integrations" / "rocketmq" / "__init__.py",
        PACKAGE_ROOT / "integrations" / "sql" / "__init__.py",
        PACKAGE_ROOT / "observability" / "metrics" / "__init__.py",
        PACKAGE_ROOT / "state" / "__init__.py",
    ]
    reference = _api_reference_text()
    missing = {
        str(path.relative_to(PACKAGE_ROOT)): sorted(name for name in _export_names(path) if name not in reference)
        for path in package_initializers
    }
    missing = {path: names for path, names in missing.items() if names}
    assert not missing, f"Public package exports missing from API reference: {missing}"


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
    assert "ray-klein dashboard" in observability


def test_feature_guides_have_dedicated_navigation() -> None:
    index = (DOCS_ROOT / "index.md").read_text(encoding="utf-8")
    assert ":caption: Features" in index
    feature_tree = index.split(":caption: Features", maxsplit=1)[1].split("```", maxsplit=1)[0]
    featured_guides = {
        "features",
        "ray-data-interop",
        "ray-native-state",
        "event-time",
        "sql",
        "delivery-semantics",
        "operator-rescaling",
        "driver-fault-tolerance",
    }
    missing = sorted(guide for guide in featured_guides if guide not in feature_tree)
    assert not missing, f"Feature guides missing from dedicated navigation: {missing}"

    features = (DOCS_ROOT / "features.md").read_text(encoding="utf-8")
    assert "`udf.ignore-exception=true`" in features


def test_documentation_navigation_is_grouped_by_user_task() -> None:
    index = (DOCS_ROOT / "index.md").read_text(encoding="utf-8")
    captions = [
        "Getting started",
        "Concepts",
        "Application development",
        "Features",
        "Connectors",
        "Deployment",
        "Operations",
        "Evaluation",
        "Reference",
        "Project development",
        "Internals",
    ]
    positions = [index.index(f":caption: {caption}") for caption in captions]
    assert positions == sorted(positions)

    def navigation_group(caption: str) -> str:
        return index.split(f":caption: {caption}", maxsplit=1)[1].split("```", maxsplit=1)[0]

    assert "datastream-programming-guide" in navigation_group("Application development")
    assert "connectors/index" in navigation_group("Connectors")
    assert "configuration-reference" in navigation_group("Deployment")
    assert "observability" in navigation_group("Operations")
    assert "private-api-inventory" in navigation_group("Internals")


def test_connector_navigation_uses_short_product_names() -> None:
    connector_paths = [
        "ray-data",
        "collections",
        "kafka",
        "rocketmq",
        "canal",
        "filesystem",
        "iceberg",
        "redis",
        "ray-serve",
        "console",
        "custom",
    ]
    root_index = (DOCS_ROOT / "index.md").read_text(encoding="utf-8")
    connector_tree = root_index.split(":caption: Connectors", maxsplit=1)[1].split("```", maxsplit=1)[0]
    positions = [connector_tree.index(f"\nconnectors/{path}\n") for path in connector_paths]
    assert positions == sorted(positions)

    overview = (DOCS_ROOT / "connectors" / "index.md").read_text(encoding="utf-8")
    assert "# Overview" in overview
    assert "```{toctree}" not in overview

    for connector_path in connector_paths:
        page = (DOCS_ROOT / "connectors" / f"{connector_path}.md").read_text(encoding="utf-8")
        title = next(line[2:] for line in page.splitlines() if line.startswith("# "))
        assert len(title.split()) <= 2, f"Connector navigation title is too long: {title}"


def test_configuration_reference_is_grouped_and_scannable() -> None:
    reference = (DOCS_ROOT / "configuration-reference.md").read_text(encoding="utf-8")
    assert reference.count("| Key | Default | Type | Description |") >= 10
    for heading in (
        "## Execution mode and task deployment",
        "## Checkpointing",
        "## Buffers and backpressure",
        "## Scheduling and placement",
        "## Managed state",
        "## Ray Serve integration",
    ):
        assert heading in reference


def test_documentation_uses_a_desktop_section_sidebar() -> None:
    config_path = DOCS_ROOT / "conf.py"
    config = config_path.read_text(encoding="utf-8")
    tree = ast.parse(config, filename=str(config_path))
    sidebar_assignment = next(
        node
        for node in tree.body
        if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == "html_sidebars" for target in node.targets)
    )
    assert ast.literal_eval(sidebar_assignment.value) == {"**": ["global-sidebar.html"]}

    sidebar_template = (DOCS_ROOT / "_templates" / "global-sidebar.html").read_text(encoding="utf-8")
    assert "generate_toctree_html(" in sidebar_template
    assert "startdepth=0" in sidebar_template

    assert '"navbar_center": []' in config
    assert '"image_light": "_static/klein-logo.svg"' in config
    assert '"image_dark": "_static/klein-logo-dark.svg"' in config
    assert (DOCS_ROOT / "_static" / "klein-logo.svg").is_file()
    assert (DOCS_ROOT / "_static" / "klein-logo-dark.svg").is_file()

    sidebar_css = (DOCS_ROOT / "_static" / "sidebar.css").read_text(encoding="utf-8")
    assert ".navbar-header-items__end" in sidebar_css
    assert ".toctree-l0 > .label-parts" in sidebar_css

    config_css = (DOCS_ROOT / "_static" / "configuration.css").read_text(encoding="utf-8")
    assert "#configuration-reference table" in config_css


def test_branding_stays_compact_across_surfaces() -> None:
    readme = (PROJECT_ROOT / "README.md").read_text(encoding="utf-8")
    assert '<img alt="Klein" src="docs/_static/klein-logo.svg" width="440">' in readme

    sidebar_css = (DOCS_ROOT / "_static" / "sidebar.css").read_text(encoding="utf-8")
    assert "max-width: min(9rem, 38vw);" in sidebar_css
    assert "max-height: 2.25rem;" in sidebar_css

    dashboard = (PROJECT_ROOT / "frontend" / "src" / "App.tsx").read_text(encoding="utf-8")
    assert "src={KleinMark} sx={{ height: 24, width: 28 }}" in dashboard


def test_documentation_uses_klein_as_the_product_name() -> None:
    legacy_brand = "Klein for " + "Ray"
    text_suffixes = {".css", ".html", ".js", ".md", ".po", ".py", ".rst", ".svg"}
    paths = [
        path
        for path in DOCS_ROOT.rglob("*")
        if path.is_file() and "_build" not in path.parts and path.suffix in text_suffixes
    ]
    paths.extend(PROJECT_ROOT.glob("*.md"))
    paths.extend(PACKAGE_ROOT.rglob("*.py"))
    paths.extend([PROJECT_ROOT / "CITATION.cff", PROJECT_ROOT / "NOTICE"])

    offenders = [
        str(path.relative_to(PROJECT_ROOT))
        for path in sorted(set(paths))
        if legacy_brand.casefold() in path.read_text(encoding="utf-8").casefold()
    ]
    assert not offenders, f"Legacy product name remains in documentation: {offenders}"


def test_restore_guide_uses_the_canonical_option() -> None:
    recovery = (DOCS_ROOT / "checkpoint-recovery.md").read_text(encoding="utf-8")
    driver_fault_tolerance = (DOCS_ROOT / "driver-fault-tolerance.md").read_text(encoding="utf-8")
    assert "execution.savepoint.path" in recovery
    assert "execution.savepoint.path" in driver_fault_tolerance
    assert "`execution.checkpointing.restore-path`" not in driver_fault_tolerance
