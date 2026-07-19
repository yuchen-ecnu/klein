# SPDX-License-Identifier: Apache-2.0
"""Standalone Sphinx configuration for the Klein for Ray documentation."""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version

project = "Klein for Ray"
author = "Klein for Ray Authors"
copyright = "2024-2026, Klein for Ray Authors"

try:
    release = version("ray-klein")
except PackageNotFoundError:
    release = "0.1.0.dev0"
version = release

extensions = [
    "myst_parser",
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.intersphinx",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "sphinx_copybutton",
    "sphinx_design",
    "sphinxcontrib.mermaid",
]

autosummary_generate = True
autodoc_typehints = "description"
autodoc_preserve_defaults = True
nitpicky = False
nitpick_ignore = [
    ("py:class", "ObjectRef"),
    ("py:class", "ray.data.dataset.Dataset"),
]

myst_enable_extensions = ["colon_fence", "deflist", "fieldlist", "substitution"]
myst_fence_as_directive = ["mermaid"]
myst_heading_anchors = 3

templates_path: list[str] = []
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]
source_suffix = {".rst": "restructuredtext", ".md": "markdown"}
master_doc = "index"

html_theme = "pydata_sphinx_theme"
html_title = "Klein for Ray"
html_static_path: list[str] = []
html_theme_options = {
    "github_url": "https://github.com/yuchen-ecnu/klein",
    "show_nav_level": 2,
    "navigation_depth": 4,
    "secondary_sidebar_items": ["page-toc", "edit-this-page"],
    "use_edit_page_button": True,
}
html_context = {
    "github_user": "yuchen-ecnu",
    "github_repo": "klein",
    "github_version": "main",
    "doc_path": "docs",
}

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "ray": ("https://docs.ray.io/en/latest", None),
}

copybutton_prompt_text = r">>> |\.\.\. |\$ "
copybutton_prompt_is_regexp = True
