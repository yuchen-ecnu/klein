# SPDX-License-Identifier: Apache-2.0
"""Standalone Sphinx configuration for the Klein for Ray documentation."""

from __future__ import annotations

import gettext
import os
import re
from html import escape, unescape
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

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
myst_heading_anchors = 3

language = os.environ.get("KLEIN_DOCS_LANGUAGE", "en")
locale_dirs = ["locales/"]
gettext_compact = True
gettext_location = False

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]
source_suffix = {".rst": "restructuredtext", ".md": "markdown"}
master_doc = "index"

html_theme = "pydata_sphinx_theme"
html_title = "Klein for Ray 中文文档" if language == "zh_CN" else "Klein for Ray"
html_static_path: list[str] = []
html_theme_options = {
    "github_url": "https://github.com/yuchen-ecnu/klein",
    "show_nav_level": 2,
    "navigation_depth": 4,
    "navbar_end": ["language-switcher.html", "theme-switcher.html", "navbar-icon-links.html"],
    "secondary_sidebar_items": ["page-toc"] if language == "zh_CN" else ["page-toc", "edit-this-page"],
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
if os.environ.get("KLEIN_DOCS_OFFLINE") == "1":
    # Keep strict local and air-gapped builds reproducible when remote
    # inventories are unavailable. Normal builds retain external API links.
    intersphinx_mapping = {}

copybutton_prompt_text = r">>> |\.\.\. |\$ "
copybutton_prompt_is_regexp = True

_DESCRIPTION_META = re.compile(r'(<meta content=")(?P<description>.*?)(" name="description" />)')


def _translate_description_meta(pagename, context) -> None:
    """Translate MyST HTML metadata, which Sphinx leaves outside the doctree."""

    metatags = context.get("metatags")
    if not isinstance(metatags, str):
        return
    match = _DESCRIPTION_META.search(metatags)
    if match is None:
        return

    message = unescape(match.group("description"))
    domain = pagename.split("/", maxsplit=1)[0]
    localedir = Path(__file__).parent / "locales"
    translated = gettext.translation(domain, localedir=localedir, languages=["zh_CN"], fallback=True).gettext(message)
    if translated == message:
        return

    replacement = f"{match.group(1)}{escape(translated, quote=True)}{match.group(3)}"
    context["metatags"] = f"{metatags[: match.start()]}{replacement}{metatags[match.end() :]}"


def _add_language_switcher_context(app, pagename, templatename, context, doctree) -> None:
    """Link equivalent pages in the root English and ``zh_CN`` sites."""

    depth = pagename.count("/")
    output_path = f"{pagename}{app.builder.out_suffix}"
    if app.config.language == "zh_CN":
        context.update(
            language_switch_url=f"{'../' * (depth + 1)}{output_path}",
            language_switch_label="English",
            language_switch_code="en",
            language_switch_aria_label="切换到英文",
        )
        _translate_description_meta(pagename, context)
    else:
        context.update(
            language_switch_url=f"{'../' * depth}zh_CN/{output_path}",
            language_switch_label="简体中文",
            language_switch_code="zh-CN",
            language_switch_aria_label="Switch to Simplified Chinese",
        )


def setup(app) -> dict[str, bool]:
    app.connect("html-page-context", _add_language_switcher_context)
    return {"parallel_read_safe": True, "parallel_write_safe": True}
