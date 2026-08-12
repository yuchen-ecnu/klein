# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import runpy
from pathlib import Path

PROJECT_ROOT = Path(__file__).parents[2]
POLICY_MODULE = runpy.run_path(str(PROJECT_ROOT / "scripts" / "check_doc_translations.py"))
check_catalogs = POLICY_MODULE["check_catalogs"]


def _write_catalog(path: Path, messages: dict[str, str]) -> None:
    body = ['msgid ""', 'msgstr ""', '"Content-Type: text/plain; charset=UTF-8\\n"', ""]
    for message_id, translation in messages.items():
        body.extend((f'msgid "{message_id}"', f'msgstr "{translation}"', ""))
    path.write_text("\n".join(body), encoding="utf-8")


def test_translation_check_rejects_stale_catalog_entries(tmp_path: Path) -> None:
    templates = tmp_path / "templates"
    translations = tmp_path / "translations"
    templates.mkdir()
    translations.mkdir()
    _write_catalog(templates / "guide.pot", {"current": ""})
    _write_catalog(translations / "guide.po", {"current": "当前", "old": "旧文案"})

    errors = check_catalogs(templates, translations)

    assert len(errors) == 1
    assert "stale: 'old'" in errors[0]


def test_translation_check_accepts_a_complete_current_catalog(tmp_path: Path) -> None:
    templates = tmp_path / "templates"
    translations = tmp_path / "translations"
    templates.mkdir()
    translations.mkdir()
    _write_catalog(templates / "guide.pot", {"current": ""})
    _write_catalog(translations / "guide.po", {"current": "当前"})

    assert check_catalogs(templates, translations) == []
