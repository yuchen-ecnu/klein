# SPDX-License-Identifier: Apache-2.0
"""Fail when a generated documentation message lacks a Chinese translation."""

from __future__ import annotations

import argparse
from pathlib import Path

from babel.messages.catalog import Message
from babel.messages.pofile import read_po


def _messages(path: Path) -> dict[str | tuple[str, str], Message]:
    with path.open(encoding="utf-8") as stream:
        catalog = read_po(stream)
    return {message.id: message for message in catalog if message.id}


def _is_translated(message: Message) -> bool:
    strings = message.string if isinstance(message.string, tuple) else (message.string,)
    return bool(strings) and all(bool(value and value.strip()) for value in strings)


def check_catalogs(templates_dir: Path, translations_dir: Path) -> list[str]:
    errors: list[str] = []
    templates = sorted(templates_dir.glob("*.pot"))
    if not templates:
        return [f"no gettext templates found in {templates_dir}"]

    translations: dict[str, dict[str | tuple[str, str], Message]] = {}
    for translation_path in sorted(translations_dir.glob("*.po")):
        messages = _messages(translation_path)
        translations[translation_path.stem] = messages
        for message_id, message in messages.items():
            label = message_id[0] if isinstance(message_id, tuple) else message_id
            if "fuzzy" in message.flags:
                errors.append(f"{translation_path}: fuzzy: {label!r}")
            elif not _is_translated(message):
                errors.append(f"{translation_path}: untranslated: {label!r}")

    for template_path in templates:
        translation_path = translations_dir / f"{template_path.stem}.po"
        translated = translations.get(template_path.stem)
        if translated is None:
            errors.append(f"missing catalog: {translation_path}")
            continue

        for message_id in _messages(template_path):
            if message_id not in translated:
                label = message_id[0] if isinstance(message_id, tuple) else message_id
                errors.append(f"{translation_path}: missing: {label!r}")
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("templates_dir", type=Path)
    parser.add_argument("translations_dir", type=Path)
    arguments = parser.parse_args()

    errors = check_catalogs(arguments.templates_dir, arguments.translations_dir)
    if errors:
        print("Chinese documentation translation check failed:")
        for error in errors:
            print(f"  - {error}")
        return 1

    print("Chinese documentation catalogs are complete and current.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
