# SPDX-License-Identifier: Apache-2.0
"""Shared support for lightweight package-level exports."""

from collections.abc import Mapping, MutableMapping
from importlib import import_module
from typing import Any


def resolve_lazy_export(
    name: str,
    exports: Mapping[str, tuple[str, str]],
    namespace: MutableMapping[str, Any],
    module_name: str,
) -> Any:
    """Import, cache, and return one declared package export."""

    target = exports.get(name)
    if target is None:
        raise AttributeError(f"module {module_name!r} has no attribute {name!r}")
    target_module, attribute_name = target
    value = getattr(import_module(target_module), attribute_name)
    namespace[name] = value
    return value
