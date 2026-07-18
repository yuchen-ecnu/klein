# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field

from ray.klein._internal.frozen_mapping import FrozenMapping
from ray.klein.api.table_column import TableColumn


@dataclass(frozen=True, slots=True)
class CatalogTable:
    """Schema and connector options stored in a :class:`SQLSession` catalog."""

    name: str
    columns: tuple[TableColumn, ...] = ()
    options: Mapping[str, str] = field(default_factory=dict)
    temporary: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "columns", tuple(self.columns))
        object.__setattr__(self, "options", FrozenMapping(self.options))

    @property
    def connector(self) -> str:
        return self.options["connector"]
