# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class TableColumn:
    """One physical column declared by a SQL ``CREATE TABLE`` statement."""

    name: str
    data_type: str
    nullable: bool = True
