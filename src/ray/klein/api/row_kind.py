# SPDX-License-Identifier: Apache-2.0
from enum import Enum


class RowKind(str, Enum):
    """Flink-compatible change carried by one dynamic-table row."""

    INSERT = "+I"
    UPDATE_BEFORE = "-U"
    UPDATE_AFTER = "+U"
    DELETE = "-D"

    @property
    def is_addition(self) -> bool:
        return self in {RowKind.INSERT, RowKind.UPDATE_AFTER}

    @property
    def is_retraction(self) -> bool:
        return self in {RowKind.UPDATE_BEFORE, RowKind.DELETE}
