# SPDX-License-Identifier: Apache-2.0
from enum import Enum


class _TerminalStatusEnum(str, Enum):
    """Internal enum base whose members declare lifecycle terminality."""

    is_terminal: bool

    def __new__(cls, value: str, is_terminal: bool) -> "_TerminalStatusEnum":
        member = str.__new__(cls, value)
        member._value_ = value
        member.is_terminal = is_terminal
        return member
