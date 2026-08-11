# SPDX-License-Identifier: Apache-2.0
"""Restricted pickle decoding for framework-owned snapshot envelopes."""

from __future__ import annotations

import io
import pickle
import pickletools
from collections.abc import Mapping
from typing import Any

_GlobalName = tuple[str, str]
_EXTENSION_OPCODES = frozenset({"EXT1", "EXT2", "EXT4"})
_MEMO_INDEX_OPCODES = frozenset({"PUT", "GET", "BINPUT", "LONG_BINPUT", "BINGET", "LONG_BINGET"})
_FRAME_HEADER_SIZE = 9


class _RestrictedUnpickler(pickle.Unpickler):
    """Unpickler that resolves only explicitly allow-listed globals."""

    def __init__(
        self,
        stream: io.BytesIO,
        allowed_globals: Mapping[_GlobalName, Any],
    ) -> None:
        super().__init__(stream)
        self._allowed_globals = allowed_globals

    def find_class(self, module: str, name: str) -> Any:
        try:
            return self._allowed_globals[(module, name)]
        except KeyError as error:
            raise pickle.UnpicklingError(f"global {module}.{name} is not allowed in a framework snapshot") from error

    def persistent_load(self, pid: object) -> Any:
        raise pickle.UnpicklingError("persistent ids are not allowed in a framework snapshot")


def restricted_pickle_loads(
    payload: bytes,
    *,
    allowed_globals: Mapping[_GlobalName, Any] | None = None,
) -> Any:
    """Decode a framework envelope without importing arbitrary pickle globals."""

    if not isinstance(payload, bytes):
        raise TypeError("restricted pickle payloads must be bytes")
    stream = io.BytesIO(payload)
    try:
        for opcode, argument, position in pickletools.genops(payload):
            if opcode.name in _EXTENSION_OPCODES:
                raise pickle.UnpicklingError("extension globals are not allowed in a framework snapshot")
            if opcode.name in _MEMO_INDEX_OPCODES and (
                not isinstance(argument, int) or argument < 0 or argument > len(payload)
            ):
                raise pickle.UnpicklingError("pickle memo index exceeds the framework snapshot bound")
            if opcode.name == "FRAME" and (
                position is None
                or not isinstance(argument, int)
                or argument < 0
                or argument > len(payload) - position - _FRAME_HEADER_SIZE
            ):
                raise pickle.UnpicklingError("pickle frame exceeds the framework snapshot payload")
        value = _RestrictedUnpickler(stream, allowed_globals or {}).load()
    except (
        AttributeError,
        EOFError,
        ImportError,
        IndexError,
        OverflowError,
        RecursionError,
        TypeError,
        ValueError,
    ) as error:
        raise pickle.UnpicklingError("malformed framework snapshot pickle") from error
    if stream.read(1):
        raise pickle.UnpicklingError("trailing data is not allowed in a framework snapshot")
    return value
