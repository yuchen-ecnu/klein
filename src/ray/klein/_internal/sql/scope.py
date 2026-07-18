# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from types import FrameType
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ray.klein.api.data_stream import DataStream
    from ray.klein.api.klein_context import KleinContext


def discover_streams(
    frame: FrameType,
    *,
    context: KleinContext | None = None,
) -> dict[str, DataStream]:
    """Discover named Klein streams with local variables taking precedence."""

    from ray.klein.api.data_stream import DataStream

    namespace = dict(frame.f_globals)
    namespace.update(frame.f_locals)
    return {
        name: value
        for name, value in namespace.items()
        if isinstance(value, DataStream) and (context is None or value.context is context)
    }
