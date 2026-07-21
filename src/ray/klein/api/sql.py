# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import inspect
from collections.abc import Mapping
from typing import TYPE_CHECKING

from ray.klein._internal.sql.scope import discover_streams
from ray.klein._internal.sql.validation import referenced_table_names
from ray.klein.api.sql_query_error import SQLQueryError
from ray.klein.api.sql_session import SQLSession

if TYPE_CHECKING:
    from ray.klein._internal.sql.scalar_function_registry import ScalarFunction
    from ray.klein.api.data_stream import DataStream
    from ray.klein.api.klein_context import KleinContext


def sql(
    query: str,
    /,
    *,
    tables: Mapping[str, DataStream] | None = None,
    functions: Mapping[str, ScalarFunction] | None = None,
    context: KleinContext | None = None,
    num_cpus: float = 1.0,
) -> DataStream:
    """Build SQL over named Klein DataStreams, discovering caller variables by default."""

    tables = _resolve_tables(tables, context)

    referenced = referenced_table_names(query)
    if referenced is not None:
        tables = {name: dataframe for name, dataframe in tables.items() if name in referenced}

    context = _resolve_context(context, tables)

    session = SQLSession(context)
    for name, function in context.sql_session._scalar_function_bindings().items():
        session.register_scalar_function(name, function)
    return session.sql(
        query,
        tables=tables,
        functions=functions,
        num_cpus=num_cpus,
    )


def _resolve_tables(
    tables: Mapping[str, DataStream] | None,
    context: KleinContext | None,
) -> dict[str, DataStream]:
    if tables is not None:
        return dict(tables)
    frame = inspect.currentframe()
    try:
        caller = frame.f_back.f_back if frame is not None and frame.f_back is not None else None
        return discover_streams(caller, context=context) if caller is not None else {}
    finally:
        del frame


def _resolve_context(
    context: KleinContext | None,
    tables: Mapping[str, DataStream],
) -> KleinContext:
    if context is not None:
        return context
    contexts = {id(stream.context): stream.context for stream in tables.values()}
    if len(contexts) > 1:
        raise SQLQueryError("Discovered DataStreams from multiple contexts; pass tables=... explicitly")
    if contexts:
        return next(iter(contexts.values()))
    from ray.klein.api.klein_context import KleinContext

    return KleinContext.current()
