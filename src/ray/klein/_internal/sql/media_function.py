# SPDX-License-Identifier: Apache-2.0
"""Validation and projection planning for built-in SQL media functions."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from sqlglot import exp

from ray.klein._internal.sql.ray_data_expression import is_ray_data_only_node
from ray.klein.api.sql_query_error import SQLQueryError

MEDIA_FUNCTION_ARITIES: dict[str, frozenset[int]] = {
    "image_width": frozenset({1}),
    "image_height": frozenset({1}),
    "image_format": frozenset({1}),
    "image_resize": frozenset({3, 4, 5, 6}),
    "pdf_page_count": frozenset({1}),
    "pdf_split": frozenset({1, 2, 3}),
    "pdf_render_page": frozenset({2, 3}),
    "pdf_to_images": frozenset({1, 2, 3, 4}),
}
MEDIA_FUNCTION_NAMES = frozenset(MEDIA_FUNCTION_ARITIES)


@dataclass(frozen=True, slots=True)
class MediaComputation:
    field_name: str
    function_name: str
    arguments: tuple[exp.Expression, ...]


@dataclass(frozen=True, slots=True)
class MediaProjectionPlan:
    downloads: tuple[tuple[str, exp.Expression], ...]
    computations: tuple[MediaComputation, ...]
    projections: tuple[exp.Expression, ...]


def is_media_function_call(expression: exp.Expression) -> bool:
    return isinstance(expression, exp.Anonymous) and expression.name.casefold() in MEDIA_FUNCTION_NAMES


def validate_media_function_calls(query: exp.Expression) -> None:
    """Validate built-in media and DOWNLOAD calls before graph construction."""

    for call in _media_calls(query):
        name = call.name.casefold()
        arity = len(call.expressions)
        allowed = MEDIA_FUNCTION_ARITIES[name]
        if arity not in allowed:
            expected = _format_arity(allowed)
            raise SQLQueryError(f"{name.upper()} requires {expected} argument(s); received {arity}")

        select = call.find_ancestor(exp.Select)
        if select is None or not _belongs_to_projection(call, select):
            raise SQLQueryError(f"{name.upper()} can only be used in a SELECT projection")
        if select.args.get("group") is not None or any(
            projection.find(exp.AggFunc) is not None for projection in select.expressions
        ):
            raise SQLQueryError(f"{name.upper()} cannot be used in an aggregate query")

        for download in call.find_all(exp.Anonymous):
            if download is call or download.name.casefold() != "download":
                continue
            owner = _nearest_media_ancestor(download)
            if owner is None or not owner.expressions or owner.expressions[0] is not download:
                raise SQLQueryError("DOWNLOAD must be the direct data argument of a SQL media function")
        if any(is_ray_data_only_node(node) and not _is_download_call(node) for node in call.walk() if node is not call):
            raise SQLQueryError(f"{call.name.upper()} arguments cannot contain Ray-native-only SQL expressions")
    validate_download_calls(query)


def validate_download_calls(query: exp.Expression) -> None:
    """Reject DOWNLOAD placements that no execution backend can lower safely."""

    for call in (node for node in query.walk() if _is_download_call(node)):
        if len(call.expressions) != 1 or not isinstance(call.expressions[0], exp.Column):
            raise SQLQueryError("DOWNLOAD requires exactly one URI column argument")
        parent = call.parent
        select = call.find_ancestor(exp.Select)
        if select is None:
            raise SQLQueryError(_DOWNLOAD_PLACEMENT_ERROR)
        if _is_direct_projection(call, select):
            continue
        if isinstance(parent, exp.AggFunc) and _belongs_to_projection(parent, select):
            continue
        if (
            isinstance(parent, exp.Anonymous)
            and is_media_function_call(parent)
            and parent.expressions
            and parent.expressions[0] is call
        ):
            continue
        raise SQLQueryError(_DOWNLOAD_PLACEMENT_ERROR)


def plan_media_projections(projections: Sequence[exp.Expression]) -> MediaProjectionPlan:
    """Hoist downloads and replace media calls with fused worker output fields."""

    downloads: list[tuple[str, exp.Expression]] = []
    computations: list[MediaComputation] = []
    download_fields: dict[str, str] = {}
    media_fields: dict[tuple[str, str], str] = {}
    rewritten: list[exp.Expression] = []
    has_media_download = any(
        _is_download_call(node) and _nearest_media_ancestor(node) is not None
        for projection in projections
        for node in projection.walk()
    )

    for projection in projections:
        result = projection.copy()
        projection_value = result.this if isinstance(result, exp.Alias) else result
        # SQLGlot walks parents before children. Reverse that order so nested
        # media expressions become ordinary hidden columns for their parent.
        for node in reversed(list(result.walk())):
            if _is_download_call(node) and (
                _nearest_media_ancestor(node) is not None or (has_media_download and node is projection_value)
            ):
                key = node.sql()
                field_name = download_fields.get(key)
                if field_name is None:
                    field_name = f"_klein_media_download_{len(downloads)}"
                    download_fields[key] = field_name
                    downloads.append((field_name, node.copy()))
                node.replace(exp.column(field_name))
                continue
            if not is_media_function_call(node):
                continue
            name = node.name.casefold()
            key = name, node.sql()
            field_name = media_fields.get(key)
            if field_name is None:
                field_name = f"_klein_media_result_{len(computations)}"
                media_fields[key] = field_name
                computations.append(
                    MediaComputation(
                        field_name,
                        name,
                        tuple(argument.copy() for argument in node.expressions),
                    )
                )
            node.replace(exp.column(field_name))
        rewritten.append(result)
    return MediaProjectionPlan(tuple(downloads), tuple(computations), tuple(rewritten))


def _media_calls(query: exp.Expression) -> tuple[exp.Anonymous, ...]:
    return tuple(node for node in query.walk() if is_media_function_call(node))


def _is_download_call(expression: exp.Expression) -> bool:
    return isinstance(expression, exp.Anonymous) and expression.name.casefold() == "download"


def _nearest_media_ancestor(expression: exp.Expression) -> exp.Anonymous | None:
    parent = expression.parent
    while parent is not None:
        if is_media_function_call(parent):
            return parent
        parent = parent.parent
    return None


def _belongs_to_projection(call: exp.Expression, select: exp.Select) -> bool:
    projections = select.expressions
    child = call
    parent = child.parent
    while parent is not None and parent is not select:
        child = parent
        parent = child.parent
    return parent is select and any(child is projection for projection in projections)


def _is_direct_projection(call: exp.Expression, select: exp.Select) -> bool:
    parent = call.parent
    if parent is select:
        return any(call is projection for projection in select.expressions)
    return (
        isinstance(parent, exp.Alias)
        and parent.this is call
        and parent.parent is select
        and any(parent is projection for projection in select.expressions)
    )


def _format_arity(values: frozenset[int]) -> str:
    ordered = sorted(values)
    if len(ordered) == 1:
        return str(ordered[0])
    if ordered == list(range(ordered[0], ordered[-1] + 1)):
        return f"between {ordered[0]} and {ordered[-1]}"
    return " or ".join(str(value) for value in ordered)


_DOWNLOAD_PLACEMENT_ERROR = (
    "DOWNLOAD(column) must be a standalone SELECT value, a direct aggregate input, "
    "or the direct data argument of a SQL media function"
)
