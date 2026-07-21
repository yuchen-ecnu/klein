# SPDX-License-Identifier: Apache-2.0
"""Stable user-facing API.

Exports are loaded lazily so importing a small contract such as
``ray.klein.api.RuntimeContext`` does not initialize Ray Data or integrations.
"""

from typing import Any

from ray.klein._internal.lazy_exports import resolve_lazy_export

_EXPORTS = {
    "CatalogTable": ("ray.klein.api.catalog_table", "CatalogTable"),
    "ChangelogRow": ("ray.klein.api.changelog_row", "ChangelogRow"),
    "DataStream": ("ray.klein.api.data_stream", "DataStream"),
    "KleinContext": ("ray.klein.api.klein_context", "KleinContext"),
    "JobHandle": ("ray.klein.api.job_handle", "JobHandle"),
    "JobStatus": ("ray.klein.api.job_status", "JobStatus"),
    "KeyedProcessFunction": (
        "ray.klein.api.keyed_process_function",
        "KeyedProcessFunction",
    ),
    "KeyedStream": ("ray.klein.api.keyed_stream", "KeyedStream"),
    "MissingDataStrategy": ("ray.klein.api.missing_data_strategy", "MissingDataStrategy"),
    "RuntimeContext": ("ray.klein.api.runtime_context", "RuntimeContext"),
    "RuntimeInfo": ("ray.klein.api.runtime_info", "RuntimeInfo"),
    "RowKind": ("ray.klein.api.row_kind", "RowKind"),
    "SinkFunction": ("ray.klein.api.sink_function", "SinkFunction"),
    "SinkCommittable": ("ray.klein.api.sink_committable", "SinkCommittable"),
    "SQLQueryError": ("ray.klein.api.sql_query_error", "SQLQueryError"),
    "SQLSession": ("ray.klein.api.sql_session", "SQLSession"),
    "SourceFunction": ("ray.klein.api.source_function", "SourceFunction"),
    "SessionWindow": ("ray.klein.api.session_window", "SessionWindow"),
    "SlidingWindow": ("ray.klein.api.sliding_window", "SlidingWindow"),
    "StreamRuntimeContext": ("ray.klein.api.stream_runtime_context", "StreamRuntimeContext"),
    "StreamSink": ("ray.klein.api.stream_sink", "StreamSink"),
    "TableColumn": ("ray.klein.api.table_column", "TableColumn"),
    "TableFactory": ("ray.klein.api.table_factory", "TableFactory"),
    "TwoPhaseCommitSinkFunction": (
        "ray.klein.api.two_phase_commit_sink_function",
        "TwoPhaseCommitSinkFunction",
    ),
    "TimeWindow": ("ray.klein.api.time_window", "TimeWindow"),
    "TumblingWindow": ("ray.klein.api.tumbling_window", "TumblingWindow"),
    "WindowAssigner": ("ray.klein.api.window_assigner", "WindowAssigner"),
    "WindowedStream": ("ray.klein.api.windowed_stream", "WindowedStream"),
    "get_config": ("ray.klein.api.klein_context", "get_config"),
    "register_scalar_function": ("ray.klein.api.klein_context", "register_scalar_function"),
    "register_table_factory": ("ray.klein.api.klein_context", "register_table_factory"),
    "sql": ("ray.klein.api.sql", "sql"),
}

__all__ = sorted(_EXPORTS)


def __getattr__(name: str) -> Any:
    return resolve_lazy_export(name, _EXPORTS, globals(), __name__)
