# SPDX-License-Identifier: Apache-2.0
"""Public Klein API, loaded lazily to keep ``import ray.klein`` lightweight."""

from importlib.metadata import PackageNotFoundError, version
from typing import Any

from ray.klein._internal.lazy_exports import resolve_lazy_export

_EXPORTS = {
    "CatalogTable": ("ray.klein.api.catalog_table", "CatalogTable"),
    "ChangelogRow": ("ray.klein.api.changelog_row", "ChangelogRow"),
    "Configuration": ("ray.klein.config.configuration", "Configuration"),
    "DataStream": ("ray.klein.api.data_stream", "DataStream"),
    "JobHandle": ("ray.klein.api.job_handle", "JobHandle"),
    "JobStatus": ("ray.klein.api.job_status", "JobStatus"),
    "KeyedProcessFunction": ("ray.klein.api.keyed_process_function", "KeyedProcessFunction"),
    "KeyedStream": ("ray.klein.api.keyed_stream", "KeyedStream"),
    "KleinContext": ("ray.klein.api.klein_context", "KleinContext"),
    "RowKind": ("ray.klein.api.row_kind", "RowKind"),
    "RuntimeContext": ("ray.klein.api.runtime_context", "RuntimeContext"),
    "RuntimeInfo": ("ray.klein.api.runtime_info", "RuntimeInfo"),
    "SQLQueryError": ("ray.klein.api.sql_query_error", "SQLQueryError"),
    "SQLSession": ("ray.klein.api.sql_session", "SQLSession"),
    "SessionWindow": ("ray.klein.api.session_window", "SessionWindow"),
    "SinkCommittable": ("ray.klein.api.sink_committable", "SinkCommittable"),
    "SinkFunction": ("ray.klein.api.sink_function", "SinkFunction"),
    "SlidingWindow": ("ray.klein.api.sliding_window", "SlidingWindow"),
    "SourceFunction": ("ray.klein.api.source_function", "SourceFunction"),
    "StreamRuntimeContext": ("ray.klein.api.stream_runtime_context", "StreamRuntimeContext"),
    "TableColumn": ("ray.klein.api.table_column", "TableColumn"),
    "TableFactory": ("ray.klein.api.table_factory", "TableFactory"),
    "TimeWindow": ("ray.klein.api.time_window", "TimeWindow"),
    "TumblingWindow": ("ray.klein.api.tumbling_window", "TumblingWindow"),
    "TwoPhaseCommitSinkFunction": (
        "ray.klein.api.two_phase_commit_sink_function",
        "TwoPhaseCommitSinkFunction",
    ),
    "WatermarkStrategy": ("ray.klein.api.watermark_strategy", "WatermarkStrategy"),
    "WindowAssigner": ("ray.klein.api.window_assigner", "WindowAssigner"),
    "WindowedStream": ("ray.klein.api.windowed_stream", "WindowedStream"),
    "aget": ("ray.klein._internal.ray", "aget"),
    "cancel_job": ("ray.klein.observability.state_api", "cancel_job"),
    "configure": ("ray.klein.api.klein_context", "configure"),
    "configure_logging": ("ray.klein._internal.logging", "configure_logging"),
    "current_context": ("ray.klein.api.klein_context", "current_context"),
    "dataset_factory": ("ray.klein.api.read_api", "dataset_factory"),
    "execute": ("ray.klein.api.klein_context", "execute"),
    "execute_sql": ("ray.klein.api.klein_context", "execute_sql"),
    "exit_actor": ("ray.klein._internal.ray", "exit_actor"),
    "explain": ("ray.klein.api.klein_context", "explain"),
    "from_items": ("ray.klein.api.read_api", "from_items"),
    "from_ray_dataset": ("ray.klein.api.read_api", "from_ray_dataset"),
    "from_values": ("ray.klein.api.read_api", "from_values"),
    "get": ("ray.klein._internal.ray", "get"),
    "get_job_snapshot": ("ray.klein.observability.state_api", "get_job_snapshot"),
    "get_actor_by_name": ("ray.klein._internal.ray", "get_actor_by_name"),
    "get_actor_status": ("ray.klein._internal.ray", "get_actor_status"),
    "install_context": ("ray.klein.api.klein_context", "install_context"),
    "is_debug_mode": ("ray.klein._internal.ray", "is_debug_mode"),
    "kill": ("ray.klein._internal.ray", "kill"),
    "kill_actor_by_name": ("ray.klein._internal.ray", "kill_actor_by_name"),
    "list_job_snapshots": ("ray.klein.observability.state_api", "list_job_snapshots"),
    "read_kafka": ("ray.klein.api.read_api", "read_kafka"),
    "register_debug_actor": ("ray.klein._internal.ray", "register_debug_actor"),
    "reset_context": ("ray.klein.api.klein_context", "reset_context"),
    "source": ("ray.klein.api.read_api", "source"),
    "sql": ("ray.klein.api.sql", "sql"),
}

__all__ = sorted([*_EXPORTS, "__version__"])


def __getattr__(name: str) -> Any:
    if name in _EXPORTS:
        return resolve_lazy_export(name, _EXPORTS, globals(), __name__)

    from ray.klein.api.ray_data.discovery import has_public_dataset_factory

    if has_public_dataset_factory(name):
        from ray.klein.api.read_api import dataset_factory

        function = dataset_factory(name)
        globals()[name] = function
        return function
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    from ray.klein.api.ray_data.discovery import public_dataset_factories

    return sorted(set(globals()) | set(public_dataset_factories()))


try:
    __version__ = version("ray-klein")
except PackageNotFoundError:  # pragma: no cover - source tree without installation
    __version__ = "0+unknown"
