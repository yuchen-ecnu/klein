# SPDX-License-Identifier: Apache-2.0
"""Single source of truth for assigning tests to CI components."""

from __future__ import annotations

from pathlib import Path

CI_COMPONENTS = ("core", "runtime", "state", "sql", "connectors")

_CORE_UNIT_MODULES = {
    "test_check_dco.py",
    "test_cli.py",
    "test_compatibility_policy.py",
    "test_configuration.py",
    "test_configuration_properties.py",
    "test_dashboard_server.py",
    "test_dashboard_state.py",
    "test_datastream_api_contracts.py",
    "test_datastream_without_sink.py",
    "test_dependency_license_policy.py",
    "test_duration.py",
    "test_enum_types.py",
    "test_klein_context.py",
    "test_lineage.py",
    "test_logging.py",
    "test_logical_function_lowering.py",
    "test_metrics.py",
    "test_observability_state_api.py",
    "test_resource_plan.py",
    "test_resource_plan_generator.py",
    "test_resources.py",
    "test_runtime_info.py",
    "test_transform.py",
}

_SQL_UNIT_MODULES = {
    "test_sql.py",
    "test_sql_execution.py",
    "test_sql_expression.py",
    "test_streaming_sql.py",
}

_STATE_UNIT_MODULES = {
    "test_barrier_aligner.py",
    "test_barrier_id_generator.py",
    "test_checkpoint_barrier_transport.py",
    "test_checkpoint_coordinator_storage.py",
    "test_checkpoint_domains.py",
    "test_checkpoint_io.py",
    "test_checkpoint_model.py",
    "test_checkpoint_trigger.py",
    "test_coordinated_input_gate.py",
    "test_event_time.py",
    "test_managed_state_snapshot.py",
    "test_replay_watermark.py",
    "test_shared_epoch_sink_prepare.py",
    "test_stateful_operators.py",
    "test_state_codec_properties.py",
    "test_window_assigners.py",
}

_CONNECTOR_UNIT_MODULES = {
    "test_filesystem_sink_options.py",
    "test_ray_data_adapter.py",
    "test_redis_lookup.py",
    "test_redis_sink.py",
    "test_redis_writer.py",
}

_RUNTIME_UNIT_MODULES = {
    "test_actor.py",
    "test_actor_status.py",
    "test_async_base_worker.py",
    "test_async_notify.py",
    "test_async_ordered_runner.py",
    "test_columnar_passthrough.py",
    "test_coverage_policy.py",
    "test_data_plane_contracts.py",
    "test_delivery_journal.py",
    "test_emit_pipeline.py",
    "test_execution_graph.py",
    "test_execution_vertex.py",
    "test_failover_supervisor.py",
    "test_failover_scheduler.py",
    "test_input_batch_accumulator.py",
    "test_job_manager_failure.py",
    "test_job_handles.py",
    "test_job_client.py",
    "test_liveness_report.py",
    "test_logical_graph.py",
    "test_message.py",
    "test_namespace_isolation.py",
    "test_operator_lifecycle.py",
    "test_operator_spec.py",
    "test_partitioner.py",
    "test_placement.py",
    "test_progress_view.py",
    "test_progress_reporter_rescale.py",
    "test_pump.py",
    "test_restart_strategy.py",
    "test_runtime_rescale.py",
    "test_runtime_rescale_delta.py",
    "test_serve_client.py",
    "test_serve_deployment_lifecycle.py",
    "test_serve_error.py",
    "test_serve_rewriter.py",
    "test_source_operator.py",
    "test_source_stream_task.py",
    "test_streaming_expression.py",
    "test_stream_task_failure_lifecycle.py",
    "test_stream_task_rescale_validation.py",
    "test_stream_task_runtime_rescale.py",
    "test_task_deployer.py",
    "test_task_output.py",
    "test_task_terminator.py",
    "test_worker_controller.py",
    "test_worker_pool_dispatch.py",
}

_UNIT_MODULE_GROUPS = (
    ("core", _CORE_UNIT_MODULES),
    ("sql", _SQL_UNIT_MODULES),
    ("state", _STATE_UNIT_MODULES),
    ("connectors", _CONNECTOR_UNIT_MODULES),
    ("runtime", _RUNTIME_UNIT_MODULES),
)

UNIT_COMPONENT_BY_MODULE = {module: component for component, modules in _UNIT_MODULE_GROUPS for module in modules}
if len(UNIT_COMPONENT_BY_MODULE) != sum(len(modules) for _component, modules in _UNIT_MODULE_GROUPS):
    raise RuntimeError("a unit test module is assigned to more than one CI component")


def _unit_component(parts: tuple[str, ...], name: str) -> str:
    if len(parts) > 1 and parts[1] == "integrations":
        return "connectors"
    try:
        return UNIT_COMPONENT_BY_MODULE[name]
    except KeyError as error:
        raise ValueError(f"unit test module {name!r} has no explicit CI component owner") from error


def _integration_component(parts: tuple[str, ...], name: str) -> str:
    if len(parts) > 1 and parts[1] in {"connectors", "external"}:
        return "connectors"
    if name.startswith("test_sql"):
        return "sql"
    if name.startswith("test_stateful"):
        return "state"
    return "runtime"


def component_for_test_path(path: Path, test_root: Path) -> str:
    """Return the one CI component responsible for ``path``.

    The manifest is intentionally centralized here. Pytest, architecture tests,
    Make targets, and GitHub Actions all consume the markers derived from this
    function, so a test cannot silently disappear from one CI shard.
    """

    relative = path.resolve().relative_to(test_root.resolve())
    parts = relative.parts
    if parts[0] == "state":
        return "state"
    if parts[0] == "unit":
        return _unit_component(parts, relative.name)
    if parts[0] == "integration":
        return _integration_component(parts, relative.name)
    # Architecture tests protect repository-wide contracts and are a core gate.
    return "core"
