# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import Mock

import pytest

import ray
import ray.klein as klein
from ray.klein.api.completed_job_handle import CompletedJobHandle
from ray.klein.api.job_client import JobClient
from ray.klein.api.klein_context import KleinContext
from ray.klein.api.live_job_handle import LiveJobHandle
from ray.klein.api.resource_plan import ResourcePlan
from ray.klein.config.configuration import Configuration
from ray.klein.config.environment_variables import EnvironmentVariables
from ray.klein.config.execution_options import ExecutionOptions
from ray.klein.config.runtime_execution_mode import RuntimeExecutionMode
from ray.klein.config.udf_options import UDFOptions


def test_execute_compile_only_returns_the_compiled_graph(monkeypatch) -> None:
    client = JobClient(Configuration(include_environment=False))
    graph = object()
    monkeypatch.setenv(EnvironmentVariables.COMPILE_ONLY, "1")
    monkeypatch.setattr(client, "_get_logical_graph", lambda *_args: graph)

    handle = client.execute("compile-only", [object()])

    assert isinstance(handle, CompletedJobHandle)
    assert handle.get() is graph


@pytest.mark.parametrize(
    ("configured_mode", "resolved_mode"),
    [
        (RuntimeExecutionMode.BATCH, RuntimeExecutionMode.BATCH),
        (RuntimeExecutionMode.STREAMING, RuntimeExecutionMode.STREAMING),
        (RuntimeExecutionMode.AUTO, RuntimeExecutionMode.BATCH),
    ],
)
def test_execute_routes_to_resolved_runtime(monkeypatch, configured_mode, resolved_mode) -> None:
    config = Configuration(include_environment=False)
    config.set(ExecutionOptions.MODE, configured_mode)
    client = JobClient(config)
    graph = object()
    batch_result = object()
    streaming_result = object()
    monkeypatch.setattr(client, "_get_logical_graph", lambda *_args: graph)
    monkeypatch.setattr(client, "_determine_runtime_mode", lambda _graph: resolved_mode)
    monkeypatch.setattr(client, "_execute_batch", lambda *_args: batch_result)
    monkeypatch.setattr(client, "_execute_streaming", lambda *_args: streaming_result)

    result = client.execute("routing", [object()])

    assert result is (batch_result if resolved_mode is RuntimeExecutionMode.BATCH else streaming_result)


def test_auto_mode_uses_streaming_for_ignore_exceptions_on_a_bounded_graph() -> None:
    config = Configuration(include_environment=False)
    config.set(UDFOptions.IGNORE_EXCEPTIONS, True)
    context = KleinContext(config)
    sink = context.from_values({"value": 1}).map(lambda row: row).take_all()
    graph = JobClient._get_logical_graph((sink,), "ignore-errors", config)

    assert JobClient._determine_runtime_mode(graph) is RuntimeExecutionMode.STREAMING


@pytest.mark.parametrize("method", ["map", "map_batches"])
def test_auto_mode_uses_streaming_for_async_transform_on_a_bounded_graph(method: str) -> None:
    config = Configuration(include_environment=False)
    context = KleinContext(config)

    async def identity(value):
        return value

    source = context.from_values({"value": 1})
    options = {"async_buffer_size": 4}
    if method == "map_batches":
        options["batch_size"] = 1
    sink = getattr(source, method)(identity, **options).take_all()
    graph = JobClient._get_logical_graph((sink,), f"async-{method}", config)

    assert JobClient._determine_runtime_mode(graph) is RuntimeExecutionMode.STREAMING


def test_auto_mode_keeps_batch_for_fully_supported_bounded_graph() -> None:
    config = Configuration(include_environment=False)
    context = KleinContext(config)
    sink = context.from_values({"value": 1}).map(lambda row: row).take_all()
    graph = JobClient._get_logical_graph((sink,), "sync-batch", config)

    assert JobClient._determine_runtime_mode(graph) is RuntimeExecutionMode.BATCH


def test_batch_execution_reports_success(monkeypatch) -> None:
    from ray.klein.runtime.graph import batch_compiler as compiler_module

    compiled = object()
    compiler = Mock()
    compiler.execute.return_value = compiled
    monkeypatch.setattr(compiler_module, "BatchCompiler", lambda _graph: compiler)
    lineage = Mock()

    handle = JobClient(Configuration())._execute_batch(object(), lineage)

    assert isinstance(handle, CompletedJobHandle)
    assert handle.get() is compiled
    lineage.report_start.assert_called_once_with()
    lineage.report_complete.assert_called_once_with()


@pytest.mark.parametrize("error", [ValueError("bad batch"), KeyboardInterrupt("stop")])
def test_batch_execution_reports_failure_and_preserves_exception(monkeypatch, error: BaseException) -> None:
    from ray.klein.runtime.graph import batch_compiler as compiler_module

    compiler = Mock()
    compiler.execute.side_effect = error
    monkeypatch.setattr(compiler_module, "BatchCompiler", lambda _graph: compiler)
    lineage = Mock()

    with pytest.raises(type(error), match=str(error)):
        JobClient(Configuration())._execute_batch(object(), lineage)

    lineage.report_start.assert_called_once_with()
    lineage.report_fail.assert_called_once()


class _RemoteJobManager:
    def __init__(self, submit_result: bool = True) -> None:
        self.submit_result = submit_result
        self.submit_calls = []
        self.inner_actor = object()

    def submit(self, *args, **kwargs) -> bool:
        self.submit_calls.append((args, kwargs))
        return self.submit_result

    def failure_detail(self) -> str:
        return "placement failed"


def test_streaming_execution_initializes_ray_and_registers_dashboard(monkeypatch) -> None:
    from ray.klein.observability import dashboard

    config = Configuration(include_environment=False)
    client = JobClient(config)
    manager = _RemoteJobManager()
    lineage = Mock()
    graph = object()
    ray_init = Mock()
    register = Mock()
    monkeypatch.setattr(ray, "is_initialized", lambda: False)
    monkeypatch.setattr(ray, "init", ray_init)
    monkeypatch.setattr(klein, "is_debug_mode", lambda: False)
    monkeypatch.setattr(klein, "get", lambda value, **_kwargs: value)
    monkeypatch.setattr("ray.klein.api.job_client.build_job_namespace", lambda **_kwargs: "job-namespace")
    monkeypatch.setattr("ray.klein.api.job_client.JobManager.create", lambda *_args, **_kwargs: manager)
    monkeypatch.setattr(dashboard, "register_job", register)

    handle = client._execute_streaming(
        "orders",
        graph,
        RuntimeExecutionMode.STREAMING,
        lineage,
    )

    assert isinstance(handle, LiveJobHandle)
    assert handle.namespace == "job-namespace"
    ray_init.assert_called_once_with()
    lineage.report_start.assert_called_once_with()
    assert manager.submit_calls[0][0] == ("orders", graph)
    assert manager.submit_calls[0][1] == {"config": config}
    register.assert_called_once_with(
        job_id="job-namespace",
        job_name="orders",
        runtime_mode="STREAMING",
        namespace="job-namespace",
        manager=manager.inner_actor,
        config=config,
    )


def test_streaming_submission_failure_is_reported(monkeypatch) -> None:
    manager = _RemoteJobManager(submit_result=False)
    lineage = Mock()
    monkeypatch.setattr(ray, "is_initialized", lambda: True)
    monkeypatch.setattr(klein, "is_debug_mode", lambda: True)
    monkeypatch.setattr(klein, "get", lambda value, **_kwargs: value)
    monkeypatch.setattr("ray.klein.api.job_client.build_job_namespace", lambda **_kwargs: "job-namespace")
    monkeypatch.setattr("ray.klein.api.job_client.JobManager.create", lambda *_args, **_kwargs: manager)

    with pytest.raises(ValueError, match="placement failed"):
        JobClient(Configuration())._execute_streaming(
            "orders",
            object(),
            RuntimeExecutionMode.STREAMING,
            lineage,
        )

    lineage.report_fail.assert_called_once()


def test_logical_graph_loads_and_writes_resource_plan(monkeypatch, tmp_path) -> None:
    from ray.klein.runtime.graph import serve_rewriter
    from ray.klein.runtime.graph.logical_graph import LogicalGraph

    initial = object()
    loaded_plan = object()
    persisted_plan = Mock()
    planned = Mock()
    planned.build_resource_plan.return_value = persisted_plan
    rewritten = Mock()
    rewritten.with_resource_plan.return_value = planned
    monkeypatch.setenv(EnvironmentVariables.RESOURCE_PLAN_INPUT, str(tmp_path / "input.json"))
    monkeypatch.setenv(EnvironmentVariables.RESOURCE_PLAN_OUTPUT, str(tmp_path / "output.json"))
    monkeypatch.setattr(LogicalGraph, "from_sinks", lambda *_args: initial)
    monkeypatch.setattr(serve_rewriter, "ServeRewriter", lambda graph: SimpleNamespace(rewrite=lambda: rewritten))
    monkeypatch.setattr(ResourcePlan, "read", lambda _path: loaded_plan)

    result = JobClient._get_logical_graph([object()], "orders", Configuration())

    assert result is planned
    rewritten.with_resource_plan.assert_called_once_with(loaded_plan)
    persisted_plan.write.assert_called_once_with(str(tmp_path / "output.json"))


def test_explain_returns_rendered_resource_plan(monkeypatch) -> None:
    client = JobClient(Configuration())
    graph = Mock()
    graph.build_resource_plan.return_value = "resource-plan"
    monkeypatch.setattr(client, "_get_logical_graph", lambda *_args: graph)

    assert client.explain("orders", [object()]) == "resource-plan"
