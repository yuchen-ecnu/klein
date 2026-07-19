# SPDX-License-Identifier: Apache-2.0

import os
from collections.abc import Sequence

import ray
import ray.klein as klein
from ray.klein._internal.constants import build_job_namespace
from ray.klein._internal.logging import get_logger
from ray.klein.api.completed_job_handle import CompletedJobHandle
from ray.klein.api.job_handle import JobHandle
from ray.klein.api.live_job_handle import LiveJobHandle
from ray.klein.api.resource_plan import ResourcePlan
from ray.klein.api.stream_sink import StreamSink
from ray.klein.config.configuration import Configuration
from ray.klein.config.environment_variables import EnvironmentVariables
from ray.klein.config.execution_options import ExecutionOptions
from ray.klein.config.job_manager_options import JobManagerOptions
from ray.klein.config.runtime_execution_mode import RuntimeExecutionMode
from ray.klein.exceptions import KleinError
from ray.klein.observability.lineage.tracker import KleinLineageTracker
from ray.klein.runtime.graph.logical_graph import LogicalGraph
from ray.klein.runtime.job_manager.job_manager import JobManager

logger = get_logger(__name__)


class JobClient:
    """Builder that compiles a job and submits it.

    ``JobClient`` is intentionally thin and cheap to construct: it holds only
    the job ``Configuration``. The heavyweight side effects — ``ray.init`` and
    creating the remote ``JobManager`` actor — happen lazily inside
    :meth:`execute`, and *only* on the streaming branch that actually needs a
    cluster. ``explain``, compile-only and batch runs never pay for an actor
    they don't use.

    :meth:`execute` returns a :class:`JobHandle` — :class:`LiveJobHandle` for a
    submitted streaming job, or :class:`CompletedJobHandle` for an
    already-finished batch / compile-only run. The two are siblings, so neither
    has to inherit and then override away the other's behaviour.
    """

    _no_sink_error = "The job has no output sink; add a sink such as DataStream.show()."

    def __init__(self, config: Configuration) -> None:
        self._config = config

    def execute(
        self,
        job_name: str,
        sinks: Sequence[StreamSink],
    ) -> "JobHandle":
        if not sinks:
            raise ValueError(self._no_sink_error)

        # Serve-extraction mode: the serve deployment runs this very script to
        # harvest the ray_serve region's operators. The graph is fully built by
        # now, so hand the sinks off and unwind before submitting anything —
        # `capture_from_sinks` raises to abort the script (see serve_extract).
        from ray.klein.runtime import serve_extract

        if serve_extract.extracting():
            serve_extract.capture_from_sinks(sinks, self._config)

        logical_graph = self._get_logical_graph(sinks, job_name, self._config)

        if os.environ.get(EnvironmentVariables.COMPILE_ONLY) is not None:
            logger.warning(
                "You have set environment variable `%s`, the job enter finished status after compile directly. "
                "If you do not need that, please remove it",
                EnvironmentVariables.COMPILE_ONLY,
            )
            return CompletedJobHandle(logical_graph)

        mode = self._config.get(ExecutionOptions.MODE)
        if mode == RuntimeExecutionMode.AUTO:
            mode = self._determine_runtime_mode(logical_graph)
            logger.info("Job will run in %s by auto detection", mode)

        lineage_tracker = KleinLineageTracker(job_name)
        lineage_tracker.initialize(logical_graph)

        if mode == RuntimeExecutionMode.BATCH:
            return self._execute_batch(logical_graph, lineage_tracker)

        return self._execute_streaming(job_name, logical_graph, mode, lineage_tracker)

    def _execute_batch(
        self,
        logical_graph: LogicalGraph,
        lineage_tracker: KleinLineageTracker,
    ) -> "JobHandle":
        from ray.klein.runtime.graph.batch_compiler import BatchCompiler

        try:
            lineage_tracker.report_start()
            result = BatchCompiler(logical_graph).execute()
            lineage_tracker.report_complete()
            return CompletedJobHandle(result)
        except (SystemExit, KeyboardInterrupt) as error:
            lineage_tracker.report_fail(KleinError(f"Batch job was terminated by external signal: {error}"))
            raise
        except Exception as error:
            lineage_tracker.report_fail(error)
            raise

    def _execute_streaming(
        self,
        job_name: str,
        logical_graph: LogicalGraph,
        mode: RuntimeExecutionMode,
        lineage_tracker: KleinLineageTracker,
    ) -> "JobHandle":
        # The streaming path is the only one that needs a live cluster and a
        # remote JobManager actor, so the heavyweight setup lives here rather
        # than in __init__.
        if not ray.is_initialized() and not klein.is_debug_mode():
            # Let Ray choose collision-free local ports. Applications that need
            # custom dashboard or metrics settings should initialize Ray before
            # executing the graph; a library must not force Ray's private
            # ``_metrics_export_port`` option or claim a global fixed port.
            ray.init()

        # Per-job Ray namespace, computed BEFORE the JobManager is created so the
        # actor lands in the right namespace. Without this, every job in the same
        # cluster would reuse the one global "JobManager"/"CheckpointCoordinator"
        # named actor from whichever job started first, silently overwriting the
        # earlier job's state.
        namespace = build_job_namespace(
            job_name=job_name,
            explicit_namespace=self._config.get(JobManagerOptions.NAMESPACE) or None,
        )
        jobmanager = JobManager.create(self._config, namespace=namespace)

        lineage_tracker.report_start()
        submit_timeout = self._config.get(JobManagerOptions.SCHEDULER_START_TIMEOUT)
        submit_result: bool = klein.get(
            jobmanager.submit(job_name, logical_graph, config=self._config),
            timeout=submit_timeout,
        )
        if submit_result is False:
            detail = klein.get(jobmanager.failure_detail())
            error = ValueError(f"Job submit failed: {detail or 'unknown scheduling error'}")
            lineage_tracker.report_fail(error)
            raise error
        if not klein.is_debug_mode():
            from ray.klein.observability.dashboard import register_job

            register_job(
                job_id=namespace,
                job_name=job_name,
                runtime_mode=mode.value,
                namespace=namespace,
                manager=jobmanager.inner_actor,
                config=self._config,
            )
        return LiveJobHandle(
            jobmanager=jobmanager,
            job_name=job_name,
            runtime_mode=mode,
            namespace=namespace,
            lineage_tracker=lineage_tracker,
        )

    def explain(
        self,
        job_name: str,
        sinks: Sequence[StreamSink],
    ) -> str:
        graph = self._get_logical_graph(sinks, job_name, self._config)
        logger.debug("%s", graph)
        return str(graph.build_resource_plan())

    @staticmethod
    def _get_logical_graph(
        sinks: Sequence[StreamSink],
        job_name: str,
        config: Configuration,
    ) -> LogicalGraph:
        from ray.klein.runtime.graph.serve_rewriter import ServeRewriter

        graph = LogicalGraph.from_sinks(sinks, job_name, config)

        # Replace any ray_serve region with an embedded proxy node. The serve
        # deployment harvests its operators by re-running this same script and
        # intercepting execute() (see ``serve_extract``), so the client side
        # only ever needs the proxy rewrite.
        graph = ServeRewriter(graph).rewrite()

        resource_plan_path = os.environ.get(EnvironmentVariables.RESOURCE_PLAN_INPUT)
        if resource_plan_path is not None:
            rp = ResourcePlan.read(resource_plan_path)
            graph = graph.with_resource_plan(rp)
            logger.info(
                "Loaded ResourcePlan from '%s', since you have set environment variable `%s`. "
                "If you do not need that, please remove it",
                resource_plan_path,
                EnvironmentVariables.RESOURCE_PLAN_INPUT,
            )

        plan_output_path = os.environ.get(EnvironmentVariables.RESOURCE_PLAN_OUTPUT)
        if plan_output_path is not None:
            logger.info(
                "Original ResourcePlan has been outputted to '%s', since you have set environment variable `%s`. "
                "If you do not need that, please remove it",
                plan_output_path,
                EnvironmentVariables.RESOURCE_PLAN_OUTPUT,
            )
            graph.build_resource_plan().write(plan_output_path)

        return graph

    @staticmethod
    def _determine_runtime_mode(graph: LogicalGraph) -> RuntimeExecutionMode:
        return RuntimeExecutionMode.STREAMING if graph.runtime_mode_requires_streaming else RuntimeExecutionMode.BATCH
