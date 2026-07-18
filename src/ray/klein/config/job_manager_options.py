# SPDX-License-Identifier: Apache-2.0
from ray.klein.config.config_option import ConfigOption


class JobManagerOptions:
    SCHEDULER_START_TIMEOUT = ConfigOption(
        "job.scheduler.start.timeout",
        300,
        int,
        description="Specifies the maximum time (in seconds) allowed for starting workers after deployment.",
    )

    DEPLOY_TIMEOUT = ConfigOption(
        "job.deploy.timeout",
        600,
        int,
        description="Total time budget (in seconds) for the whole deploy/schedule "
        "operation (coordinator open + start workers + coordinator start). "
        "Each individual step is additionally capped by "
        "job.scheduler.start.timeout. Bounds the entire deploy so a stuck "
        "step can't let the operation run unbounded.",
    )

    STOP_TIMEOUT = ConfigOption(
        "job.stop.timeout",
        60,
        int,
        description="Total time budget (in seconds) for tearing a job down (stop "
        "supervisor + stop workers + stop coordinator). Bounds the whole "
        "stop so cancel() can't hang indefinitely; individual coordinator "
        "RPCs are additionally capped by job.coordinator.rpc.timeout.",
    )

    COORDINATOR_RPC_TIMEOUT = ConfigOption(
        "job.coordinator.rpc.timeout",
        30,
        int,
        description="Maximum time (seconds) for lightweight coordinator RPCs "
        "(health probes, snapshot flush, stop). Startup-heavy "
        "operations (open, start) use job.scheduler.start.timeout "
        "instead because they may legitimately take minutes (model "
        "loading, checkpoint restoration).",
    )

    HEALTH_CHECK_INTERVAL = ConfigOption(
        "job.healthcheck.interval", 15, int, description="Specifies the health check interval (in seconds)."
    )

    # Per-job Ray namespace used to isolate this job's named actors
    # (JobManager, CheckpointCoordinator, StreamTasks) from any other Klein
    # job running in the same Ray cluster. When left at the empty-string
    # default, JobClient auto-generates ``klein-{job_name}-{uuid8}`` for
    # each JobClient instance so two jobs (even with the same job name) get
    # different namespaces. Set this explicitly to share a namespace across
    # JobClients (e.g. to attach to an already-running job's actors) or to
    # use a stable namespace for ops tooling.
    NAMESPACE = ConfigOption(
        "job.namespace",
        "",
        str,
        description="Per-job Ray namespace used to isolate this job's named actors "
        "from other Klein jobs running in the same cluster. Empty means "
        "auto-generate a unique namespace per JobClient.",
    )
