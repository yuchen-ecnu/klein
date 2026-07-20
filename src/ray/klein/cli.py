# SPDX-License-Identifier: Apache-2.0
"""Klein CLI — operator status, live attach, and job management."""

from __future__ import annotations

import json
import os
import signal
import sys
import threading
import time
from operator import itemgetter
from typing import Any, TypedDict

import click

import ray
import ray.klein as klein
from ray.klein._internal.constants import ComponentName
from ray.klein._internal.logging import get_logger
from ray.klein.api.job_status import JobStatus
from ray.klein.runtime.actor import KleinActorHandle

logger = get_logger(__name__)

_NAMESPACE_PREFIX = "klein-"
_NON_TERMINAL = frozenset(status.name for status in JobStatus if not status.is_terminal)
_STATE_QUERY_TIMEOUT_SECONDS = 10
_RPC_TIMEOUT_SECONDS = 5.0
_PROGRESS_FAILURE_LIMIT = 3


class _JobInfo(TypedDict):
    namespace: str
    job_name: str
    job_state: str
    actor_state: str
    dashboard_stale: bool


def _ensure_ray_init() -> None:
    if ray.is_initialized():
        return
    try:
        ray.init(address="auto", ignore_reinit_error=True)
    except Exception as error:
        raise click.ClickException("Cannot connect to a Ray cluster. Start Ray locally or set RAY_ADDRESS.") from error


def _extract_job_name(namespace: str) -> str:
    """Best-effort job name for actor-discovery fallback records."""
    if not namespace.startswith(_NAMESPACE_PREFIX):
        return namespace
    rest = namespace[len(_NAMESPACE_PREFIX) :]
    if len(rest) > 9 and rest[-9] == "-" and all(character in "0123456789abcdef" for character in rest[-8:]):
        return rest[:-9]
    return rest


def _published_job_info(snapshot: dict[str, Any]) -> _JobInfo | None:
    namespace = str(snapshot.get("namespace") or snapshot.get("job_id") or "")
    if not namespace:
        return None
    stale = bool(snapshot.get("dashboard_stale"))
    return {
        "namespace": namespace,
        "job_name": str(snapshot.get("job_name") or _extract_job_name(namespace)),
        "job_state": str(snapshot.get("status") or "UNKNOWN"),
        "actor_state": "STALE" if stale else "PUBLISHED",
        "dashboard_stale": stale,
    }


def _discover_published_jobs() -> list[_JobInfo]:
    try:
        from ray.klein.observability.state_api import list_job_snapshots

        snapshots = list_job_snapshots()
    except Exception as error:
        logger.debug("Published Klein job discovery failed: %s", error)
        return []

    jobs = []
    for snapshot in snapshots:
        job = _published_job_info(snapshot)
        if job is not None:
            jobs.append(job)
    return jobs


def _discover_actor_jobs() -> list[_JobInfo]:
    """Discover JobManagers when state publication is disabled or unavailable."""
    try:
        from ray.util import list_named_actors

        actors = list_named_actors(all_namespaces=True)
    except Exception as error:
        raise click.ClickException(
            "Cannot enumerate named actors in the Ray cluster. Ensure the cluster is reachable."
        ) from error

    jobs: list[_JobInfo] = []
    pending_status: dict[Any, _JobInfo] = {}
    for actor in actors:
        if actor.get("name") != ComponentName.KLEIN_JOB_MANAGER:
            continue
        job, reference = _named_actor_job_info(str(actor.get("namespace") or ""))
        if job is None:
            continue
        jobs.append(job)
        if reference is not None:
            pending_status[reference] = job
    _resolve_actor_statuses(pending_status)
    return jobs


def _named_actor_job_info(namespace: str) -> tuple[_JobInfo | None, Any | None]:
    if not namespace:
        return None, None
    job: _JobInfo = {
        "namespace": namespace,
        "job_name": _extract_job_name(namespace),
        "job_state": "ALIVE",
        "actor_state": "ALIVE",
        "dashboard_stale": False,
    }
    manager = klein.get_actor_by_name(ComponentName.KLEIN_JOB_MANAGER, namespace=namespace)
    if manager is None:
        job["job_state"] = "UNREACHABLE"
        return job, None
    try:
        return job, manager.job_status()
    except Exception as error:
        logger.debug("Cannot request status for Klein job %s: %s", namespace, error)
        job["job_state"] = "UNREACHABLE"
        return job, None


def _resolve_actor_statuses(pending_status: dict[Any, _JobInfo]) -> None:
    if not pending_status:
        return
    try:
        ready, _ = ray.wait(
            list(pending_status),
            num_returns=len(pending_status),
            timeout=_RPC_TIMEOUT_SECONDS,
        )
    except Exception as error:
        logger.debug("Klein job status wait failed: %s", error)
        ready = []

    ready_set = set(ready)
    for reference, job in pending_status.items():
        if reference not in ready_set:
            job["job_state"] = "UNREACHABLE"
            continue
        try:
            job["job_state"] = klein.get(reference).name
        except Exception as error:
            logger.debug("Cannot resolve status for Klein job %s: %s", job["namespace"], error)
            job["job_state"] = "UNREACHABLE"


def _discover_jobs() -> list[_JobInfo]:
    """Return published jobs merged with direct actor-discovery fallback."""
    _ensure_ray_init()
    published = _discover_published_jobs()
    try:
        actor_jobs = _discover_actor_jobs()
    except click.ClickException:
        if not published:
            raise
        actor_jobs = []

    jobs_by_namespace = {job["namespace"]: job for job in actor_jobs}
    jobs_by_namespace.update({job["namespace"]: job for job in published})
    jobs = list(jobs_by_namespace.values())
    jobs.sort(key=itemgetter("namespace"))
    return jobs


def _stdin_is_tty() -> bool:
    try:
        return bool(sys.stdin.isatty())
    except (AttributeError, ValueError):
        return False


def _stdout_is_tty() -> bool:
    try:
        return bool(sys.stdout.isatty())
    except (AttributeError, ValueError):
        return False


def _pick_job(jobs: list[_JobInfo]) -> str:
    """Prompt for one job and return its namespace."""
    if not _stdin_is_tty():
        raise click.UsageError("NAMESPACE is required when input is not an interactive terminal.")

    click.echo("")
    for index, job in enumerate(jobs, 1):
        foreground = _job_status_style(job["job_state"])
        click.echo(
            f"  {click.style(str(index), bold=True, fg='yellow')}) "
            f"{click.style(job['job_name'], bold=True)}  "
            f"{click.style(job['job_state'], fg=foreground)}  "
            f"{click.style(job['namespace'], dim=True)}"
        )
    click.echo("")
    choice = click.prompt(
        "Choose a job (number, or Ctrl+C to cancel)",
        type=click.IntRange(1, len(jobs)),
        default=1,
        show_default=True,
    )
    return jobs[choice - 1]["namespace"]


def _job_status_style(state: str) -> str:
    return {
        "RUNNING": "cyan",
        "FINISHED": "green",
        "FAILED": "red",
        "CANCELLED": "yellow",
        "CREATED": "dim",
        "SUBMITTING": "dim",
        "DEPLOYING": "dim",
        "INITIALIZING": "dim",
        "UNREACHABLE": "red",
        "DEAD": "red",
    }.get(state, "white")


def _echo_json(value: Any) -> None:
    click.echo(json.dumps(value, indent=2, sort_keys=True))


@click.group(name="klein")
@click.version_option(package_name="ray-klein")
def klein_cli_group() -> None:
    """Klein streaming job management.

    Set RAY_ADDRESS to manage a remote Ray cluster.
    """


@klein_cli_group.command(name="list")
@click.option("--all", "include_all", is_flag=True, help="Include terminal and unreachable jobs.")
@click.option("--json", "as_json", is_flag=True, help="Emit machine-readable JSON.")
def klein_list(include_all: bool, as_json: bool) -> None:
    """List Klein streaming jobs."""
    jobs = _discover_jobs()
    if not include_all:
        jobs = [job for job in jobs if job["job_state"] in _NON_TERMINAL]
    if as_json:
        _echo_json(jobs)
        return
    if not jobs:
        click.echo("No Klein jobs found." if include_all else "No running Klein jobs found.")
        return

    click.echo("")
    for job in jobs:
        foreground = _job_status_style(job["job_state"])
        stale = click.style("  stale", fg="yellow") if job["dashboard_stale"] else ""
        click.echo(
            f"  {click.style('●', fg=foreground)} "
            f"{click.style(job['job_name'], bold=True)}  "
            f"{click.style(job['job_state'], fg=foreground)}  "
            f"{click.style(job['namespace'], dim=True)}{stale}"
        )
    click.echo("")


def _get_published_snapshot(namespace: str) -> dict[str, Any] | None:
    try:
        from ray.klein.observability.state_api import get_job_snapshot

        return get_job_snapshot(namespace)
    except Exception as error:
        logger.debug("Published snapshot lookup failed for %s: %s", namespace, error)
        return None


def _get_job_snapshot(namespace: str) -> dict[str, Any]:
    _ensure_ray_init()
    snapshot = _get_published_snapshot(namespace)
    if snapshot is not None:
        return snapshot

    manager = _connect(namespace)
    try:
        return klein.get(manager.dashboard_snapshot(), timeout=_STATE_QUERY_TIMEOUT_SECONDS)
    except Exception as error:
        raise click.ClickException(f"Failed to query job status for {namespace}: {error}") from error


def _render_job_status(snapshot: dict[str, Any], namespace: str) -> None:
    status = str(snapshot.get("status") or "UNKNOWN")
    job_name = str(snapshot.get("job_name") or _extract_job_name(namespace))
    foreground = _job_status_style(status)
    click.echo(
        f"\n  {click.style('●', fg=foreground)} "
        f"{click.style(job_name, bold=True)}  "
        f"{click.style(status, fg=foreground)}  "
        f"{click.style(namespace, dim=True)}"
    )

    if snapshot.get("dashboard_stale"):
        detail = snapshot.get("dashboard_error") or "the live JobManager snapshot is unavailable"
        click.secho(f"  Warning: showing the last known snapshot ({detail}).", fg="yellow", err=True)

    overview = snapshot.get("overview") or {}
    if overview:
        click.echo(
            "  "
            f"operators={overview.get('operators', 0)}  "
            f"tasks={overview.get('task_instances', 0)}  "
            f"rows_in={overview.get('rows_in', 0)}  "
            f"rows_out={overview.get('rows_out', 0)}  "
            f"restarts={overview.get('restarts', 0)}"
        )

    for operator in snapshot.get("operators") or []:
        operator_status = str(operator.get("status") or "unknown")
        operator_foreground = {
            "running": "cyan",
            "finished": "green",
            "recovering": "yellow",
            "failed": "red",
        }.get(operator_status, "white")
        backpressure = operator.get("backpressure_percent")
        backpressure_text = f"  bp={backpressure:.0f}%" if isinstance(backpressure, int | float) else ""
        click.echo(
            f"    {click.style('●', fg=operator_foreground)} {str(operator.get('name') or 'unknown'):<30s}  "
            f"par={operator.get('parallelism', 0)}  "
            f"in={operator.get('rows_in', 0)}  out={operator.get('rows_out', 0)}"
            f"{backpressure_text}  {click.style(operator_status, fg=operator_foreground)}"
        )

    checkpoints = snapshot.get("checkpoints") or {}
    checkpoint_summary = checkpoints.get("summary") or {}
    if checkpoint_summary:
        click.echo(
            "  checkpoints: "
            f"completed={checkpoint_summary.get('completed', 0)}  "
            f"failed={checkpoint_summary.get('failed', 0)}  "
            f"in_progress={checkpoint_summary.get('in_progress', 0)}"
        )
        if checkpoints.get("latest_path"):
            click.echo(f"  latest checkpoint: {checkpoints['latest_path']}")

    if snapshot.get("failure"):
        click.secho(f"  failure: {snapshot['failure']}", fg="red")
    click.echo("")


@klein_cli_group.command(name="status")
@click.argument("namespace", required=False)
@click.option("--json", "as_json", is_flag=True, help="Emit the complete JSON-safe snapshot.")
def klein_status(namespace: str | None, as_json: bool) -> None:
    """Show the current or retained status of a Klein job."""
    namespace = namespace or _resolve_namespace(require_running=False)
    snapshot = _get_job_snapshot(namespace)
    if as_json:
        _echo_json(snapshot)
        return
    _render_job_status(snapshot, namespace)


@klein_cli_group.command(name="attach")
@click.argument("namespace", required=False)
def klein_attach(namespace: str | None) -> None:
    """Attach to a running job and watch progress until it becomes terminal.

    If NAMESPACE is omitted, an interactive picker lists running jobs. Press
    Ctrl+C to detach without stopping the job.
    """
    if not _stdout_is_tty():
        raise click.UsageError("attach requires an interactive terminal on stdout.")
    namespace = namespace or _resolve_namespace(require_running=True)
    manager = _connect(namespace)
    status = _get_status(manager)
    if status.is_terminal:
        raise click.ClickException(f"Job is {status.name}; a terminal job cannot be attached.")

    published = _get_published_snapshot(namespace)
    job_name = str((published or {}).get("job_name") or _extract_job_name(namespace))
    click.echo(f"Attaching to {click.style(job_name, bold=True)} ({click.style(namespace, dim=True)}) …\n")
    _run_attached_progress(manager, job_name)


def _run_attached_progress(job_manager: KleinActorHandle, job_name: str) -> None:
    if not _stdout_is_tty():
        raise click.UsageError("attach requires an interactive terminal on stdout.")
    os.environ.pop("KLEIN_NO_RICH_UI", None)

    from ray.klein.observability.progress_view import ProgressView, print_summary

    stop_event = threading.Event()
    detached_event = threading.Event()
    original_sigint = signal.getsignal(signal.SIGINT)

    def _on_sigint(_signum, _frame) -> None:
        detached_event.set()
        stop_event.set()

    progress_result = {"rows": 0}
    started = time.monotonic()
    terminal_reference = job_manager.wait_until_terminal()
    signal.signal(signal.SIGINT, _on_sigint)
    try:
        with ProgressView(job_name=job_name, mode="ATTACHED") as view:
            final_status = _poll_attached_progress(
                job_manager,
                terminal_reference,
                view,
                stop_event,
                detached_event,
                progress_result,
            )

        if detached_event.is_set():
            click.echo(f"\n{click.style('Detached', fg='yellow')} — job is still running.")
            return
        if final_status is None:
            raise click.ClickException("Attachment ended before the job reached a terminal state.")
        print_summary(
            job_name,
            final_status.name,
            time.monotonic() - started,
            progress_result["rows"],
        )
        if final_status == JobStatus.FAILED:
            raise click.ClickException("The attached job failed. Run `ray-klein status` for details.")
    finally:
        signal.signal(signal.SIGINT, original_sigint)


def _poll_attached_progress(
    job_manager: KleinActorHandle,
    terminal_reference: Any,
    view: Any,
    stop_event: threading.Event,
    detached_event: threading.Event,
    progress_result: dict[str, int],
) -> JobStatus | None:
    consecutive_failures = 0
    while not stop_event.is_set():
        try:
            ready, _ = ray.wait([terminal_reference], timeout=0)
        except Exception as error:
            raise click.ClickException(f"Lost the terminal-state subscription: {error}") from error
        if ready:
            try:
                return klein.get(terminal_reference, timeout=_RPC_TIMEOUT_SECONDS)
            except Exception as error:
                raise click.ClickException(f"Failed to read the terminal job status: {error}") from error

        try:
            snapshot = klein.get(job_manager.progress_snapshot(), timeout=2.0)
            if snapshot and snapshot.operators:
                view.update(snapshot)
                progress_result["rows"] = view.total_rows
            consecutive_failures = 0
        except KeyboardInterrupt:
            detached_event.set()
            stop_event.set()
        except Exception as error:
            consecutive_failures += 1
            logger.debug("Progress snapshot polling failed: %s", error)
            if consecutive_failures >= _PROGRESS_FAILURE_LIMIT:
                raise click.ClickException(
                    f"Lost progress updates after {_PROGRESS_FAILURE_LIMIT} attempts: {error}"
                ) from error
        stop_event.wait(0.25)
    return None


@klein_cli_group.command(name="cancel")
@click.argument("namespace", required=False)
@click.option("--force", "-f", "--yes", is_flag=True, help="Skip the confirmation prompt.")
@click.option(
    "--timeout", type=click.IntRange(min=1), default=60, show_default=True, help="Cancellation timeout in seconds."
)
def klein_cancel(namespace: str | None, force: bool, timeout: int) -> None:
    """Cancel a running Klein job.

    If NAMESPACE is omitted, an interactive picker lists running jobs.
    """
    namespace = namespace or _resolve_namespace(require_running=True)
    published = _get_published_snapshot(namespace)
    published_status = str((published or {}).get("status") or "")
    if published_status and published_status not in _NON_TERMINAL:
        click.echo(f"Job is already {published_status}.")
        return

    manager = _connect(namespace)
    status = _get_status(manager)
    if status.is_terminal:
        click.echo(f"Job is already {status.name}.")
        return

    job_name = str((published or {}).get("job_name") or _extract_job_name(namespace))
    if not force:
        click.echo(f"About to cancel {click.style(job_name, bold=True)} ({click.style(namespace, dim=True)})")
        click.confirm("Continue?", abort=True)

    try:
        cancelled = bool(klein.get(manager.cancel(timeout=timeout), timeout=timeout + _RPC_TIMEOUT_SECONDS))
    except Exception as error:
        raise click.ClickException(f"Failed to cancel job {namespace}: {error}") from error
    if cancelled:
        click.echo(f"  {click.style('✖', fg='yellow')} Job cancelled.")
        return

    latest_status = _get_status(manager)
    if latest_status.is_terminal:
        click.echo(f"Job reached {latest_status.name} before cancellation completed.")
        return
    raise click.ClickException("The JobManager did not acknowledge cancellation.")


# ``cancel`` matches the Python API; ``stop`` remains a visible compatibility
# alias for users of the original CLI.
klein_cli_group.add_command(klein_cancel, name="stop")


def _resolve_namespace(*, require_running: bool) -> str:
    jobs = _discover_jobs()
    if require_running:
        jobs = [job for job in jobs if job["job_state"] in _NON_TERMINAL]
    if not jobs:
        message = "No running Klein jobs found." if require_running else "No Klein jobs found."
        raise click.ClickException(message)
    if len(jobs) == 1:
        return jobs[0]["namespace"]
    return _pick_job(jobs)


def _connect(namespace: str) -> KleinActorHandle:
    _ensure_ray_init()
    manager = klein.get_actor_by_name(ComponentName.KLEIN_JOB_MANAGER, namespace=namespace)
    if manager is None:
        raise click.ClickException(f"No JobManager found in namespace {namespace}.")
    return manager


def _get_status(job_manager: KleinActorHandle) -> JobStatus:
    try:
        return klein.get(job_manager.job_status(), timeout=_RPC_TIMEOUT_SECONDS)
    except Exception as error:
        raise click.ClickException(f"Failed to query job status: {error}") from error
