# SPDX-License-Identifier: Apache-2.0
"""Klein CLI — operator status, live attach, and job management.

Register as ``ray klein`` sub-command group in ``ray.scripts.scripts``.
"""

from __future__ import annotations

import os
import signal
import sys
import threading
import time
from operator import itemgetter

import click

import ray
import ray.klein as klein
from ray.klein._internal.constants import ComponentName
from ray.klein._internal.logging import get_logger
from ray.klein.api.job_status import JobStatus
from ray.klein.runtime.actor import KleinActorHandle

logger = get_logger(__name__)

_NAMESPACE_PREFIX = "klein-"
_NON_TERMINAL = ("RUNNING", "CREATED", "SUBMITTING", "DEPLOYING", "INITIALIZING")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _ensure_ray_init() -> None:
    if not ray.is_initialized():
        ray.init(address="auto", ignore_reinit_error=True)


def _extract_job_name(namespace: str) -> str:
    """Extract the human-readable job name from a Klein namespace.

    ``klein-{sanitized_name}-{uuid8}`` → ``sanitized_name``.
    Falls back to the full namespace for explicit / non-standard namespaces.
    """
    if not namespace.startswith(_NAMESPACE_PREFIX):
        return namespace
    rest = namespace[len(_NAMESPACE_PREFIX) :]
    if len(rest) > 9 and rest[-9] == "-" and all(character in "0123456789abcdef" for character in rest[-8:]):
        return rest[:-9]
    return rest


def _discover_jobs() -> list[dict]:
    """Return a list of {namespace, job_name, job_state, actor_state} for each
    Klein JobManager actor found in the cluster."""
    _ensure_ray_init()

    try:
        from ray.util.state import list_actors

        actors = list_actors(filters=[("name", "=", ComponentName.KLEIN_JOB_MANAGER)])
    except Exception as exc:
        click.echo("Cannot query Ray state API. Ensure the dashboard is running or the cluster is reachable.")
        raise click.Abort from exc

    jobs = []
    for actor in actors:
        ns = actor.ray_namespace or ""
        if not ns.startswith(_NAMESPACE_PREFIX):
            continue
        job_name = _extract_job_name(ns)
        job_state = "?"
        if actor.state == "ALIVE":
            try:
                jm = klein.get_actor_by_name(ComponentName.KLEIN_JOB_MANAGER, namespace=ns)
                if jm is not None:
                    s = klein.get(jm.job_status())
                    job_state = s.name
            except Exception:
                job_state = actor.state
        else:
            job_state = actor.state
        jobs.append(
            {
                "namespace": ns,
                "job_name": job_name,
                "job_state": job_state,
                "actor_state": actor.state,
            }
        )

    jobs.sort(key=itemgetter("namespace"))
    return jobs


def _pick_job(jobs: list[dict]) -> str | None:
    """Interactive picker: returns the namespace of the chosen job or None."""
    if not jobs:
        click.echo("No running Klein jobs found.")
        return None

    click.echo("")
    state_colors = {
        "RUNNING": "cyan",
        "FINISHED": "green",
        "FAILED": "red",
        "CANCELLED": "yellow",
    }
    for i, j in enumerate(jobs, 1):
        fg = state_colors.get(j["job_state"], "white")
        click.echo(
            f"  {click.style(str(i), bold=True, fg='yellow')}) "
            f"{click.style(j['job_name'], bold=True)}  "
            f"{click.style(j['job_state'], fg=fg)}  "
            f"{click.style(j['namespace'], dim=True)}"
        )
    click.echo("")

    try:
        choice = click.prompt(
            "Choose a job (number, or Ctrl+C to cancel)",
            type=int,
            default=1,
            show_default=False,
        )
    except click.Abort:
        return None

    if 1 <= choice <= len(jobs):
        return jobs[choice - 1]["namespace"]
    click.echo(f"Invalid choice: {choice}")
    return None


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
    }.get(state, "white")


# ---------------------------------------------------------------------------
# CLI group
# ---------------------------------------------------------------------------


@click.group(name="klein")
@click.version_option(package_name="ray-klein")
def klein_cli_group() -> None:
    """Klein streaming job management."""


# ── list ────────────────────────────────────────────────────────────────────


@klein_cli_group.command(name="list")
def klein_list() -> None:
    """List running Klein streaming jobs."""
    jobs = _discover_jobs()
    if not jobs:
        click.echo("No running Klein jobs found.")
        return

    click.echo("")
    for job in jobs:
        fg = _job_status_style(job["job_state"])
        click.echo(
            f"  {click.style('●', fg=fg)} "
            f"{click.style(job['job_name'], bold=True)}  "
            f"{click.style(job['job_state'], fg=fg)}  "
            f"{click.style(job['namespace'], dim=True)}"
        )
    click.echo("")


# ── status ──────────────────────────────────────────────────────────────────


@klein_cli_group.command(name="status")
@click.argument("namespace", required=False)
def klein_status(namespace: str | None) -> None:
    """Show a quick status summary for a Klein job."""
    if namespace is None:
        namespace = _resolve_namespace(require_running=True)
        if namespace is None:
            return

    jm = _connect(namespace)
    status = _get_status(jm)
    snapshot = klein.get(jm.progress_snapshot())

    fg = _job_status_style(status.name)
    job_name = _extract_job_name(namespace)
    click.echo(
        f"\n  {click.style('●', fg=fg)} "
        f"{click.style(job_name, bold=True)}  "
        f"{click.style(status.name, fg=fg)}  "
        f"{click.style(namespace, dim=True)}\n"
    )
    if snapshot.operators:
        for op in snapshot.operators:
            op_fg = {
                "running": "cyan",
                "finished": "green",
                "recovering": "yellow",
            }.get(op.status, "white")
            click.echo(
                f"    {click.style('●', fg=op_fg)} {op.name:<30s}  "
                f"par={op.parallelism}  rows={op.rows_out}  "
                f"{click.style(op.status, fg=op_fg)}"
            )
    click.echo("")


# ── attach ──────────────────────────────────────────────────────────────────


@klein_cli_group.command(name="attach")
@click.argument("namespace", required=False)
def klein_attach(namespace: str | None) -> None:
    """Attach to a running Klein job and watch its live progress.

    If NAMESPACE is omitted, an interactive picker lists running jobs.

    Press Ctrl+C to detach without stopping the job.
    """
    if namespace is None:
        namespace = _resolve_namespace(require_running=True)
        if namespace is None:
            return

    jm = _connect(namespace)
    status = _get_status(jm)
    if status.name not in _NON_TERMINAL:
        click.echo(f"Job is {status.name} — cannot attach to a terminal job.")
        return

    job_name = _extract_job_name(namespace)
    click.echo(f"Attaching to {click.style(job_name, bold=True)} ({click.style(namespace, dim=True)}) …\n")
    _run_attached_progress(jm, job_name)


def _run_attached_progress(job_manager, job_name: str) -> None:
    os.environ.pop("KLEIN_NO_RICH_UI", None)
    if not sys.stdout.isatty():
        click.echo("stdout is not a TTY. Run from an interactive terminal.")
        return

    from ray.klein.observability.progress_view import ProgressView, print_summary

    stop_event = threading.Event()
    detached_event = threading.Event()
    original_sigint = signal.getsignal(signal.SIGINT)

    def _on_sigint(_signum, _frame) -> None:
        # Signal-safe: only set flags, no I/O.
        detached_event.set()
        stop_event.set()

    signal.signal(signal.SIGINT, _on_sigint)

    progress_result = {"rows": 0}
    started = time.monotonic()
    try:
        with ProgressView(job_name=job_name, mode="ATTACHED") as view:
            _poll_attached_progress(job_manager, view, stop_event, detached_event, progress_result)

        if detached_event.is_set():
            click.echo(f"\n{click.style('Detached', fg='yellow')} — job is still running.")
            return

        final_status = klein.get(job_manager.job_status(), timeout=5.0)
        print_summary(
            job_name,
            final_status.name,
            time.monotonic() - started,
            progress_result["rows"],
        )
    finally:
        signal.signal(signal.SIGINT, original_sigint)


def _poll_attached_progress(job_manager, view, stop_event, detached_event, progress_result) -> None:
    while not stop_event.is_set():
        try:
            snapshot = klein.get(job_manager.progress_snapshot(), timeout=2.0)
            if snapshot and snapshot.operators:
                view.update(snapshot)
                progress_result["rows"] = view.total_rows
        except KeyboardInterrupt:
            detached_event.set()
            break
        except Exception as error:
            logger.debug("Progress snapshot polling failed: %s", error)
        stop_event.wait(0.25)


# ── stop ────────────────────────────────────────────────────────────────────


@klein_cli_group.command(name="stop")
@click.argument("namespace", required=False)
@click.option("--force", "-f", is_flag=True, help="Skip confirmation prompt.")
def klein_stop(namespace: str | None, force: bool) -> None:
    """Cancel a running Klein job.

    If NAMESPACE is omitted, an interactive picker lists running jobs.
    """
    if namespace is None:
        namespace = _resolve_namespace(require_running=True)
        if namespace is None:
            return

    jm = _connect(namespace)
    status = _get_status(jm)
    if status.name not in _NON_TERMINAL:
        click.echo(f"Job is already {status.name}.")
        return

    job_name = _extract_job_name(namespace)
    if not force:
        click.echo(f"About to cancel {click.style(job_name, bold=True)} ({click.style(namespace, dim=True)})")
        click.confirm("Continue?", abort=True)

    klein.get(jm.cancel(timeout=60))
    click.echo(f"  {click.style('✖', fg='yellow')} Job cancelled.")


# ── shared helpers ──────────────────────────────────────────────────────────


def _resolve_namespace(require_running: bool = True) -> str | None:
    """Resolve a namespace: pick the only running job, or interactive picker."""
    _ensure_ray_init()
    jobs = _discover_jobs()
    if not jobs:
        click.echo("No Klein jobs found.")
        return None
    if require_running:
        jobs = [job for job in jobs if job["job_state"] in _NON_TERMINAL]
        if not jobs:
            click.echo("No running Klein jobs.")
            return None
    if len(jobs) == 1:
        return jobs[0]["namespace"]
    return _pick_job(jobs)


def _connect(namespace: str) -> KleinActorHandle:
    """Connect to the JobManager actor in the given namespace."""
    _ensure_ray_init()
    jm = klein.get_actor_by_name(ComponentName.KLEIN_JOB_MANAGER, namespace=namespace)
    if jm is None:
        click.echo(f"No JobManager found in namespace {namespace}.")
        raise click.Abort
    return jm


def _get_status(job_manager: KleinActorHandle) -> JobStatus:
    """Get the JobStatus of a connected JobManager."""
    try:
        return klein.get(job_manager.job_status())
    except Exception as error:
        click.echo(f"Failed to query job status: {error}")
        raise click.Abort from error
