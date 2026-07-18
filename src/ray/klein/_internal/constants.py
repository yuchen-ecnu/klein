# SPDX-License-Identifier: Apache-2.0
import re
import uuid


class ComponentName:
    """
    ComponentName.

    Note: these are the *short* named-actor names used at registration time.
    Multiple Klein jobs in the same Ray cluster share these short names — the
    cross-job uniqueness comes from each job using a different Ray namespace
    (see :func:`build_job_namespace`), not from mangling the names themselves.
    Keeping the names short and stable also keeps logs / debug output readable.
    """

    KLEIN_CHECKPOINT_COORDINATOR = "CheckpointCoordinator"
    KLEIN_JOB_MANAGER = "JobManager"


# ``klein-{sanitized_job_name}-{uuid8}`` — sanitized so a job_name with
# whitespace / unusual chars (the API doesn't constrain it) still produces a
# Ray-namespace-legal string. Ray accepts any non-empty UTF-8 namespace, but
# keeping it lowercase / alnum / dashes makes the namespace greppable in
# dashboards and rules out collisions with the literal characters we use as
# separators below.
_NAMESPACE_PREFIX = "klein-"
_JOB_NAME_SANITIZE_RE = re.compile(r"[^a-z0-9-]+")
_JOB_NAME_MAX_LEN = 40


def _sanitize_job_name_for_namespace(job_name: str) -> str:
    """Make ``job_name`` safe to embed in a Ray namespace.

    Lowercase, collapse runs of non [a-z0-9-] to a single ``-``, trim leading/
    trailing dashes, and cap length so the final namespace stays readable.
    Empty input collapses to ``"job"`` so callers always get a non-empty
    component.
    """
    if not job_name:
        return "job"
    normalized = _JOB_NAME_SANITIZE_RE.sub("-", job_name.lower()).strip("-")
    if not normalized:
        return "job"
    return normalized[:_JOB_NAME_MAX_LEN]


def build_job_namespace(
    job_name: str | None = None,
    explicit_namespace: str | None = None,
) -> str:
    """Decide which Ray namespace this JobClient should use.

    Resolution order:

    * If the caller passed a non-empty ``explicit_namespace`` (set
      via ``JobManagerOptions.NAMESPACE`` for jobs that want a stable /
      shared namespace, e.g. for ops tooling that attaches by name), use that
      verbatim.
    * Otherwise auto-generate ``klein-{sanitized_job_name}-{uuid8}`` so
      two JobClients in the same cluster never collide, even when the user
      submits two jobs with the same ``job_name``.

    Every call returns a ready-to-use Ray namespace string.
    """
    if explicit_namespace:
        return explicit_namespace
    sanitized = _sanitize_job_name_for_namespace(job_name or "")
    return f"{_NAMESPACE_PREFIX}{sanitized}-{uuid.uuid4().hex[:8]}"
