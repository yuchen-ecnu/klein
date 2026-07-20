---
myst:
  html_meta:
    description: "Complete ray-klein CLI reference for job discovery, status, attachment, cancellation, and the local operations Dashboard."
---
<!-- SPDX-License-Identifier: Apache-2.0 -->

(klein-cli-reference)=
# CLI reference

The `ray-klein` console command discovers and controls Klein streaming jobs in
an existing Ray cluster. It is installed with the base package.

```text
ray-klein [--help] [--version] COMMAND [ARGS]...
```

| Command | Purpose | Machine-readable output |
| --- | --- | --- |
| `dashboard` | Serve the bundled local operations Dashboard. | No |
| `list` | List current jobs, optionally including retained terminal jobs. | `--json` |
| `status` | Show one current or retained job snapshot. | `--json` |
| `attach` | Render live progress until the job terminates or the user detaches. | No |
| `cancel` | Request cooperative cancellation. | No |
| `stop` | Compatibility alias for `cancel`. | No |

Use `ray-klein COMMAND --help` to inspect the installed version. The command
names and defaults below describe the current `0.1.0.dev0` source tree.

(cli-ray-connection)=
## Connect to the Ray cluster

Every operational subcommand initializes Ray with `ray.init(address="auto")`;
the CLI does not start a new cluster when none is available. `--help` and
`--version` do not connect. Set `RAY_ADDRESS` before invoking an operational
command to select a remote cluster:

```bash
export RAY_ADDRESS="<address accepted by your Ray deployment>"
ray-klein list
```

Use the address scheme and authentication expected by that Ray deployment;
`ray://` addresses require Ray Client support in the environment. A connection
failure is reported without an application traceback and exits with status
`1`. Operational commands use the authority of the connected Ray client, so
verify the cluster before issuing a control command.

## Job names, namespaces, and job IDs

These identifiers serve different purposes:

- **Job name** is the human-readable name passed to `execute()`.
- **Namespace** is the per-job Ray namespace containing the named
  `JobManager`. It is returned by `JobHandle.namespace` and is the positional
  identifier accepted by every CLI job command.
- **Job ID** is the identifier used by the published Python state API and the
  Dashboard. In the current runtime it normally matches the namespace, but
  automation must not substitute one field for the other.

When `job.namespace` is empty, Klein generates a namespace of the form
`klein-<sanitized-job-name>-<eight-hex-characters>`. Copy the exact `namespace`
from `ray-klein list --json`; do not reconstruct it from the job name. See the
[configuration reference](configuration-reference.md#job-lifecycle) before
choosing a stable explicit namespace.

When a positional `NAMESPACE` is omitted:

1. no matching job produces an operational error;
2. exactly one matching job is selected automatically; and
3. multiple matching jobs open a numbered picker on interactive stdin.

Multiple jobs with non-interactive stdin produce usage exit code `2`. Pass the
namespace explicitly in scripts.

## `dashboard`

Serve the independent Klein web Dashboard and its operator-control endpoint:

```text
ray-klein dashboard [--host TEXT] [--port INTEGER] [--open]
                    [--allow-unauthenticated]
```

| Option | Default | Meaning |
| --- | --- | --- |
| `--host TEXT` | `127.0.0.1` | Listener address. `localhost`, IPv4 loopback, and IPv6 loopback are treated as local. |
| `--port INTEGER` | `8266` | Listener port in the inclusive range 1–65535. |
| `--open` | Off | Ask the default browser to open the displayed URL. |
| `--allow-unauthenticated` | Off | Permit a non-loopback listener even though the control endpoint has no authentication. |

The command runs in the foreground until Ctrl+C, then closes the server and
returns successfully. Binding a non-loopback host is refused with exit code `1`
unless `--allow-unauthenticated` is explicit. That flag only disables the
safety check; it does not add authentication or encryption.

Prefer the default loopback listener through an SSH tunnel. If a non-loopback
listener is unavoidable, place it behind an authenticated, encrypted reverse
proxy and restrict network access. See [Security](security.md#dashboard-and-control-apis)
and [Observability](observability.md#use-the-klein-dashboard).

## `list`

List jobs discovered through the cluster state actor and named-actor fallback:

```text
ray-klein list [--all] [--json]
```

| Option | Default | Meaning |
| --- | --- | --- |
| `--all` | Off | Include `FINISHED`, `FAILED`, `CANCELLED`, unreachable, and other non-running retained entries. |
| `--json` | Off | Emit the normalized JSON array described below. |

Without `--all`, only non-terminal lifecycle states are shown: `CREATED`,
`SUBMITTING`, `DEPLOYING`, `INITIALIZING`, and `RUNNING`. Results are sorted by
namespace. An empty result is successful: text mode prints `No running Klein
jobs found.` and JSON mode prints `[]`.

Examples:

```bash
ray-klein list
ray-klein list --all
ray-klein list --all --json
```

## `status`

Show the current or retained snapshot for one job:

```text
ray-klein status [NAMESPACE] [--json]
```

| Argument or option | Default | Meaning |
| --- | --- | --- |
| `NAMESPACE` | Automatic selection | Exact Ray namespace from `JobHandle.namespace` or `list --json`. Terminal retained jobs are eligible. |
| `--json` | Off | Emit the complete JSON-safe snapshot instead of the terminal summary. |

Text output includes job state, aggregate rows and restarts, operator
parallelism and backpressure, checkpoint counts and latest path, and a failure
summary when present. If the live `JobManager` is unavailable but a cached
snapshot exists, the command prints a stale-data warning.

`status` returning exit code `0` means the query succeeded; it does **not** mean
the job is healthy. A snapshot whose `status` is `FAILED` is still a successful
CLI query. Automation must inspect the JSON status field.

## `attach`

Attach a terminal progress view to a running job:

```text
ray-klein attach [NAMESPACE]
```

`attach` always requires interactive stdout. If the namespace is omitted and
more than one running job exists, it also requires interactive stdin for the
picker. Redirected output or a pipe is rejected with usage exit code `2`; use
`status --json` for non-interactive monitoring.

Ctrl+C detaches and leaves the job running. Reaching `FINISHED` or `CANCELLED`
prints a final summary and returns successfully. Reaching `FAILED`, attaching
to an already terminal job, or losing progress updates produces exit code `1`.
Use `cancel` or `stop` when the intent is to terminate the job.

## `cancel` and `stop`

`stop` is a visible compatibility alias for `cancel`; their arguments and
behavior are identical:

```text
ray-klein cancel [NAMESPACE] [--force] [--timeout INTEGER]
ray-klein stop   [NAMESPACE] [--force] [--timeout INTEGER]
```

| Argument or option | Default | Meaning |
| --- | --- | --- |
| `NAMESPACE` | Automatic running-job selection | Exact Ray namespace of the job to cancel. |
| `--force`, `-f`, `--yes` | Off | Skip the confirmation prompt. These are equivalent spellings. |
| `--timeout INTEGER` | `60` seconds | Positive cancellation time budget passed to the `JobManager`. |

Cancellation is cooperative. `--force` means “do not prompt”; it does not kill
actors more aggressively or bypass checkpoint and shutdown logic. The command
is idempotent for a retained terminal job and returns success after reporting
its existing state. It exits with `1` if the `JobManager` cannot be reached,
raises an error, times out, or does not acknowledge cancellation.

For unattended use, always provide all control inputs:

```bash
ray-klein cancel klein-orders-0123abcd --force --timeout 60
```

## Terminal requirements

| Invocation | Interactive stdin | Interactive stdout |
| --- | ---: | ---: |
| `list`, including `--json` | No | No |
| `status NAMESPACE --json` | No | No |
| `status` with multiple jobs | Yes, for the picker | No |
| `attach NAMESPACE` | No | Yes |
| `attach` with multiple jobs | Yes | Yes |
| `cancel NAMESPACE --force` or `stop NAMESPACE --force` | No | No |
| `cancel`/`stop` without `--force` | Yes, for confirmation and possibly selection | No |
| `dashboard` | No | No; it remains in the foreground until stopped |

(cli-json-contract)=
## JSON output contract

Both JSON modes write pretty-printed, JSON-safe values to stdout. Retain stderr
separately for connection diagnostics and stale-data or security warnings.
Consumers must accept additional object keys in future compatible versions and
must not depend on key ordering.

### `list --json`

The top-level value is an array sorted by `namespace`. Every current element
has these fields:

| Field | Type | Meaning |
| --- | --- | --- |
| `namespace` | string | Exact CLI identifier and Ray namespace. |
| `job_name` | string | Display name; actor fallback may derive it from the namespace. |
| `job_state` | string | Lifecycle state, `UNKNOWN`, or `UNREACHABLE`. Scripts should tolerate new states. |
| `actor_state` | string | Discovery source/health marker such as `PUBLISHED`, `STALE`, or `ALIVE`. |
| `dashboard_stale` | boolean | `true` when published metadata came from the last good cached snapshot. |

Example:

```json
[
  {
    "actor_state": "PUBLISHED",
    "dashboard_stale": false,
    "job_name": "orders",
    "job_state": "RUNNING",
    "namespace": "klein-orders-0123abcd"
  }
]
```

### `status --json`

The value is the complete state snapshot returned by the publication API, or a
direct `JobManager` snapshot when publication is unavailable. Current
top-level fields are:

| Field | Type | Meaning |
| --- | --- | --- |
| `job_id`, `job_name`, `namespace` | string or null where applicable | Published identity, display name, and CLI namespace. |
| `status` | string | `CREATED`, `SUBMITTING`, `DEPLOYING`, `INITIALIZING`, `RUNNING`, `FINISHED`, `CANCELLED`, or `FAILED`. |
| `created_at_ms`, `started_at_ms`, `ended_at_ms`, `updated_at_ms`, `duration_ms` | integer or null | Unix-epoch timestamps and elapsed duration in milliseconds. |
| `status_history` | array | Status transitions with status, previous status, and timestamp. |
| `operators` | array | Operator topology, subtask state, resources, counters, rates, backpressure, and rescale eligibility. |
| `edges` | array | Objects with source and target operator IDs. |
| `overview` | object | Aggregate operator/task counts, rows/bytes, and restart-window data. |
| `checkpoints` | object | Summary, history, latest durable path, and recovery/rescale state. |
| `rescale_operations` | array | Current and retained operator-rescale operation records. |
| `configuration` | object | Effective configuration with credential-like values redacted. |
| `failure` | string or null | Failure detail for a failed job. |
| `dashboard_stale` | boolean, when published | Whether the state actor returned its last good cached value. |
| `dashboard_error` | string, when stale | Reason the live snapshot could not be refreshed. |

Nested snapshot objects can gain fields. Treat absent optional fields and JSON
`null` as distinct from zero or an empty collection. Terminal history is kept
in state-actor memory and is not a durable audit log; checkpoint storage has a
separate lifecycle. The [observability API reference](api/observability.rst)
documents the equivalent Python interface.

## Exit statuses

| Exit status | Meaning |
| ---: | --- |
| `0` | The command completed its requested CLI action. This includes an empty `list`, a successful status query for a failed job, an already terminal cancellation target, Ctrl+C detachment from `attach`, and normal Dashboard shutdown. |
| `1` | Operational failure or refused action: cluster connection/query failure, unknown `JobManager`, unsafe Dashboard binding, failed attachment, or unacknowledged cancellation. |
| `2` | Command-line usage error: invalid/missing argument or option, a required non-interactive namespace, or `attach` without terminal stdout. |

Do not use the exit status of `list` or `status` as the job-health signal. Parse
the JSON `job_state` or `status` field instead.

## Automation examples

Select a running job by name and preserve its namespace:

```bash
namespace="$(ray-klein list --json | jq -er '
  [.[] | select(.job_name == "orders" and .job_state == "RUNNING")] |
  if length == 1 then .[0].namespace
  else error("expected exactly one running orders job") end
')"
ray-klein status "$namespace" --json | jq -e '.status == "RUNNING"'
```

Cancel it without a TTY:

```bash
ray-klein cancel "$namespace" --force --timeout 60
```

If a richer or in-process contract is preferable, initialize Ray explicitly
and use `ray.klein.list_job_snapshots()`, `get_job_snapshot()`, or
`cancel_job()`. Note that those functions address published job IDs, while the
CLI addresses namespaces.

## Security guidance

- Treat Ray cluster access as privileged: the CLI can inspect and cancel jobs
  reachable by that client.
- Never expose `dashboard --allow-unauthenticated` directly to an untrusted
  network. The endpoint can initiate operator rescaling.
- Protect Ray credentials, tokens, tunnels, and proxy configuration outside
  command history and source control.
- Snapshot configuration redacts credential-like field names, but failure
  details and application-provided names remain operationally sensitive.
- Prefer `list --json`, `status --json`, and explicit namespaces over terminal
  scraping or interactive selection in automation.

For symptoms and recovery steps, see [Troubleshooting](troubleshooting.md#jobs-and-cli).
