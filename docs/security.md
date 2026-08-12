---
myst:
  html_meta:
    description: "Security model, trust boundaries, and production hardening guidance for Klein."
---
<!-- SPDX-License-Identifier: Apache-2.0 -->

# Security

Klein is a distributed execution library for trusted Python applications. It
is not a sandbox, an authorization layer, or a tenant-isolation system. A user
who can submit a Klein job can execute Python on Ray workers, open network
connections, access worker-visible files and environment variables, and use
the credentials available to that workload.

This page explains deployment security. To report a suspected vulnerability,
use the private process in the repository's
[security policy](https://github.com/yuchen-ecnu/klein/blob/main/SECURITY.md),
not a public issue.

## Trust model

Treat the following principals and artifacts as trusted:

- everyone allowed to submit code or connect a Python client to the Ray cluster;
- application code, UDFs, serializers, source and sink implementations, and
  their Python/native dependencies;
- every process that can write to a checkpoint prefix;
- the Ray cluster, its node images, runtime environments, and administrators;
- the platform identities that can read secrets or connector data.

Treat source records, connector services, users of an application built on
Klein, and network clients outside the protected cluster boundary as
untrusted unless the application establishes a narrower contract.

:::{danger}
Do not restore a checkpoint from an untrusted or shared-writable location.
Version 4 checkpoint metadata has a non-executable JSON envelope, and Klein
validates every embedded payload's declared size and SHA-256 before loading
application state. Source state, custom state keys and serializers, generic
checkpoint-store values, and prepared sink committables can still contain
Python pickle payloads. Deserializing a malicious, otherwise valid application
payload can execute code with the worker's identity. A stored checksum detects
accidental corruption relative to checkpoint metadata; it is not a signature
and does not make an attacker-controlled checkpoint authentic.
:::

## Responsibility boundaries

Security is the composition of Klein, Ray, the deployment platform, and the
application. No one layer compensates automatically for a missing control in
another.

| Owner | Responsible for | Not provided by that layer |
| --- | --- | --- |
| Klein | Default loopback Dashboard binding; refusal of a non-loopback binding unless explicitly overridden; Host-header and same-origin checks; bounded Dashboard request sizes, concurrency, and socket-read time; security headers; liveness/readiness probes; correlated sanitized access and control events; key-name-based redaction in published configuration and structured log fields; safe checkpoint-metadata envelopes; restricted decoding of framework-owned state envelopes; size/checksum validation before application payload use; documenting source/sink recovery boundaries. | Authentication, authorization, TLS, hostile-code sandboxing, secret storage, tenant isolation, checkpoint signatures, record-level encryption, proxy-grade rate limiting, or a durable audit log independent of Ray. |
| Ray | Running actors/tasks, namespaces, runtime environments, Object Store, worker/actor logs, metrics, cluster connectivity, and the authentication/TLS features configured by the Ray operator. | Klein-specific authorization or isolation between mutually untrusted Klein jobs. A Ray namespace avoids actor-name collisions; it is not an access-control boundary. |
| Platform operator | Network isolation, firewall and ingress policy, TLS termination, Ray authentication, Kubernetes/cloud IAM, node and container hardening, image provenance, secret delivery, object-store IAM/encryption/versioning, log/metric access, backups, and separation of security domains. | Correct UDF behavior, safe application logging, connector semantics, or sink idempotency. |
| Application owner | Trusted dependencies and code review; input validation; UDF/resource limits; connector credential use; state/schema choices; sensitive-data classification; safe job/operator names; output authorization, idempotency, and retention. | Cluster isolation or platform controls that were never configured. |

Ray's own security guidance requires a controlled network and trusted code and
recommends separate clusters for workloads that need isolation. Configure
current Ray controls according to the
[Ray security documentation](https://docs.ray.io/en/latest/ray-security/index.html).

## Python code, UDFs, and pickle

Klein sends application functions and operator objects to Ray workers and
uses Python serialization for application-defined state. This has several
consequences:

- UDFs, custom sources, custom sinks, serializers, callbacks, and dependencies
  have the same effective trust as the job submitter.
- Pickle and cloudpickle are data/code transport formats, not safe parsers for
  hostile bytes.
- Framework-owned managed-state envelopes use a restricted unpickler that
  resolves only the exact Klein classes required by that envelope. This does
  not make the application-owned bytes nested inside it safe.
- Prefix-less and version 3 pickle checkpoint metadata is rejected before any
  pickle global can be resolved. Version 4 first validates its JSON structure,
  payload sizes, and SHA-256 digests, then loads source state and sink
  committables as trusted application payloads.
- Moving or renaming Python classes and modules can make a trusted checkpoint
  unreadable even when no attacker is involved.
- Deserialization must happen only in an environment whose code and dependency
  versions are controlled by the operator.

Use signed or otherwise provenance-checked application artifacts, pin
dependencies, scan the final worker image, and review custom serialization
hooks. Parse untrusted external data with a constrained data format such as
JSON, Avro, or Parquet before it becomes application state. Do not call
`pickle.loads()` on a record supplied by an external user.

For checkpoint storage:

1. Give the job identity read/write access only to its dedicated prefix.
2. Give restore tooling read access only to explicitly approved prefixes.
3. Enable transport encryption, server-side encryption, versioning, retention,
   and access logging at the object store.
4. Separate prefixes and credentials across production, staging, and tenants.
5. Treat deletion, replacement of `_metadata`, or writes to `shared/` and
   `taskowned/` as security-sensitive actions.
6. Never copy only `chk-N/_metadata` and assume the checkpoint is self-contained;
   it can reference state objects elsewhere in the job prefix.

See [Checkpoint storage](checkpoint-storage.md) for the actual publication and
validation behavior.

## SQL DOWNLOAD and media URLs

Treat a URI column consumed by SQL `DOWNLOAD` as a network-capable input.
Klein permits local paths and selected object-store schemes for compatibility,
but HTTP(S) uses its own SSRF boundary. The default rejects URL credentials,
unknown schemes, non-global resolved addresses, HTTPS-to-HTTP redirects, more
than five redirects, and more than 64 MiB retained for one row. Every redirect
is validated again, and the connection is pinned to an address that passed the
IP policy.

Configure `sql.download.allowed-hosts` and `denied-hosts` for service names, and
`allowed-ip-ranges` and `denied-ip-ranges` for resolved addresses. These checks
are conjunctive: allowlisting `assets.example.com` does not let that name resolve
to loopback, link-local metadata, or a private address. Use a narrow explicit IP
range for an approved internal service; avoid the broader
`sql.download.allow-private-network=true`.

The byte bound is cumulative across downloads in one SQL projection or batch
row. A standalone streaming `stream.ray_data.with_column(..., download(...))`
operator has the same per-response bound; separate chained operators each have
their own budget. HTTP connection, redirect, and body reads share the configured
I/O budget. Python's synchronous system resolver cannot be interrupted by that
timer, and S3/GS timeout and retry behavior remains owned by the configured
filesystem/provider. Local and object-store schemes do not have HTTP host/IP
checks, so do not feed attacker-controlled paths to them. Media decoders apply
their own byte, pixel, page, output, and DPI limits after the download boundary.

## Dashboard and control APIs

`ray-klein dashboard` serves a separate Klein HTTP page. It is not part of the
Ray Dashboard and has no built-in user authentication or TLS.

The default listener is `127.0.0.1:8266`. Keep that default and use an SSH
tunnel or port-forward when possible:

```bash
ray-klein dashboard --host 127.0.0.1 --port 8266
```

The server refuses a non-loopback listener unless
`--allow-unauthenticated` is present. That flag only acknowledges the risk; it
does not add protection. A remote listener exposes job topology and status and
can submit supported operator-rescale actions. Programmatic server creation has
the same default and requires `allow_unauthenticated=True` for a non-loopback
address. Control requests also reject an untrusted `Host`, a mismatched
`Origin`, or cross-site Fetch Metadata.

If remote access is required:

1. place the listener behind an authenticated, authorized TLS reverse proxy;
2. restrict ingress to the operations network and approved identities;
3. preserve or rewrite `Host` to a host trusted by the listener;
4. enforce request, rate, and session limits at the proxy;
5. retain proxy audit logs. Klein suppresses the base HTTP log but emits
   sanitized `dashboard.http.request` and `dashboard.control.*` structured
   events with `X-Request-ID`; Ray log retention is not an independent audit
   store;
6. do not rely on the built-in Host-header, same-origin, CSP, or frame checks as
   substitutes for authentication.

The current built-in server admits at most 64 active request handlers, keeps a
128-connection accept backlog, and applies a 10-second socket timeout. Saturated
requests receive HTTP 503, while a timed-out JSON request body receives HTTP
408. Treat these as defense-in-depth limits; the reverse proxy still owns
identity, policy, rate limits, and abuse protection.

The Python state API and CLI connect through Ray to a detached state actor and
JobManager. Any principal with sufficient Ray access to use those actors must
already be trusted to observe and control the jobs. Protect Ray Client, Ray
Jobs, the Ray Dashboard, GCS, and worker ports according to the platform's Ray
deployment. Ray token authentication is defense in depth, not a replacement
for network isolation; see
[Ray token authentication](https://docs.ray.io/en/latest/ray-security/token-auth.html).

Setting `observability.dashboard.enabled=false` stops publication to Klein's
state actor. It does not disable Ray logs, Ray metrics, checkpointing, or every
way trusted Ray code can address job actors.

## Credentials and sensitive data

Prefer workload identity, instance roles, mounted secret files, or a platform
secret provider. Avoid literals in source code, Table DDL, command lines,
resource plans, and committed configuration. Restrict each job identity to the
specific source, sink, and checkpoint resources it needs.

Klein applies limited, key-name-based redaction:

- published configuration recursively redacts keys resembling passwords,
  secrets, tokens, credentials, authorization values, cookies, access keys,
  private keys, and API keys;
- structured Klein log fields redact a similar set of names;
- Redis omits its password from the configuration object's representation.

Redaction is a guardrail, not a data-loss-prevention system. It does not scan
free-form strings or record values, and application or third-party loggers can
bypass it. Do not put sensitive values in:

- job, operator, task, metric, or namespace names;
- record values sent to a console sink;
- exception messages or manually formatted log text;
- metric labels;
- connector URLs or checkpoint paths;
- arbitrary configuration keys whose name does not look secret.

Configuration and connector objects are serialized to workers. Iceberg catalog
properties, checkpoint storage options, Kafka/Redis/RocketMQ settings, and
custom source/sink objects therefore remain visible to trusted worker code and
may also appear in process memory. Rotate a credential after suspected
exposure and review Ray logs, runtime-environment metadata, shell history, and
object-store access logs.

## Multi-tenancy

Klein does not provide isolation between hostile jobs in one Ray cluster.
Per-job Ray namespaces prevent named-actor collisions and help operations find
a job, but they do not prevent another trusted cluster client or workload from
discovering actors, consuming cluster resources, accessing worker-visible
credentials, or attacking shared services.

Use separate Ray clusters, service accounts, networks, checkpoint roots,
connector identities, log stores, and encryption domains when workloads or
teams do not fully trust one another. Resource quotas and placement groups are
capacity controls, not security boundaries. In-process debug mode provides
even less isolation and must not be used to evaluate production security.

## Production hardening checklist

- Allow only reviewed identities to submit Ray jobs or connect Ray clients.
- Keep all Ray and Klein control endpoints off the public internet.
- Enable the Ray authentication and encryption controls supported by the
  deployed Ray version, plus network isolation and external authorization.
- Run mutually untrusted workloads in separate Ray clusters.
- Use immutable, scanned head and worker images with the same pinned
  dependencies.
- Run workers without unnecessary host mounts, cloud permissions, Linux
  capabilities, or access to another job's secrets.
- Keep the Klein Dashboard on loopback, or place it behind authenticated TLS
  ingress with audit logging.
- Give each job a dedicated, versioned, encrypted checkpoint prefix and test
  restore with the same least-privilege identity.
- Use different identities for sources, sinks, and administrative recovery
  when the platform can support that separation.
- Export Ray/Klein logs and metrics to access-controlled durable stores; test
  that secret values do not appear in a representative failure.
- Define credential rotation, artifact rollback, checkpoint revocation, and
  incident-response procedures before launch.

For operational release gates, use the
[Production readiness checklist](production-readiness.md). For upgrade and
rollback security, including trusted checkpoint handling, see
[Upgrade Klein jobs](upgrading.md).
