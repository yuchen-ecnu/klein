---
myst:
  html_meta:
    description: "Run and write Klein for Ray unit, state, architecture, integration, and external-service tests."
---
<!-- SPDX-License-Identifier: Apache-2.0 -->

# Testing

Klein for Ray uses pytest with a `src` layout and importlib import mode. Tests are
classified on two independent axes: a **tier** describes the dependency boundary
the test crosses, while a **component** identifies the subsystem that owns it.

| Tier | Location | Purpose | Command |
| --- | --- | --- | --- |
| Unit | `tests/unit` | In-process behavior with dependencies replaced at their boundary | `make unit` |
| State | `tests/state` | Deterministic backend and checkpoint-storage contracts | `make unit` |
| Architecture | `tests/architecture` | Package and test-suite invariants | `make unit` |
| Integration | `tests/integration` | Behavior on a managed local Ray cluster | `make integration` |
| External | `tests/integration/external` | Kafka, Redis, Docker, or another service | `make external` |

`make test` is intentionally the fast, deterministic pull-request loop and is
an alias for `make unit`. Integration modules receive an isolated, managed Ray
runtime through the `ray_cluster` fixture. External tests are skipped unless
`--run-external` is supplied.

## CI components

Every collected test receives exactly one component marker from
`tests/component_suites.py`. The same markers drive local Make targets and the
GitHub Actions jobs, preventing the two command sets from drifting apart.

| Component | Marker | Unit command | Integration command |
| --- | --- | --- | --- |
| Core | `component_core` | `make unit-core` | Covered through its runtime consumers |
| Runtime | `component_runtime` | `make unit-runtime` | `make integration-runtime` |
| State | `component_state` | `make unit-state` | `make integration-state` |
| SQL | `component_sql` | `make unit-sql` | `make integration-sql` |
| Connectors | `component_connectors` | `make unit-connectors` | `make integration-connectors` |

The pull-request workflow encodes the architectural dependency order explicitly:

```text
quality -> core -> {runtime, state}
runtime + state -> SQL
runtime -> connectors
runtime integration -> {state integration, connector integration}
state integration -> SQL integration
connector integration -> external services
```

State integration includes real-Ray Memory and RocksDB checkpoint restoration
across a parallelism change. SQL integration restores a managed streaming
aggregate from a durable checkpoint. Runtime integration kills a live StreamTask
and requires the JobManager to replace it while keeping the job running.

## Write tests

- Prefer plain test functions, pytest fixtures, and `pytest.mark.parametrize`.
- Put reusable doubles and assertion helpers in `tests/support`; do not import
  helpers from another test tier.
- Use `tmp_path` and `monkeypatch` for files, environment variables, and global
  state. Do not manually restore process state in `finally` blocks.
- Assert complete observable results. Avoid print-only assertions and tests
  whose only expectation is that code does not raise.
- Replace sleeps and open-ended polling with the `eventually` fixture. Every
  asynchronous wait must have a timeout and an actionable failure message.
- A test that starts Ray belongs in `tests/integration`. A test that requires a
  service or Docker also belongs in `tests/integration/external`.
- Keep test data immutable in `tests/data`; create generated output under
  `tmp_path`.

Per-test timeouts, strict marker/config validation, strict expected failures,
and warnings-as-errors are enabled globally in `pyproject.toml`.

Architecture tests also enforce the observability boundaries: production
modules use component-scoped Klein loggers, logging calls use deferred `%s`
formatting, stdout writes are limited to declared user-facing boundaries, and
connector command buffers aren't embedded in log messages.

Documentation contract tests also keep the reference synchronized with code:

- every top-level export is accounted for in the API reference;
- every public `DataStream` member is listed;
- every declared `ConfigOption` appears in the configuration reference;
- standalone examples compile;
- CLI and checkpoint-recovery guides use the current command and option names.

`tests/integration/test_documented_examples.py` executes the standalone batch
SQL and finite stateful-streaming examples on the managed local Ray cluster.
The Sphinx CI job separately treats documentation warnings as errors.
