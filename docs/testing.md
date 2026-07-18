---
myst:
  html_meta:
    description: "Run and write Klein for Ray unit, state, architecture, integration, and external-service tests."
---
<!-- SPDX-License-Identifier: Apache-2.0 -->

# Testing

Klein for Ray uses pytest with a `src` layout and importlib import mode. Tests are
classified by the dependency boundary they cross, not by a filename allowlist.

| Tier | Location | Purpose | Command |
| --- | --- | --- | --- |
| Unit | `tests/unit` | In-process behavior with dependencies replaced at their boundary | `make unit` |
| State | `tests/state` | Deterministic Object Store state contracts | `make unit` |
| Architecture | `tests/architecture` | Package and test-suite invariants | `make unit` |
| Integration | `tests/integration` | Behavior on a managed local Ray cluster | `make integration` |
| External | `tests/integration/external` | Kafka, Redis, Docker, or another service | `make external` |

`make test` is intentionally the fast, deterministic pull-request loop and is
an alias for `make unit`. Integration modules receive an isolated, managed Ray
runtime through the `ray_cluster` fixture. External tests are skipped unless
`--run-external` is supplied.

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
