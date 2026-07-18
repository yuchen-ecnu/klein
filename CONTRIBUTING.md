<!-- SPDX-License-Identifier: Apache-2.0 -->

# Contributing to Klein for Ray

Thank you for helping improve Klein for Ray. By participating, you agree to follow
the [Code of Conduct](CODE_OF_CONDUCT.md), and you certify each contribution
under the Developer Certificate of Origin described below.

## Development setup

```bash
git clone https://github.com/yuchen-ecnu/klein.git
cd klein
python -m venv .venv
source .venv/bin/activate
python -m pip install -U pip
python -m pip install -e ".[dev]"
pre-commit install
```

Before opening a pull request, run:

```bash
make lint
make test
make docs
make audit
python -m build
python -m twine check dist/*
```

The test suite has explicit unit, integration, and external-service tiers.
Run `make integration` before changing streaming runtime behavior and
`make external` before changing Kafka, Redis, or other service integrations.
See the [testing guide](docs/testing.md) for fixture and test-writing rules.

The project enforces a 65% branch-coverage floor. New and changed behavior must
have focused tests; do not weaken the threshold or add exclusions to land a
change.

## Pull requests

1. Open an issue for substantial user-facing or architectural changes.
2. Keep changes focused and add tests for observable behavior.
3. Update public documentation and `CHANGELOG.md` when behavior changes.
4. Avoid importing new `ray._private` or `ray.data._internal` APIs. If there is
   no public alternative, isolate the import in `ray.klein._compat`, document
   the reason, and add a compatibility test.
5. Add an Apache-2.0 SPDX header to new source and configuration files.

## Developer Certificate of Origin

All commits must be signed off with `git commit -s`. The sign-off certifies the
[Developer Certificate of Origin 1.1](https://developercertificate.org/). It
appears in the commit message as:

```text
Signed-off-by: Your Name <your.email@example.com>
```

## Reporting security issues

Do not disclose suspected vulnerabilities in a public issue. Follow
[SECURITY.md](SECURITY.md).
