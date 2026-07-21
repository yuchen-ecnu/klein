---
myst:
  html_meta:
    description: "Install Klein for Ray from source, select optional integrations, and keep every Ray worker on a compatible environment."
---
<!-- SPDX-License-Identifier: Apache-2.0 -->

(klein-installation)=
# Install Klein for Ray

Klein is currently alpha software. This guide installs it from a source
checkout or from an artifact that you build from that checkout; it does not
assume that a `ray-klein` distribution is available from PyPI.

## Supported environment

| Component | Supported range | Notes |
| --- | --- | --- |
| Python | 3.10, 3.11, or 3.12 | Use the same minor version on the driver and workers. |
| Ray | `ray[data]>=2.56.1,<2.57` | Klein pins one Ray minor because some Ray Data extension points are Developer APIs. |
| Operating system | Linux or macOS | These are the platforms declared by the package metadata. Other platforms are not part of the current support target. |
| Project maturity | Alpha | Public APIs and checkpoint formats can still change before 1.0. |

The base installation also constrains NumPy to `<3`, pandas to `<3`, protobuf
to `<7`, and SQLGlot to `>=30.12,<31`. Let the package resolver install these
dependencies instead of overriding them independently. Read
[Compatibility](compatibility.md) before changing Python, Ray, protobuf, or
checkpoint-producing Klein versions.

Use an isolated environment so another Ray application cannot silently widen
the supported Ray range:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
```

## Regular installation from a checkout

Clone the repository, then perform a non-editable installation for ordinary
use:

```bash
git clone https://github.com/yuchen-ecnu/klein.git
cd klein
python -m pip install .
```

This builds and installs the current checkout plus the base dependencies,
including `ray[data]`. The installed code does not change when the checkout is
edited; reinstall it to pick up a new revision.

If your organization builds a wheel from a reviewed checkout, install that
same wheel on the driver and workers:

```bash
python -m pip install "/path/to/ray_klein-<version>-py3-none-any.whl"
```

Klein contributes the `ray.klein` namespace. It does not replace Ray's
`ray/__init__.py`; both the compatible Ray package and Klein must be installed
in the same environment.

## Editable source installation

Use an editable installation when experimenting with the source tree:

```bash
git clone https://github.com/yuchen-ecnu/klein.git
cd klein
python -m pip install -e .
```

Python code changes are then visible without reinstalling. Re-run the install
after changing `pyproject.toml`, switching to a revision with different
dependencies, or selecting another optional extra.

## Contributor installation

The `dev` extra includes every integration, the documentation and test
dependencies, pre-commit, and Ruff:

```bash
python -m pip install -e ".[dev]"
pre-commit install
```

Run the normal contributor checks with:

```bash
make lint
make test
make docs
```

Integration tests start a managed local Ray cluster, while external-service
tests are opt-in. See [Testing](testing.md) for the test tiers and commands.

## Optional extras

Install only the integrations used by the application. Extras can be combined,
for example `python -m pip install -e ".[kafka,rocksdb]"` in an editable
checkout.

| Extra | Installed dependencies | Use it for |
| --- | --- | --- |
| `kafka` | `confluent-kafka>=2.3,<3` | Bounded or continuous Kafka input and Kafka output. |
| `iceberg` | `pyiceberg>=0.11.1,<0.12` | Iceberg catalog access and batch or checkpointed append output. Catalog-specific dependencies may still be required. |
| `rocketmq` | `rocketmq-client-python>=2,<3` | Continuous RocketMQ input. A compatible native `librocketmq` is also required on every executing worker. |
| `redis` | `redis>=5,<7` | Redis lookup, filtering, and output. |
| `rocksdb` | `rocksdict>=0.3.29,<0.4` | The node-local RocksDB managed-state backend. |
| `serve` | `aiohttp>=3.13.3`, `orjson>=3.9`, and compatible `ray[serve]` | Ray Serve execution regions and the embedded proxy client. |
| `all` | All runtime integrations above | An integration-development environment; it does not include test or documentation tools. |
| `test` | pytest, build/audit tools, testcontainers, and test-only Iceberg support | Running the repository test suites. |
| `docs` | Sphinx, MyST, the PyData theme, copybutton, and sphinx-design | Building this documentation. |
| `dev` | `all`, `docs`, `test`, pre-commit, and Ruff | Full contributor setup. |

The [connector catalog](connectors/index.md) gives each connector's execution
modes, data shape, configuration, and delivery guarantee. A connector may also
need a broker, database driver, filesystem credential provider, or native
library that cannot be supplied by a Python extra.

## Keep the cluster environment consistent

Installing Klein only on the submission machine is insufficient. Every Ray
worker that can run a Klein source, operator, sink, or user function needs:

- the same Klein artifact and compatible Ray version;
- the extras imported by that graph;
- application modules and user-function dependencies;
- native libraries such as `librocketmq`; and
- compatible credentials and filesystem drivers for external services.

Prefer one immutable container image or one versioned wheel set across the Ray
head and workers. A Ray runtime environment is also suitable when it installs
the same immutable artifacts on every eligible node. Do not install packages
from inside a user function: different worker startup times and dependency
resolution can make deployment nondeterministic.

If imports succeed on the driver but fail remotely, compare the interpreter,
`ray-klein`, Ray, and extra versions inside the failing worker image. See
[Deploy Klein jobs](deployment.md) for Ray Jobs, containers, and KubeRay
guidance.

## Verify the installation

Check the selected interpreter and installed distribution metadata:

```bash
python - <<'PY'
from importlib.metadata import version

import ray
import ray.klein

print("Python integration: OK")
print("ray-klein:", version("ray-klein"))
print("Ray:", ray.__version__)
PY

python -m pip check
ray-klein --version
```

From a source checkout, run the bounded smoke test as well:

```bash
python examples/quick_start.py
```

The import and version checks do not start a Ray cluster. The example starts
or reuses the runtime needed by its bounded Ray Data execution.

## Upgrade or change extras

Review [Upgrade Klein jobs](upgrading.md) and [Compatibility](compatibility.md)
before upgrading an application that must restore checkpoints. Then update the
checkout and reinstall the required extras:

```bash
python -m pip install --upgrade ".[kafka,rocksdb]"
```

For an editable contributor environment, re-run:

```bash
python -m pip install -e ".[dev]"
```

Do not upgrade Ray past the declared `<2.57` bound independently. For a
production cluster, build a new immutable environment, rehearse checkpoint
restore against production-shaped state, and follow the cutover procedure in
[Deploy Klein jobs](deployment.md#upgrade-procedure).

## Uninstall

Remove Klein with the interpreter that owns the installation:

```bash
python -m pip uninstall ray-klein
```

Pip does not automatically remove dependencies that were installed through
Klein's extras. Review them before uninstalling anything shared with another
application. An editable uninstall removes the environment link but does not
delete the source checkout. Do not manually delete the surrounding `ray`
package: Ray is a separate distribution.

## Troubleshoot installation

| Symptom | Check and fix |
| --- | --- |
| Pip rejects the Python version | Run `python --version`. Create a Python 3.10–3.12 environment and invoke pip as `python -m pip`. |
| The resolver reports a Ray conflict | Another package requires a Ray version outside `>=2.56.1,<2.57`. Use a separate environment rather than widening Klein's bound without compatibility testing. |
| `ModuleNotFoundError: ray.klein` | Confirm `python -m pip show ray-klein` uses the same interpreter as the application. Merely placing the checkout beside an installed Ray package is not an installation. |
| `ray-klein: command not found` | Activate the environment and confirm its `bin` directory is on `PATH`. Reinstall the project to create the console entry point. |
| A connector module is missing | Install its named extra on the driver and every eligible worker, then restart or redeploy those processes. |
| RocketMQ loads in Python but cannot load its client library | Install a compatible native `librocketmq` in the worker image and make it available to the platform dynamic linker. |
| The driver imports a dependency but a Ray task cannot | The cluster environments differ. Deploy the same wheel/image and extras to all nodes; a local driver virtual environment is not propagated automatically. |
| `python -m pip check` reports protobuf or SQLGlot conflicts | Keep the bounds declared by `pyproject.toml`, or isolate Klein from the application requiring the incompatible version. |
| Installation succeeds but the CLI cannot connect | This is a cluster connection issue, not an import issue. Start or select a Ray cluster and see [CLI reference](cli-reference.md#connect-to-the-ray-cluster) and [Troubleshooting](troubleshooting.md). |
