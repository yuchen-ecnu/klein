---
myst:
  html_meta:
    description: "Klein for Ray source layout, package ownership rules, public API boundaries, and architecture checks."
---
<!-- SPDX-License-Identifier: Apache-2.0 -->

# Package structure

Klein for Ray uses a conventional `src` layout. The importable package is under
`src/ray/klein`; tests, examples, and documentation do not live inside the
runtime package.

```text
ray-klein/
├── pyproject.toml
├── src/ray/klein/
│   ├── api/              # stable user contracts and graph-building API
│   ├── config/           # configuration values, modes, and option namespaces
│   ├── integrations/     # external source and sink integrations
│   ├── state/            # progress and computational-state abstractions
│   ├── runtime/          # scheduling and execution implementation
│   ├── observability/    # metrics and lineage
│   └── _internal/        # non-public helpers
├── tests/
├── examples/
└── docs/
```

## Import public APIs

Applications should start with the short, stable imports:

```python
import ray
from ray.klein import Configuration, DataStream, KleinContext
from ray.klein.api import RuntimeContext, SinkFunction, SourceFunction
from ray.klein.config import ExecutionOptions, RuntimeExecutionMode
```

The package `__init__` modules lazily resolve public names. Internal modules
continue to import from the module that defines a symbol; they do not import
through a convenience re-export. This keeps dependency cycles visible.

## Follow naming and ownership rules

1. A public class is defined in a snake-case module with the corresponding
   name: `DataStream` lives in `data_stream.py`, `StateHandle` in
   `state_handle.py`, and so on.
2. A public module normally defines one public class. Small private helper
   classes may remain beside the implementation that exclusively owns them.
3. Package `__init__.py` files contain exports and package documentation, not
   implementations.
4. `api` owns user contracts and graph construction. `runtime` implements
   those contracts. An integration depends on contracts, configuration, and
   source-owned state rather than scheduler internals.
5. `_internal` is not a compatibility promise. New application code must not
   import it.
6. Generic buckets such as `common`, `core`, `utils`, and `entity` are not
   architectural layers. A helper moves to the domain that owns it, or into
   `_internal` when it is genuinely cross-cutting and private.

## Check dependency boundaries

Architecture tests enforce package boundaries and the one-public-class
convention in stable packages. Ruff validates imports, exception handling, and
cyclomatic complexity. Full verification also imports the top-level API, builds
the wheel and source distribution, and runs documentation checks.
