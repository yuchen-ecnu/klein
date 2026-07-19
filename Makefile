# SPDX-License-Identifier: Apache-2.0
.PHONY: audit build clean docs external format integration integration-connectors integration-runtime \
	integration-sql integration-state lint test unit unit-connectors unit-core unit-runtime unit-sql unit-state

format:
	ruff format .
	ruff check --fix .

lint:
	ruff format --check .
	ruff check .

test:
	$(MAKE) unit

unit:
	python -m pytest tests/unit tests/state tests/architecture -m "not slow"

unit-core:
	python -m pytest tests/unit tests/state tests/architecture -m "component_core and not slow"

unit-runtime:
	python -m pytest tests/unit tests/state tests/architecture -m "component_runtime and not slow"

unit-state:
	python -m pytest tests/unit tests/state tests/architecture -m "component_state and not slow"

unit-sql:
	python -m pytest tests/unit tests/state tests/architecture -m "component_sql and not slow"

unit-connectors:
	python -m pytest tests/unit tests/state tests/architecture -m "component_connectors and not slow"

integration:
	python -m pytest tests/integration -m "integration and not external" --timeout=300

integration-runtime:
	python -m pytest tests/integration -m "integration and component_runtime and not external" --timeout=300

integration-state:
	python -m pytest tests/integration -m "integration and component_state and not external" --timeout=300

integration-sql:
	python -m pytest tests/integration -m "integration and component_sql and not external" --timeout=300

integration-connectors:
	python -m pytest tests/integration -m "integration and component_connectors and not external" --timeout=300

external:
	python -m pytest tests/integration/external -m "external and component_connectors" --run-external --timeout=300

audit:
	reuse lint
	pip-audit --local --skip-editable --progress-spinner off
	licensecheck --requirements-paths pyproject.toml --license Apache-2.0 --extras all --zero

docs:
	sphinx-build -W --keep-going -b html docs docs/_build/html

build:
	python -m build
	python -m twine check dist/*

clean:
	rm -rf build dist docs/_build .coverage coverage.xml htmlcov .pytest_cache .ruff_cache .mypy_cache
