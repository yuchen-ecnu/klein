# SPDX-License-Identifier: Apache-2.0
.PHONY: audit benchmark build clean coverage docs external format integration integration-connectors integration-runtime \
	integration-sql integration-state lint test unit unit-connectors unit-core unit-runtime unit-sql unit-state

format:
	ruff format .
	ruff check --fix .

lint:
	ruff format --check .
	ruff check .
	mypy

test:
	$(MAKE) unit

coverage:
	python -m pytest tests/unit tests/state tests/architecture -m "not slow" \
		--cov=ray.klein --cov-report=term --cov-report=json:coverage.json
	python scripts/check_coverage_policy.py coverage.json

benchmark:
	python scripts/benchmark_data_plane.py --quick --min-speedup 0.9

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
	python scripts/check_dependency_licenses.py

docs:
	sphinx-build -W --keep-going -b html docs docs/_build/html

build:
	python -m build
	python -m twine check dist/*
	python scripts/check_distribution.py dist/*

clean:
	rm -rf build dist docs/_build .coverage coverage.json coverage.xml htmlcov .pytest_cache .ruff_cache .mypy_cache
