# SPDX-License-Identifier: Apache-2.0
.PHONY: audit build clean docs external format integration lint test unit

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

integration:
	python -m pytest tests/integration -m "integration and not external" --timeout=300

external:
	python -m pytest tests/integration/external -m external --run-external --timeout=300

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
