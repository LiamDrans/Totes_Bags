#################################################################################
#
# Makefile to build the project
#
#################################################################################

PROJECT_NAME = totes_bags
REGION = eu-west-2
PYTHON_INTERPRETER = python
WD=$(shell pwd)
PYTHONPATH=${WD}
SHELL := /bin/bash
PROFILE = default
PIP:=pip
POETRY := $(shell command -v poetry)


## Create python interpreter environment.
create-environment:
	@echo ">>> About to create environment: $(PROJECT_NAME)..."
	@echo ">>> check python3 version"
	( \
		$(PYTHON_INTERPRETER) --version; \
	)
	@echo ">>> check poetry version"
	( \
		$(POETRY) --version; \
	)

## Build the environment requirements
requirements: create-environment
	$(POETRY) install

## Run the pylint check
run-pylint-src:
	$(POETRY) run pylint src

run-pylint: run-pylint-src

## Run the unit tests
unit-test:
	$(POETRY) run pytest tests

## Run all checks
run-checks: unit-test

## Run the security test (bandit + safety)
safetycheck:
	$(POETRY) run safety check -r ./pyproject.toml

banditcheck:
	$(POETRY) run bandit -lll */*.py

security-test: safetycheck banditcheck

check-coverage:
	$(POETRY) run pytest --cov=src tests/ --cov-fail-under=80
