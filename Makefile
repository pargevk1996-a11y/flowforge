.DEFAULT_GOAL := help
VENV := .venv
PY := $(VENV)/bin/python
PIP := $(VENV)/bin/pip

.PHONY: help venv install lint typecheck test check up down ui ui-install serve

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

venv: ## Create the virtualenv
	python3.12 -m venv $(VENV)

install: venv ## Install the package with dev + serve extras
	$(PIP) install -e ".[dev,serve]"

lint: ## Run ruff
	$(VENV)/bin/ruff check src tests workflows

typecheck: ## Run mypy --strict
	$(VENV)/bin/mypy

test: ## Run the test suite
	$(VENV)/bin/pytest

ui-install: ## Install the debugger UI's dependencies
	npm --prefix ui ci

ui: ## Typecheck, test and build the debugger UI
	npm --prefix ui run typecheck && npm --prefix ui test && npm --prefix ui exec vite build

serve: ## Run the control plane + debugger on :8000 (demo workflows, canned LLM)
	$(VENV)/bin/flowforge api --demo

check: lint typecheck test ## Run the full Python gate

up: ## Start local infra (postgres + redis)
	docker compose up -d

down: ## Stop local infra
	docker compose down
