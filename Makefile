UV_VERSION := 0.10.7
UV := uvx uv@$(UV_VERSION)

.PHONY: test test-verbose test-coverage test-fast clean install-dev help test-unit test-integration lint typecheck docs docs-serve

help: ## Show this help message
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install-dev: ## Install development dependencies
	$(UV) sync

test: ## Run all tests
	$(UV) run pytest

test-verbose: ## Run tests with verbose output
	$(UV) run pytest -v

test-coverage: ## Run tests with coverage report
	$(UV) run pytest --cov=slonk --cov-report=html --cov-report=term

test-fast: ## Run tests with minimal output, fail fast
	$(UV) run pytest -x -q

clean: ## Clean test artifacts
	rm -rf .pytest_cache
	rm -rf htmlcov
	rm -rf .coverage
	find . -type d -name __pycache__ -delete
	find . -name "*.pyc" -delete

test-unit: ## Run only unit tests (excluding integration tests)
	$(UV) run pytest -m "not integration"

test-integration: ## Run only integration tests
	$(UV) run pytest -m "integration"

lint: ## Run ruff linter and formatter check
	$(UV) run ruff check .
	$(UV) run ruff format --check .

typecheck: ## Run mypy type checker
	$(UV) run mypy src/

docs: ## Build documentation site
	$(UV) run --group docs mkdocs build --strict

docs-serve: ## Serve documentation locally for preview
	$(UV) run --group docs mkdocs serve
