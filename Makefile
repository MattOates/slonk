.PHONY: test test-verbose test-coverage test-fast clean install-dev help test-unit test-integration lint typecheck

help: ## Show this help message
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install-dev: ## Install development dependencies
	uv sync

test: ## Run all tests
	uv run pytest

test-verbose: ## Run tests with verbose output
	uv run pytest -v

test-coverage: ## Run tests with coverage report
	uv run pytest --cov=slonk --cov-report=html --cov-report=term

test-fast: ## Run tests with minimal output, fail fast
	uv run pytest -x -q

clean: ## Clean test artifacts
	rm -rf .pytest_cache
	rm -rf htmlcov
	rm -rf .coverage
	find . -type d -name __pycache__ -delete
	find . -name "*.pyc" -delete

test-unit: ## Run only unit tests (excluding integration tests)
	uv run pytest -m "not integration"

test-integration: ## Run only integration tests
	uv run pytest -m "integration"

lint: ## Run ruff linter and formatter check
	uv run ruff check .
	uv run ruff format --check .

typecheck: ## Run mypy type checker
	uv run mypy src/
