# Contributing to Slonk

Thank you for considering contributing to Slonk! This document explains
how to set up a development environment and the conventions we follow.

## Prerequisites

- **Python >= 3.14** (free-threaded builds supported)
- **[uv](https://docs.astral.sh/uv/)** for dependency management

## Development setup

```bash
# Clone the repository
git clone https://github.com/MattOates/slonk.git
cd slonk

# Install dependencies (dev + docs groups)
make install-dev

# Run the full test suite
make test

# Lint and type-check
make lint
make typecheck
```

## Running tests

```bash
# All tests (includes doctests from source modules)
make test

# Fast mode (stop on first failure)
make test-fast

# With coverage
make test-coverage

# Only unit tests
make test-unit
```

The test suite uses a parameterized `run_pipeline` fixture that runs each
test in both `[sync]` and `[parallel]` modes.  If a test is incompatible
with one mode, mark it with `@pytest.mark.sync_only` or
`@pytest.mark.parallel_only`.

## Code style

- **Formatter/Linter**: [Ruff](https://docs.astral.sh/ruff/) with a
  100-character line limit.
- **Type checker**: [mypy](https://mypy-lang.org/) in strict mode.
- **Docstrings**: Google style.  All public symbols must have docstrings
  with `>>>` examples that are tested by `--doctest-modules`.
- **Imports**: sorted by ruff/isort; `from __future__ import annotations`
  in every module.
- Ruff enforces `type` statements over `TypeAlias`, `X | Y` over `Union`,
  and `strict=True` on `zip()`.

Run checks before committing:

```bash
make lint
make typecheck
make test
```

## Project structure

```
src/slonk/
  __init__.py           # Public API re-exports
  constants.py          # Sentinels, config, free-threading detection
  roles.py              # _Role enum, Source/Transform/Sink protocols
  base.py               # SlonkBase mixin (middleware event emission)
  middleware.py          # Middleware base class, event dispatcher
  queue.py              # Queue utilities for streaming pipeline
  handlers.py           # PathHandler, ShellCommandHandler, SQLAlchemyHandler, callable wrappers, parallel()
  streaming.py          # _StreamingPipeline (threaded executor)
  pipeline.py           # Slonk class, TeeHandler, tee(), _compute_roles()
  builtin_middleware.py  # TimingMiddleware, LoggingMiddleware, StatsMiddleware
tests/
  conftest.py           # Shared fixtures (run_pipeline parametrisation)
  helpers.py            # Test models (_TestBase, _TestModel)
  test_*.py             # Test modules
```

## Submitting changes

1. Fork the repository and create a feature branch.
2. Make your changes with tests.
3. Ensure `make lint`, `make typecheck`, and `make test` all pass.
4. Open a pull request with a clear description of what changed and why.

## License

By contributing you agree that your contributions will be licensed under
the [MIT License](LICENSE).
