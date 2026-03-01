# CHANGELOG

<!-- version list -->

## v0.3.0 (2026-03-01)

### Features

- Add merge() and cat() fan-in primitives for combining multiple data sources
  ([`a18894e`](https://github.com/MattOates/slonk/commit/a18894eddacb1aaaff31d0b351350dbc9d020727))


## v0.2.0 (2026-03-01)

### Bug Fixes

- Resolve SIM212 ruff lint in test_edge_cases.py
  ([`96fdd25`](https://github.com/MattOates/slonk/commit/96fdd25c4a923bd6fb336789fac2e6c4932031ba))

### Features

- Extend SQLAlchemyHandler with Transform and Sink roles, add data engineering docs
  ([`1dc18d2`](https://github.com/MattOates/slonk/commit/1dc18d2a4ae37564ec23b21939f9dfb0f9f7d2a3))


## v0.1.1 (2026-03-01)

### Feature

- Comprehensive Google-style docstrings on all public and key internal symbols.
- Doctest examples throughout the library, tested via `--doctest-modules`.
- MkDocs Material documentation site with API reference.
- `LICENSE` (MIT), `CONTRIBUTING.md`, and `CHANGELOG.md`.
- GitHub Actions workflow for documentation deployment.
- `docs` and `docs-serve` Makefile targets.
- Middleware system with `Middleware` base class, `_EventDispatcher`, and
  built-in `TimingMiddleware`, `LoggingMiddleware`, `StatsMiddleware`.
- Multi-module package split (was single-file).
- Role-based protocol architecture (`Source`, `Transform`, `Sink`).
- Parallel/streaming pipeline execution via `_StreamingPipeline` (threads
  connected by bounded queues with backpressure).
- `parallel()` wrapper for data-parallel execution across threads
  (free-threaded Python) or processes (standard Python via cloudpickle).
- `TeeHandler` and `tee()` for forking data to side pipelines.
- `SlonkBase` mixin for custom event emission.
- GitHub Actions CI (lint, typecheck, test on Python 3.14 and 3.14t).
- Ruff and mypy configuration.
- Replaced `cloudpathlib` with `universal-pathlib` (UPath).
- Migrated toolchain from Rye to uv.
- Requires Python >= 3.14.

## v0.1.0 (2024-01-01)

### Feature

- Initial implementation of `Slonk` pipeline builder with `|` operator.
- `PathHandler` for local/cloud file I/O.
- `ShellCommandHandler` for piping data through shell commands.
- `SQLAlchemyHandler` for querying database rows.
- Basic callable wrapping with signature inference.
