# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- Comprehensive Google-style docstrings on all public and key internal symbols.
- Doctest examples throughout the library, tested via `--doctest-modules`.
- MkDocs Material documentation site with API reference.
- `LICENSE` (MIT), `CONTRIBUTING.md`, and `CHANGELOG.md`.
- GitHub Actions workflow for documentation deployment.
- `docs` and `docs-serve` Makefile targets.

## [0.1.1] - 2026

### Added

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

### Changed

- Replaced `cloudpathlib` with `universal-pathlib` (UPath).
- Migrated toolchain from Rye to uv.
- Requires Python >= 3.14.

## [0.1.0] - 2024

### Added

- Initial implementation of `Slonk` pipeline builder with `|` operator.
- `PathHandler` for local/cloud file I/O.
- `ShellCommandHandler` for piping data through shell commands.
- `SQLAlchemyHandler` for querying database rows.
- Basic callable wrapping with signature inference.
