# Installation

## Requirements

- Python >= 3.14

## From PyPI

```bash
pip install slonk
```

Or with [uv](https://docs.astral.sh/uv/):

```bash
uv add slonk
```

## From source

```bash
git clone https://github.com/MattOates/slonk.git
cd slonk
uv sync
```

## Development installation

To install with development and documentation dependencies:

```bash
uv sync --group dev --group docs
```

Or use the Makefile shortcut:

```bash
make install-dev
```

## Dependencies

Slonk depends on:

- **[universal-pathlib](https://github.com/fsspec/universal_pathlib)** --
  unified path handling for local and remote filesystems
- **[SQLAlchemy](https://www.sqlalchemy.org/)** -- database ORM for
  `SQLAlchemyHandler`
- **[cloudpickle](https://github.com/cloudpipe/cloudpickle)** --
  serialisation for `parallel()` in process-pool mode
