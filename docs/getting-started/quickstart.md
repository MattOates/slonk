# Quick Start

This guide walks through building your first Slonk pipeline.

## A minimal pipeline

A pipeline needs at least one stage.  The simplest is a **source** --
a callable that takes no arguments and returns an iterable of strings:

```python
from slonk import Slonk

pipeline = Slonk() | (lambda: ["hello", "world"])
result = pipeline.run()
print(list(result))  # ['hello', 'world']
```

## Adding transforms

Chain a **transform** to process data flowing through:

```python
pipeline = (
    Slonk()
    | (lambda: ["hello", "world"])
    | (lambda data: [s.upper() for s in data])
)
result = pipeline.run()
print(list(result))  # ['HELLO', 'WORLD']
```

## Seed data

Instead of starting with a source, you can feed data into the pipeline
at run time:

```python
pipeline = Slonk() | (lambda data: [s + "!" for s in data])
result = pipeline.run(["hi", "there"])
print(list(result))  # ['hi!', 'there!']
```

## Shell commands

Plain strings that don't look like file paths are treated as shell commands:

```python
pipeline = (
    Slonk()
    | (lambda: ["banana", "apple", "cherry"])
    | "sort"
)
result = pipeline.run()
print(list(result))  # ['apple', 'banana', 'cherry']
```

## File paths

Strings that start with `/`, `./`, `../`, or a known URI scheme
(e.g. `s3://`) are treated as file paths:

```python
pipeline = (
    Slonk()
    | (lambda: ["line 1", "line 2"])
    | "./output.txt"
)
# Writes to output.txt and passes data through
```

Cloud and remote paths are supported via
[universal-pathlib](https://github.com/fsspec/universal_pathlib):

```python
# Read from S3
pipeline = Slonk() | "s3://my-bucket/input.csv"

# Write to GCS and pass through
pipeline = (
    Slonk()
    | (lambda: ["row1", "row2"])
    | "gs://my-bucket/output.csv"
)
```

## SQLAlchemy models

Pipe a SQLAlchemy model directly into a pipeline.  The handler
automatically adapts to its position -- as a **Source** (first stage),
**Transform** (middle, upserts + passthrough), or **Sink** (last stage,
bulk write):

```python
from sqlalchemy import create_engine, Column, String
from sqlalchemy.orm import DeclarativeBase, sessionmaker

class Base(DeclarativeBase):
    pass

class User(Base):
    __tablename__ = "users"
    id = Column(String, primary_key=True)
    data = Column(String)

engine = create_engine("sqlite:///app.db")
Session = sessionmaker(bind=engine)

# Read all rows (Source)
pipeline = Slonk(session_factory=Session) | User
rows = list(pipeline.run())  # ["1\tAlice", "2\tBob", ...]

# Transform: upsert rows into the DB and pass through
pipeline = (
    Slonk(session_factory=Session)
    | (lambda: ["3\tCharlie", "4\tDiana"])
    | User        # upserts each row, passes data through
    | "grep Diana"
)

# Sink: bulk-write rows (last stage, returns empty)
pipeline = (
    Slonk(session_factory=Session)
    | (lambda: ["5\tEve", "6\tFrank"])
    | User        # bulk-writes, returns nothing
)
```

## Sequential mode

By default pipelines run in parallel (each stage in its own thread).
Use `parallel=False` for sequential execution:

```python
result = pipeline.run(parallel=False)
```

## Adding middleware

Middleware lets you observe pipeline execution without modifying handlers:

```python
from slonk import Slonk, TimingMiddleware

tm = TimingMiddleware()
pipeline = Slonk()
pipeline.add_middleware(tm)

pipeline |= (lambda: ["a", "b", "c"])
pipeline |= (lambda data: [s.upper() for s in data])
pipeline.run()

print(f"Pipeline took {tm.pipeline_duration:.4f}s")
```

## Next steps

- [Pipeline Concepts](../guide/concepts.md) -- understand sources,
  transforms, sinks, and roles
- [Handlers](../guide/handlers.md) -- built-in handler types
- [Middleware](../guide/middleware.md) -- observability hooks
- [Parallel Execution](../guide/parallel.md) -- threading, backpressure,
  and data parallelism
- [Recipes](../guide/recipes.md) -- real-world data engineering patterns
