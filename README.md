# slonk

Typed data pipelines with operator-overloaded `|` syntax.

Slonk lets you build data pipelines by chaining stages with Python's `|`
operator. Stages can be plain callables, shell commands, file paths,
SQLAlchemy models, or custom handler objects.

## Features

- **Pipe operator composition** — chain stages naturally with `|`.
- **Automatic handler inference** — strings become path or shell handlers,
  callables are wrapped based on their signature, SQLAlchemy models become
  database sources, transforms, or sinks depending on position.
- **Parallel execution** — stages run concurrently in threads connected by
  bounded queues with automatic backpressure (default mode).
- **Free-threaded Python support** — detected at runtime; uses threads
  instead of processes for `parallel()` when the GIL is disabled.
- **Role-based protocols** — `Source`, `Transform`, and `Sink` protocols
  define the contract for custom handlers.
- **Middleware system** — observe pipeline lifecycle events (start, end,
  error) and custom events without modifying handler code.
- **Built-in middleware** — `TimingMiddleware`, `LoggingMiddleware`, and
  `StatsMiddleware` ship out of the box.
- **Tee/fork** — split data to side pipelines with `tee()`.
- **Fan-in** — combine multiple sources with `merge()` (interleaved,
  concurrent) or `cat()` (ordered, sequential).
- **Data-parallel processing** — `parallel()` distributes work across a
  thread or process pool with `cloudpickle` serialisation.

## Requirements

- Python >= 3.14

## Installation

```bash
pip install slonk
```

Or with [uv](https://docs.astral.sh/uv/):

```bash
uv add slonk
```

## Quick start

```python
from slonk import Slonk

# Source -> Transform pipeline
result = (
    Slonk()
    | (lambda: ["hello", "world"])
    | (lambda data: [s.upper() for s in data])
).run()

print(list(result))  # ['HELLO', 'WORLD']
```

### With seed data

```python
result = (
    Slonk()
    | (lambda data: [s + "!" for s in data])
).run(["hi", "there"])

print(list(result))  # ['hi!', 'there!']
```

### Shell commands

```python
result = (
    Slonk()
    | (lambda: ["banana", "apple", "cherry"])
    | "sort"
).run()

print(list(result))  # ['apple', 'banana', 'cherry']
```

### File I/O

```python
# Write to a file and pass data through
pipeline = (
    Slonk()
    | (lambda: ["line 1", "line 2"])
    | "./output.txt"  # PathHandler: write + passthrough
)
```

Cloud and remote paths are supported via
[universal-pathlib](https://github.com/fsspec/universal_pathlib):

```python
# Read from S3, filter, write to GCS
pipeline = (
    Slonk()
    | "s3://input-bucket/events.csv"
    | (lambda rows: [r for r in rows if "ERROR" in r])
    | "gs://output-bucket/errors.csv"
)
```

### SQLAlchemy integration

Pipe SQLAlchemy models directly -- the handler adapts to its position
as Source, Transform, or Sink:

```python
from sqlalchemy import create_engine, Column, String
from sqlalchemy.orm import DeclarativeBase, sessionmaker

class Base(DeclarativeBase):
    pass

class Record(Base):
    __tablename__ = "records"
    id = Column(String, primary_key=True)
    data = Column(String)

engine = create_engine("sqlite:///data.db")
Session = sessionmaker(bind=engine)

# Read -> transform -> write back
pipeline = (
    Slonk(session_factory=Session)
    | Record                     # Source: read all rows
    | (lambda rows: [r.upper() for r in rows])
    | Record                     # Sink: bulk-write transformed rows
)
```

### Middleware

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

### Data-parallel execution

```python
from slonk import Slonk, parallel

result = (
    Slonk()
    | (lambda: [str(i) for i in range(1000)])
    | parallel(lambda chunk: [s + "!" for s in chunk], workers=4, chunk_size=250)
).run()
```

### Custom handlers

```python
from slonk import SlonkBase

class MyTransform(SlonkBase):
    def process_transform(self, input_data):
        for item in input_data:
            self.emit("processing", {"item": item})
            yield item.upper()
```

### Fan-in: merge and cat

Combine data from multiple pipelines into one stream:

```python
from slonk import Slonk, merge, cat

api  = Slonk() | (lambda: ["api_1", "api_2"])
logs = Slonk() | (lambda: ["log_1", "log_2"])

# merge — interleaved (non-deterministic order, concurrent)
result = (
    Slonk()
    | (lambda: ["upstream"])
    | merge(api, logs)
).run()

sorted(result)  # ['api_1', 'api_2', 'log_1', 'log_2', 'upstream']
```

```python
# cat — ordered (deterministic, sequential)
result = (
    Slonk()
    | (lambda: ["upstream"])
    | cat(api, logs)
).run()

list(result)  # ['upstream', 'api_1', 'api_2', 'log_1', 'log_2']
```

## Sequential mode

By default pipelines run with parallel execution (each stage in its own
thread).  Pass `parallel=False` for sequential execution:

```python
result = pipeline.run(parallel=False)
```

## Development

```bash
# Install dev dependencies
make install-dev

# Run tests (includes doctests)
make test

# Lint + type-check
make lint
make typecheck
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for full details.

## License

[MIT](LICENSE)
