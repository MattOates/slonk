# Handlers

Handlers are the building blocks of a pipeline.  Each handler implements
one or more of the role protocols (`Source`, `Transform`, `Sink`).

## Built-in handlers

### PathHandler

Handles local and remote file I/O via
[universal-pathlib](https://github.com/fsspec/universal_pathlib).

- **Source**: reads lines from the file.
- **Transform**: writes input to the file and passes each item through.
- **Sink**: writes input to the file (no passthrough).

```python
from slonk import Slonk

# Read from a file (Source)
pipeline = Slonk() | "./input.txt"

# Write and pass through (Transform)
pipeline = Slonk() | (lambda: ["a", "b"]) | "./output.txt"

# Cloud paths work too
pipeline = Slonk() | "s3://bucket/data.csv"
```

### ShellCommandHandler

Pipes data through a shell command via `stdin`/`stdout`.

- **Transform**: feeds input to stdin via a writer thread, yields stdout
  lines as they arrive.

```python
pipeline = (
    Slonk()
    | (lambda: ["banana", "apple", "cherry"])
    | "sort"
    | "grep a"
)
```

### SQLAlchemyHandler

Read, upsert, or bulk-write rows for a SQLAlchemy declarative model.
Supports all three roles -- the role is assigned automatically based on
the handler's position in the pipeline.

- **Source**: fetches all rows using `yield_per()` for chunked streaming,
  yielding `"<id>\t<data>"` strings.
- **Transform**: upserts each incoming `"<id>\t<data>"` row via
  `session.merge()` and passes data through unchanged.
- **Sink**: bulk-writes incoming rows with periodic flushes and a single
  commit at the end.

```python
from sqlalchemy import create_engine, Column, String
from sqlalchemy.orm import DeclarativeBase, sessionmaker
from slonk import Slonk

class Base(DeclarativeBase):
    pass

class Record(Base):
    __tablename__ = "records"
    id = Column(String, primary_key=True)
    data = Column(String)

engine = create_engine("sqlite:///data.db")
Session = sessionmaker(bind=engine)

# Source: read all rows
pipeline = Slonk(session_factory=Session) | Record
for row in pipeline.run():
    print(row)  # "1\tsome data"

# Transform: upsert and pass through (middle position)
pipeline = (
    Slonk(session_factory=Session)
    | (lambda: ["10\tnew row", "11\tanother row"])
    | Record          # upserts each row, yields unchanged
    | "grep new"      # downstream sees the data
)

# Sink: efficient bulk write (last position with seed data)
pipeline = (
    Slonk(session_factory=Session)
    | (lambda: ["20\tbulk A", "21\tbulk B"])
    | Record          # bulk-writes, returns nothing
)
```

Requires passing a `session_factory` to `Slonk()`.  The tab-separated
format (`"<id>\t<data>"`) is the interchange format between stages.

### MergeHandler

Fan-in handler that merges upstream data with output from multiple
sub-pipelines concurrently — items are interleaved as they become
available (non-deterministic order).

- **Transform**: runs each sub-pipeline in its own thread, yielding
  items from any source as soon as they are ready.  Uses a shared
  bounded queue for backpressure.

Use the `merge()` convenience factory with the pipe operator, or call
`.merge()` on an existing pipeline:

```python
from slonk import Slonk, merge

api_data = Slonk() | (lambda: ["api_row1", "api_row2"])
db_data  = Slonk() | (lambda: ["db_row1", "db_row2"])

# Factory style — merge upstream + sub-pipelines
pipeline = (
    Slonk()
    | (lambda: ["upstream1", "upstream2"])
    | merge(api_data, db_data)
)
result = sorted(pipeline.run())
# ['api_row1', 'api_row2', 'db_row1', 'db_row2', 'upstream1', 'upstream2']
```

Because producers run in threads, items from different sources will be
interleaved.  Use `cat()` instead when deterministic ordering matters.

### CatHandler

Fan-in handler that concatenates upstream data with output from multiple
sub-pipelines in listed order — all upstream items first, then each
sub-pipeline sequentially (deterministic order).

- **Transform**: yields upstream items, then iterates each sub-pipeline
  in turn.  No threads are needed.

Use the `cat()` convenience factory or `.cat()` method:

```python
from slonk import Slonk, cat

source_a = Slonk() | (lambda: ["a1", "a2"])
source_b = Slonk() | (lambda: ["b1", "b2"])

pipeline = (
    Slonk()
    | (lambda: ["upstream"])
    | cat(source_a, source_b)
)
result = list(pipeline.run())
# ['upstream', 'a1', 'a2', 'b1', 'b2']
```

!!! note "Shell-inspired naming"
    `tee` (fan-out), `merge` (interleaved fan-in), and `cat`
    (ordered fan-in) mirror their Unix shell counterparts.

## Callable wrappers

Plain callables are automatically wrapped based on their signature:

```python
# Source (no params, returns iterable)
Slonk() | (lambda: ["a", "b"])

# Transform (takes input, returns iterable)
Slonk() | (lambda data: [s.upper() for s in data])

# Sink (takes input, returns None)
Slonk() | (lambda data: print(data))
```

## Custom handlers

Create custom handlers by subclassing `SlonkBase` and implementing
the appropriate protocol method(s):

```python
from slonk import SlonkBase

class MyTransform(SlonkBase):
    def process_transform(self, input_data):
        for item in input_data:
            self.emit("processing", {"item": item})
            yield item.strip().upper()
```

The `SlonkBase` mixin provides the `emit()` method for sending custom
events to middleware.

## Protocol-only handlers

You don't need to subclass `SlonkBase`.  Any object implementing the
protocol methods works:

```python
class MinimalSource:
    def process_source(self):
        return ["hello", "world"]

pipeline = Slonk() | MinimalSource()
```

However, without `SlonkBase` you won't have access to `emit()`.
