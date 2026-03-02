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

Runs a shell command, piping data through `stdin`/`stdout`.

- **Source**: runs the command with no stdin, yielding stdout lines.
  Useful for commands that generate output on their own (e.g. `echo`,
  `seq`, `find`, `ls`).
- **Transform**: feeds input to stdin via a writer thread, yields stdout
  lines as they arrive.

```python
# Source: run a command that generates output
pipeline = Slonk() | "seq 10"

# Transform: pipe data through sort and grep
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

### FilterHandler

Keep only items where a predicate returns truthy.

- **Transform**: evaluates the predicate per item.

Use the `filter()` factory or `.filter()` method:

```python
from slonk import Slonk, filter

# Factory style
pipeline = (
    Slonk()
    | (lambda: ["error: disk full", "info: ok", "error: timeout"])
    | filter(lambda line: line.startswith("error"))
)
result = list(pipeline.run())
# ['error: disk full', 'error: timeout']

# Method style
pipeline = (
    Slonk()
    | (lambda: [1, 2, 3, 4, 5])
).filter(lambda x: x % 2 == 0)
# yields 2, 4
```

!!! note
    The `filter()` factory shadows the Python builtin `filter`.
    Import it explicitly from `slonk` when you need it.

### MapHandler

Apply a function to each item individually, yielding the transformed
result.  Unlike a transform callable (which receives the whole iterable),
`map` operates per-item.

- **Transform**: calls `func(item)` for each item.

Use the `map()` factory or `.map()` method:

```python
from slonk import Slonk, map

pipeline = (
    Slonk()
    | (lambda: ["hello", "world"])
    | map(str.upper)
)
result = list(pipeline.run())
# ['HELLO', 'WORLD']
```

!!! note
    The `map()` factory shadows the Python builtin `map`.
    Import it explicitly from `slonk` when you need it.

### FlattenHandler

Flatten one level of nesting -- if an item is iterable, yield its
elements individually.  Strings and bytes are treated as atoms (not
expanded into characters).

- **Transform**: yields sub-elements of iterable items.

Use the `flatten()` factory or `.flatten()` method:

```python
from slonk import Slonk, flatten

pipeline = (
    Slonk()
    | (lambda: [["a", "b"], ["c", "d"], "e"])
    | flatten()
)
result = list(pipeline.run())
# ['a', 'b', 'c', 'd', 'e']
```

### HeadHandler

Yield only the first *n* items, then drain the rest.  The drain
ensures upstream queues are not blocked in parallel mode.

- **Transform**: yields at most *n* items.

Use the `head()` factory or `.head()` method:

```python
from slonk import Slonk, head

pipeline = (
    Slonk()
    | (lambda: range(1000))
    | head(5)
)
result = list(pipeline.run())
# [0, 1, 2, 3, 4]
```

### SkipHandler

Skip the first *n* items and yield the rest.

- **Transform**: discards *n* leading items.

Use the `skip()` factory or `.skip()` method:

```python
from slonk import Slonk, skip

# Skip a CSV header row
pipeline = (
    Slonk()
    | "./data.csv"
    | skip(1)
    | (lambda rows: [process(r) for r in rows])
)
```

### TailHandler

Yield only the last *n* items.  The entire input must be consumed
before any output is produced (uses a bounded `deque` internally so
memory stays at O(n)).

- **Transform**: yields the last *n* items.

Use the `tail()` factory or `.tail()` method:

```python
from slonk import Slonk, tail

pipeline = (
    Slonk()
    | (lambda: ["first", "second", "third", "fourth"])
    | tail(2)
)
result = list(pipeline.run())
# ['third', 'fourth']
```

### BatchHandler

Group items into fixed-size batches (lists).  Downstream stages
receive `list` items instead of individual items.  The final batch
may be smaller if the total count is not evenly divisible.

- **Transform**: yields lists of up to *size* items.

Use the `batch()` factory or `.batch()` method:

```python
from slonk import Slonk, batch

pipeline = (
    Slonk()
    | (lambda: range(7))
    | batch(3)
)
result = list(pipeline.run())
# [[0, 1, 2], [3, 4, 5], [6]]
```

Batching is useful for grouping items before bulk operations:

```python
from slonk import Slonk, batch, flatten

pipeline = (
    Slonk()
    | "./large_input.txt"
    | batch(100)
    | (lambda batches: [bulk_api_call(b) for b in batches])
    | flatten()                  # unpack batch results
    | "./results.txt"
)
```

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
