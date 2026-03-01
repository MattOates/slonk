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

Queries all rows from a SQLAlchemy model.

- **Source**: fetches rows using `yield_per()` for chunked streaming.

```python
from sqlalchemy.orm import sessionmaker
from slonk import Slonk

pipeline = Slonk(session_factory=Session) | MyModel
```

Requires passing a `session_factory` to `Slonk()`.

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
