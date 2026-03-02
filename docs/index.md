# Slonk

Typed data pipelines with operator-overloaded `|` syntax.

---

Slonk lets you build data pipelines by chaining stages with Python's `|`
operator. Stages can be plain callables, shell commands, file paths
(local and cloud), SQLAlchemy models, or custom handler objects -- and
they run concurrently with automatic backpressure.

## Key Features

- **Pipe operator composition** -- chain stages naturally with `|`
- **Automatic handler inference** -- strings, callables, and models are
  wrapped automatically
- **Stream combinators** -- `filter`, `map`, `flatten`, `head`, `skip`,
  `tail`, `batch` for expressive data manipulation
- **Fan-out / fan-in** -- `tee` forks data to side pipelines, `merge`
  interleaves sources concurrently, `cat` concatenates in order
- **Shell commands** -- pipe data through `grep`, `sort`, `jq`, or any
  Unix tool
- **Cloud and local file I/O** -- read/write S3, GCS, HDFS, and local
  paths via [universal-pathlib](https://github.com/fsspec/universal_pathlib)
- **SQLAlchemy integration** -- use ORM models directly as pipeline
  sources, transforms, or sinks
- **Parallel execution** -- stages run concurrently in threads with
  bounded-queue backpressure
- **Data-parallel processing** -- `parallel()` distributes work across
  thread or process pools
- **Free-threaded Python support** -- detected at runtime for true
  thread parallelism
- **Role-based protocols** -- `Source`, `Transform`, `Sink` with generic
  type parameters
- **Middleware system** -- observe pipeline lifecycle events without
  touching handler code

## Example: ETL with datalake archival

A realistic pipeline that reads events from a database, filters and
transforms them, tees a copy to an S3 datalake for archival, deduplicates
via a shell command, and writes the results to a local file:

```python
from slonk import Slonk, tee, filter, map, head
from sqlalchemy import create_engine, Column, String
from sqlalchemy.orm import DeclarativeBase, sessionmaker

# --- Models -----------------------------------------------------------------
class Base(DeclarativeBase):
    pass

class RawEvent(Base):
    __tablename__ = "raw_events"
    id = Column(String, primary_key=True)
    data = Column(String)

engine = create_engine("sqlite:///warehouse.db")
Session = sessionmaker(bind=engine)

# --- Side pipeline: archive raw events to S3 datalake -----------------------
datalake = Slonk() | "s3://my-datalake/events/raw_archive.csv"

# --- Main pipeline ----------------------------------------------------------
pipeline = (
    Slonk(session_factory=Session)
    | RawEvent                                  # Source: stream rows from DB
    | filter(lambda row: "ERROR" in row)        # Keep only error events
    | map(lambda row: row.strip().lower())      # Normalise text
    | tee(datalake)                             # Archive a copy to S3
    | "sort -u"                                 # Deduplicate via shell
    | head(1000)                                # Cap output at 1000 rows
    | "./error_report.txt"                      # Sink: write to local file
)
pipeline.run()
```

Every stage runs in its own thread with bounded queues providing
backpressure -- the database is not read faster than downstream stages
can consume.

## More Examples

Slonk pipelines compose naturally.  Here are a few patterns to give you
a feel for the API:

### Filter and batch

```python
from slonk import Slonk, filter, batch

pipeline = (
    Slonk()
    | "./large_dataset.csv"
    | filter(lambda line: not line.startswith("#"))   # skip comments
    | batch(500)                                      # group into chunks
    | (lambda chunks: [process_batch(c) for c in chunks])
)
```

### Fan-in from multiple sources

```python
from slonk import Slonk, merge

api_feed = Slonk() | (lambda: fetch_api_events())
log_feed = Slonk() | "/var/log/app.log" | "grep CRITICAL"

pipeline = (
    Slonk()
    | (lambda: ["local_event"])
    | merge(api_feed, log_feed)     # interleave all sources concurrently
    | "sort"
    | "./all_events.txt"
)
```

### Shell command as a source

```python
from slonk import Slonk, tail

pipeline = (
    Slonk()
    | "find /var/log -name '*.log' -mtime -1"   # Source: shell command
    | tail(10)                                    # Keep last 10 results
)
```

### Pipeline visualisation

Slonk pipelines have a human-readable `repr`:

```python
>>> pipeline = Slonk() | "./input.csv" | "grep ERROR" | "sort" | "./output.txt"
>>> pipeline
Slonk(./input.csv | grep ERROR | sort | ./output.txt)
```

## Next Steps

- [Installation](getting-started/installation.md) -- get up and running
- [Quick Start](getting-started/quickstart.md) -- build your first pipeline
- [User Guide](guide/overview.md) -- architecture, concepts, and handlers
- [Recipes](guide/recipes.md) -- real-world data engineering patterns
- [API Reference](reference/index.md) -- full public API documentation
