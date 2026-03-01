# Recipes

Real-world data engineering patterns using Slonk.

## Database ETL: extract, transform, load

Read rows from a database, transform them, and write the results back:

```python
from sqlalchemy import create_engine, Column, String
from sqlalchemy.orm import DeclarativeBase, sessionmaker
from slonk import Slonk

class Base(DeclarativeBase):
    pass

class RawEvent(Base):
    __tablename__ = "raw_events"
    id = Column(String, primary_key=True)
    data = Column(String)

class CleanEvent(Base):
    __tablename__ = "clean_events"
    id = Column(String, primary_key=True)
    data = Column(String)

engine = create_engine("sqlite:///warehouse.db")
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)

def normalise(rows):
    for row in rows:
        record_id, data = row.split("\t", 1)
        yield f"{record_id}\t{data.strip().lower()}"

pipeline = (
    Slonk(session_factory=Session)
    | RawEvent        # Source: read all raw events
    | normalise       # Transform: clean the data
    | CleanEvent      # Sink: bulk-write into clean_events
)
pipeline.run()
```

## Cloud file processing with UPath

Read from S3, transform, and write to GCS (or any
[fsspec](https://filesystem-spec.readthedocs.io/en/latest/) backend):

```python
from slonk import Slonk

pipeline = (
    Slonk()
    | "s3://input-bucket/events.csv"      # Source: read lines
    | (lambda rows: [r for r in rows if "ERROR" in r])
    | "gs://output-bucket/errors.csv"     # Sink: write filtered rows
)
pipeline.run()
```

Install the appropriate fsspec backend (e.g. `s3fs` for S3, `gcsfs`
for GCS) alongside `universal-pathlib`.

## Log file filtering with shell commands

Combine file I/O with Unix tools for log processing:

```python
from slonk import Slonk

pipeline = (
    Slonk()
    | "/var/log/app.log"        # Source: read log file
    | "grep ERROR"              # Transform: keep error lines
    | "sort"                    # Transform: sort chronologically
    | "./filtered_errors.txt"   # Sink: write results
)
pipeline.run()
```

## Tee: backup while processing

Fork data to a side pipeline for backup, then continue processing:

```python
from slonk import Slonk, tee

backup = Slonk() | "./backup.csv"

pipeline = (
    Slonk()
    | "s3://bucket/data.csv"
    | tee(backup)                # writes to backup.csv AND passes through
    | (lambda data: [row.upper() for row in data])
    | "./processed.csv"
)
pipeline.run()
```

## Merge: combine multiple sources concurrently

Pull data from several independent sources and merge them into a single
stream.  Items arrive as soon as any source produces them
(non-deterministic order):

```python
from slonk import Slonk, merge

api_feed  = Slonk() | (lambda: ["api_event_1", "api_event_2"])
log_feed  = Slonk() | "/var/log/app.log" | "grep ERROR"
db_feed   = Slonk() | (lambda: ["db_row_1"])

pipeline = (
    Slonk()
    | (lambda: ["local_item"])
    | merge(api_feed, log_feed, db_feed)
    | "sort"
    | "./all_events.txt"
)
pipeline.run()
```

Each sub-pipeline runs in its own thread for maximum throughput.  The
shared queue provides automatic backpressure if any producer is faster
than the consumer.

## Cat: concatenate sources in order

When deterministic ordering matters, use `cat()` to yield all upstream
items first, followed by each sub-pipeline in the order listed:

```python
from slonk import Slonk, cat

header  = Slonk() | (lambda: ["id\tname"])
records = Slonk() | (lambda: ["1\tAlice", "2\tBob"])
footer  = Slonk() | (lambda: ["# end of report"])

pipeline = (
    Slonk()
    | (lambda: ["# generated report"])
    | cat(header, records, footer)
    | "./report.tsv"
)
result = list(pipeline.run())
# ['# generated report', 'id\tname', '1\tAlice', '2\tBob', '# end of report']
```

## Merge + Tee: fan-in with side effects

Combine fan-in and fan-out for complex data flows — merge several feeds
then tee a copy to an audit log:

```python
from slonk import Slonk, merge, tee

feed_a = Slonk() | (lambda: ["event_a1", "event_a2"])
feed_b = Slonk() | (lambda: ["event_b1"])
audit  = Slonk() | "./audit.log"

pipeline = (
    Slonk()
    | (lambda: ["primary"])
    | merge(feed_a, feed_b)
    | tee(audit)
    | (lambda data: [e.upper() for e in data])
    | "./processed.txt"
)
pipeline.run()
```

## Data-parallel processing

Distribute CPU-intensive work across threads (free-threaded Python) or
processes:

```python
from slonk import Slonk, parallel
import json

def parse_json_batch(rows):
    results = []
    for row in rows:
        obj = json.loads(row)
        results.append(f"{obj['id']}\t{obj['value']}")
    return results

pipeline = (
    Slonk()
    | "./large_jsonl.txt"
    | parallel(parse_json_batch, workers=8, chunk_size=1000)
    | "./parsed_output.tsv"
)
pipeline.run()
```

## Database to file export

Export database contents to a local or remote file:

```python
pipeline = (
    Slonk(session_factory=Session)
    | MyModel                        # Source: read all rows
    | "./export.tsv"                 # Sink: write to file
)
pipeline.run()
```

## File to database import

Load a file into a database table:

```python
pipeline = (
    Slonk(session_factory=Session)
    | "./import.tsv"                 # Source: read lines
    | MyModel                        # Sink: bulk-write to DB
)
pipeline.run()
```

## Database passthrough with middleware

Upsert into a staging table while monitoring performance:

```python
from slonk import Slonk, TimingMiddleware

tm = TimingMiddleware()

pipeline = Slonk(session_factory=Session)
pipeline.add_middleware(tm)

pipeline |= RawEvent          # Source: read raw events
pipeline |= StagingEvent      # Transform: upsert into staging, pass through
pipeline |= "grep CRITICAL"   # Transform: filter critical events
pipeline |= "./alerts.txt"    # Sink: write alerts to file

pipeline.run()

for i, duration in enumerate(tm.stage_durations):
    print(f"Stage {i}: {duration:.3f}s")
```

## Multi-stage database pipeline

Chain multiple models for multi-hop ETL:

```python
def enrich(rows):
    for row in rows:
        record_id, data = row.split("\t", 1)
        yield f"{record_id}\t{data} [enriched]"

pipeline = (
    Slonk(session_factory=Session)
    | RawEvent          # Source: read raw
    | StagingEvent      # Transform: upsert into staging
    | enrich            # Transform: enrich data
    | FinalEvent        # Sink: bulk-write to final table
)
pipeline.run()
```

## Custom streaming handler

Build a handler that reads from an API and yields items lazily:

```python
from slonk import SlonkBase
import urllib.request
import json

class APISource(SlonkBase):
    def __init__(self, url):
        self.url = url

    def process_source(self):
        with urllib.request.urlopen(self.url) as resp:
            for line in resp:
                obj = json.loads(line)
                self.emit("api_row", {"id": obj["id"]})
                yield f"{obj['id']}\t{obj['name']}"

pipeline = (
    Slonk(session_factory=Session)
    | APISource("https://api.example.com/users.jsonl")
    | UserModel   # Sink: bulk-write API data into the DB
)
pipeline.run()
```
