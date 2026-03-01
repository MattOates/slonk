# Parallel Execution

By default, Slonk runs pipelines in parallel mode.  This page explains
how it works and when to use sequential mode instead.

## Streaming architecture

In parallel mode, each stage runs in its own thread.  Stages are
connected by bounded `queue.Queue` instances:

```
[Source thread] -> Queue -> [Transform thread] -> Queue -> [Sink thread]
```

- **Backpressure** is automatic: a fast producer blocks when its output
  queue is full.
- **Lazy streaming**: generators yield items one at a time through the
  queues, so memory usage stays bounded.
- The sentinel `_DONE` signals end-of-stream.

## Configuration

```python
result = pipeline.run(
    parallel=True,           # default
    max_queue_size=1024,     # queue capacity (default)
)
```

Reduce `max_queue_size` to limit memory usage for large pipelines, or
increase it to reduce contention for bursty workloads.

## Sequential mode

Use `parallel=False` when:

- Debugging pipeline logic (deterministic execution).
- Stages have side effects that must not overlap.
- Running in environments where threading is problematic.

```python
result = pipeline.run(parallel=False)
```

In sequential mode each stage runs to completion before the next starts.
Generators are **not** lazily streamed -- the pipeline materialises output
between stages.

## Data parallelism with `parallel()`

For CPU-bound transforms, `parallel()` distributes work across a pool:

```python
from slonk import Slonk, parallel

def heavy_transform(chunk):
    return [process(item) for item in chunk]

pipeline = (
    Slonk()
    | (lambda: generate_items())
    | parallel(heavy_transform, workers=8, chunk_size=100)
)
```

**On free-threaded Python** (no GIL): uses `ThreadPoolExecutor` -- zero
serialisation overhead.

**On standard Python**: uses `ProcessPoolExecutor` with `cloudpickle` for
serialising the callable across process boundaries.

Free-threaded Python is detected at runtime via `_is_free_threaded()`.

## Sub-pipelines in parallel mode

When a `Slonk` sub-pipeline appears as a stage, it materialises its
input and runs synchronously within its thread.  This prevents nested
threading complexity while still participating in the outer pipeline's
concurrent execution.

## Error handling

If a stage thread raises an exception:

1. Its input queue is drained (to unblock upstream stages).
2. The `_DONE` sentinel is pushed to its output queue (to unblock
   downstream stages).
3. The error is collected and re-raised after all threads join.
4. The first error encountered is the one that propagates.
