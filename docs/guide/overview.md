# Overview

Slonk is a Python library for building typed data pipelines using the
`|` (pipe) operator.  It is designed for composing heterogeneous data
processing stages -- plain functions, shell commands, file I/O,
database queries -- into a single pipeline that can run sequentially
or in parallel.

## Design principles

1. **Composition via operators** -- pipelines read like Unix pipes.
2. **Convention over configuration** -- strings, callables, and models
   are automatically wrapped in the appropriate handler.
3. **Streaming by default** -- parallel mode connects stages with bounded
   queues so data flows lazily with backpressure.
4. **Observable** -- the middleware system lets you instrument pipelines
   without touching handler code.
5. **Type-safe** -- role-based protocols (`Source`, `Transform`, `Sink`)
   enforce correctness at pipeline construction time.

## Architecture

```
Slonk()
  | stage_1    # Source, Transform, or Sink (auto-detected)
  | stage_2
  | ...
  .run()       # -> Iterable[str]
```

Under the hood:

- **`_compute_roles()`** inspects each stage's position and protocol
  conformance to assign a `_Role` (SOURCE, TRANSFORM, SINK).
- **`run_sync()`** executes stages one at a time, passing output to
  the next stage's input.
- **`run_parallel()`** (default) launches each stage in its own thread
  connected by bounded `Queue` instances via `_StreamingPipeline`.
- **Middleware** is dispatched on a dedicated background thread via
  `_EventDispatcher`.

## Data model

All data flowing between stages is `Iterable[str]`.  Stages produce and
consume strings -- if you need structured data, serialise/deserialise at
stage boundaries.
