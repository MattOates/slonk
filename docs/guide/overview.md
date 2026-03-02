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
5. **Type-safe** -- role-based protocols (`Source[T]`, `Transform[T_in, T_out]`,
   `Sink[T]`) with generic type parameters enforce correctness at pipeline
   construction time.

## Architecture

```
Slonk()
  | stage_1    # Source, Transform, or Sink (auto-detected)
  | stage_2
  | ...
  .run()       # -> Iterable[Any]
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

Data flowing between stages is `Iterable[Any]`.  The built-in file and
database handlers use strings as their interchange format, but you are
free to pass any type through the pipeline.  The generic type parameters
on `Source[T]`, `Transform[T_in, T_out]`, and `Sink[T]` let you express
the types your stages produce and consume.

For structured data, parse at stage boundaries -- for example use
`map(json.loads)` to convert JSON strings into dicts.

## Pipeline visualisation

Pipelines have a human-readable `__repr__` that shows the stage chain:

```python
>>> from slonk import Slonk
>>> pipeline = Slonk() | "./input.csv" | "grep ERROR" | "sort" | "./output.txt"
>>> pipeline
Slonk(./input.csv | grep ERROR | sort | ./output.txt)
```

Callable stages show the function name (or `lambda` for anonymous
functions), and nested sub-pipelines show their own repr recursively.

## Stream combinators

In addition to source, transform, and sink stages, Slonk provides
built-in combinators for common data manipulation patterns:

| Combinator | Description |
|---|---|
| `filter(predicate)` | Keep items where predicate is truthy |
| `map(func)` | Transform each item individually |
| `flatten()` | Unpack one level of nested iterables |
| `head(n)` | Yield only the first *n* items |
| `skip(n)` | Discard the first *n* items |
| `tail(n)` | Yield only the last *n* items |
| `batch(size)` | Group items into fixed-size lists |
| `tee(pipeline)` | Fork data to a side pipeline |
| `merge(*pipelines)` | Fan-in: interleave from multiple sources |
| `cat(*pipelines)` | Fan-in: concatenate in listed order |

Each combinator is available as both a standalone factory function
(e.g. `filter(pred)`) and a method on `Slonk` (e.g. `.filter(pred)`).
See [Handlers](handlers.md) for full details.
