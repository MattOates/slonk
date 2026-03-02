# Pipeline Concepts

## Stages

A pipeline is an ordered sequence of **stages**.  Each stage is a handler
object that conforms to one or more of the role protocols.

## Roles

Every stage is assigned exactly one role based on its position in the
pipeline and the protocols it implements:

| Role | Protocol | When assigned |
|------|----------|---------------|
| **Source** | `process_source() -> Iterable[T]` | First stage with no seed data |
| **Transform** | `process_transform(Iterable[T_in]) -> Iterable[T_out]` | Middle stages, or first stage with seed data |
| **Sink** | `process_sink(Iterable[T]) -> None` | Last stage that returns nothing |

The protocols are generic: `Source[T]` produces `T` items,
`Transform[T_in, T_out]` maps `T_in` to `T_out`, and `Sink[T]` consumes
`T` items.  When left unparameterised they default to `Any`.

Role assignment is performed by `_compute_roles()` at the start of each
pipeline run.

## Automatic wrapping

When you use the `|` operator, Slonk inspects the right-hand operand and
wraps it in the appropriate handler:

| Operand type | Handler |
|---|---|
| String starting with `/`, `./`, `../`, or a known URI scheme | `PathHandler` |
| Other string | `ShellCommandHandler` |
| `Slonk` instance | Nested sub-pipeline (acts as Transform) |
| SQLAlchemy `DeclarativeBase` subclass | `SQLAlchemyHandler` (Source, Transform, or Sink) |
| `_ParallelHandler` (from `parallel()`) | Used directly |
| Object implementing `Source`, `Transform`, or `Sink` | Used directly |
| Other callable | Wrapped via `_wrap_callable()` (role inferred from signature) |

## Callable role inference

When a plain callable is piped in, its role is inferred from its signature:

- **No parameters** and returns something -> Source
- **Has input parameter** and returns `None` -> Sink
- **Has input parameter** and returns something -> Transform (default)

## Sub-pipelines

A `Slonk` instance can be used as a stage in another pipeline.  It always
acts as a Transform -- it receives input, runs its internal stages, and
returns output.

## Stream combinators

Slonk provides built-in combinators for common data manipulation.  Each
is available as a factory function (used with `|`) and as a method on
`Slonk`:

### Tee (fan-out)

`tee()` forks data to a side pipeline.  The main pipeline receives the
original data plus any output from the side pipeline:

```python
from slonk import Slonk, tee

side = Slonk() | "./backup.txt"
pipeline = (
    Slonk()
    | (lambda: ["data"])
    | tee(side)
)
```

### Merge (concurrent fan-in)

`merge()` interleaves items from multiple sub-pipelines concurrently.
Items arrive in non-deterministic order:

```python
from slonk import Slonk, merge

feed_a = Slonk() | (lambda: ["a1", "a2"])
feed_b = Slonk() | (lambda: ["b1", "b2"])

pipeline = (
    Slonk()
    | (lambda: ["upstream"])
    | merge(feed_a, feed_b)
)
```

### Cat (ordered fan-in)

`cat()` concatenates sub-pipeline outputs in listed order (deterministic):

```python
from slonk import Slonk, cat

header  = Slonk() | (lambda: ["# header"])
records = Slonk() | (lambda: ["row1", "row2"])

pipeline = (
    Slonk()
    | (lambda: ["preamble"])
    | cat(header, records)
)
```

### Filter, map, flatten

```python
from slonk import Slonk, filter, map, flatten

pipeline = (
    Slonk()
    | (lambda: [["a", "b"], ["c"]])
    | flatten()                          # -> "a", "b", "c"
    | filter(lambda x: x != "b")        # -> "a", "c"
    | map(str.upper)                     # -> "A", "C"
)
```

### Head, skip, tail

```python
from slonk import Slonk, head, skip, tail

# First 5 items
Slonk() | (lambda: range(100)) | head(5)

# Skip header, take rest
Slonk() | "./data.csv" | skip(1)

# Last 10 items
Slonk() | (lambda: range(100)) | tail(10)
```

### Batch

```python
from slonk import Slonk, batch

# Group into lists of 50
Slonk() | (lambda: range(120)) | batch(50)
# yields [0..49], [50..99], [100..119]
```

## Seed data

You can provide initial data to a pipeline at run time:

```python
pipeline = Slonk() | (lambda data: [s.upper() for s in data])
result = pipeline.run(["hello", "world"])
```

When seed data is provided, the first stage must implement Transform
(or Sink if it's the only stage).  Without seed data, the first stage
must implement Source.
