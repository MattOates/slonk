# Pipeline Concepts

## Stages

A pipeline is an ordered sequence of **stages**.  Each stage is a handler
object that conforms to one or more of the role protocols.

## Roles

Every stage is assigned exactly one role based on its position in the
pipeline and the protocols it implements:

| Role | Protocol | When assigned |
|------|----------|---------------|
| **Source** | `process_source() -> Iterable[str]` | First stage with no seed data |
| **Transform** | `process_transform(Iterable[str]) -> Iterable[str]` | Middle stages, or first stage with seed data |
| **Sink** | `process_sink(Iterable[str]) -> None` | Last stage that returns nothing |

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

## Tee / Fork

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

## Seed data

You can provide initial data to a pipeline at run time:

```python
pipeline = Slonk() | (lambda data: [s.upper() for s in data])
result = pipeline.run(["hello", "world"])
```

When seed data is provided, the first stage must implement Transform
(or Sink if it's the only stage).  Without seed data, the first stage
must implement Source.
