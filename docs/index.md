# Slonk

Typed data pipelines with operator-overloaded `|` syntax.

---

Slonk lets you build data pipelines by chaining stages with Python's `|`
operator. Stages can be plain callables, shell commands, file paths,
SQLAlchemy models, or custom handler objects.

## Key Features

- **Pipe operator composition** -- chain stages naturally with `|`
- **Automatic handler inference** -- strings, callables, and models are
  wrapped automatically
- **Parallel execution** -- stages run concurrently with backpressure
- **Free-threaded Python support** -- detected at runtime
- **Role-based protocols** -- `Source`, `Transform`, `Sink`
- **Middleware system** -- observe pipeline lifecycle events
- **Data-parallel processing** -- `parallel()` distributes work across pools

## Quick Example

```python
from slonk import Slonk

result = (
    Slonk()
    | (lambda: ["hello", "world"])
    | (lambda data: [s.upper() for s in data])
).run()

print(list(result))  # ['HELLO', 'WORLD']
```

## Next Steps

- [Installation](getting-started/installation.md)
- [Quick Start](getting-started/quickstart.md)
- [User Guide](guide/overview.md)
- [API Reference](reference/index.md)
