# Middleware

The middleware system lets you observe pipeline lifecycle events without
modifying handler code.

## How it works

Middleware callbacks run on a single dedicated background thread via
`_EventDispatcher`.  This means:

- Middleware implementations do **not** need to be thread-safe.
- Middleware errors are swallowed silently -- they never break the pipeline.
- Events are delivered in order, but asynchronously relative to stage
  execution.

## Writing middleware

Subclass `Middleware` and override the hooks you care about:

```python
from slonk import Middleware

class MyMiddleware(Middleware):
    def on_pipeline_start(self, stages, roles):
        print(f"Starting pipeline with {len(stages)} stages")

    def on_stage_end(self, stage, role, index, duration):
        print(f"Stage {index} completed in {duration:.4f}s")

    def on_pipeline_end(self, stages, roles, duration):
        print(f"Pipeline completed in {duration:.4f}s")
```

## Available hooks

| Hook | When called |
|------|-------------|
| `on_pipeline_start(stages, roles)` | Before any stage executes |
| `on_pipeline_end(stages, roles, duration)` | After all stages complete |
| `on_stage_start(stage, role, index)` | When a stage begins |
| `on_stage_end(stage, role, index, duration)` | When a stage completes |
| `on_stage_error(stage, role, index, error)` | When a stage raises |
| `on_event(stage, role, index, event, data)` | For custom events |

## Registering middleware

### Persistent (runs on every execution)

```python
from slonk import Slonk, TimingMiddleware

pipeline = Slonk()
tm = TimingMiddleware()
pipeline.add_middleware(tm)

# tm receives events on every pipeline.run() call
```

### Per-run

```python
from slonk import StatsMiddleware

sm = StatsMiddleware()
pipeline.run(middleware=[sm])

# sm only receives events for this single run
```

Both persistent and per-run middleware are merged for each execution.

## Custom events

Handlers that extend `SlonkBase` can emit custom events:

```python
from slonk import SlonkBase

class MyHandler(SlonkBase):
    def process_transform(self, input_data):
        for item in input_data:
            self.emit("row_processed", {"value": item})
            yield item
```

These arrive at `on_event()` in registered middleware.

## Built-in middleware

Slonk ships with three middleware classes:

- **`TimingMiddleware`** -- records `stage_durations` and `pipeline_duration`
- **`LoggingMiddleware`** -- logs events via the standard `logging` module
- **`StatsMiddleware`** -- counts events per stage

See [Built-in Middleware Reference](../reference/builtin_middleware.md)
for full API details.
