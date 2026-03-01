# API Reference

This section documents the public API and key internal symbols of Slonk.

## Modules

| Module | Description |
|--------|-------------|
| [`slonk.pipeline`](pipeline.md) | `Slonk` class, `TeeHandler`, `tee()`, `_compute_roles()` |
| [`slonk.roles`](roles.md) | `Source`, `Transform`, `Sink` protocols, `_Role` enum |
| [`slonk.handlers`](handlers.md) | `PathHandler`, `ShellCommandHandler`, `SQLAlchemyHandler`, callable wrappers, `parallel()` |
| [`slonk.middleware`](middleware.md) | `Middleware` base class, `_EventDispatcher`, `_Event`, `_EventType` |
| [`slonk.builtin_middleware`](builtin_middleware.md) | `TimingMiddleware`, `LoggingMiddleware`, `StatsMiddleware` |
| [`slonk.base`](internals.md#slonk.base) | `SlonkBase` mixin |
| [`slonk.constants`](internals.md#slonk.constants) | Sentinels, configuration, free-threading detection |
| [`slonk.queue`](internals.md#slonk.queue) | Queue utilities for streaming execution |
| [`slonk.streaming`](internals.md#slonk.streaming) | `_StreamingPipeline` threaded executor |
