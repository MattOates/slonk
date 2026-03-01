"""Slonk — typed data pipelines with operator-overloaded ``|`` syntax.

Slonk lets you build data pipelines by chaining stages with the ``|``
operator.  Stages can be plain callables, shell commands (strings),
file paths, SQLAlchemy models, or custom handler objects that implement
the :class:`Source`, :class:`Transform`, or :class:`Sink` protocols.

Quick start::

    from slonk import Slonk

    result = (
        Slonk()
        | (lambda: ["hello", "world"])
        | (lambda data: [s.upper() for s in data])
    ).run(parallel=False)

See the :class:`Slonk` class for full API documentation.
"""

from __future__ import annotations

# Public API re-exports.
from slonk.base import SlonkBase
from slonk.builtin_middleware import LoggingMiddleware, StatsMiddleware, TimingMiddleware
from slonk.constants import _is_free_threaded
from slonk.handlers import (
    PathHandler,
    ShellCommandHandler,
    SQLAlchemyHandler,
    _CallableSink,
    _CallableSource,
    _CallableTransform,
    _ParallelHandler,
    parallel,
)
from slonk.middleware import Middleware, _Event, _EventDispatcher, _EventType
from slonk.pipeline import (
    CatHandler,
    MergeHandler,
    Slonk,
    TeeHandler,
    _compute_roles,
    cat,
    merge,
    tee,
)
from slonk.roles import Sink, Source, StageType, Transform, _Role
from slonk.streaming import _StreamingPipeline

__all__ = [
    "CatHandler",
    "LoggingMiddleware",
    "MergeHandler",
    "Middleware",
    "PathHandler",
    "SQLAlchemyHandler",
    "ShellCommandHandler",
    "Sink",
    "Slonk",
    "SlonkBase",
    "Source",
    "StageType",
    "StatsMiddleware",
    "TeeHandler",
    "TimingMiddleware",
    "Transform",
    "_CallableSink",
    "_CallableSource",
    "_CallableTransform",
    "_Event",
    "_EventDispatcher",
    "_EventType",
    "_ParallelHandler",
    "_Role",
    "_StreamingPipeline",
    "_compute_roles",
    "_is_free_threaded",
    "cat",
    "merge",
    "parallel",
    "tee",
]
