"""Middleware system for observing pipeline lifecycle events.

The middleware layer provides a non-invasive hook system that lets
observers react to pipeline and stage lifecycle events (start, end,
error) as well as custom events emitted by handlers.  All callbacks run
on a single dedicated dispatcher thread, so middleware implementations
do **not** need to be thread-safe.

Key types:

* :class:`Middleware` — base class; subclass and override hooks.
* :class:`_EventDispatcher` — drains the event queue on a background thread.
* :class:`_Event` — event envelope pushed through the queue.
* :class:`_EventType` — enum of built-in event kinds.

Examples:
    >>> from slonk.middleware import Middleware
    >>> class MyMiddleware(Middleware):
    ...     def on_pipeline_start(self, stages, roles):
    ...         print(f"Starting with {len(stages)} stages")
    >>> mw = MyMiddleware()
    >>> mw.on_pipeline_start([], [])
    Starting with 0 stages
"""

from __future__ import annotations

import dataclasses
import enum
import threading
import time
from queue import Queue
from typing import Any

from slonk.roles import StageType, _Role


class _EventType(enum.Enum):
    """Internal event types dispatched through the middleware queue.

    Examples:
        >>> from slonk.middleware import _EventType
        >>> _EventType.PIPELINE_START.value
        'pipeline_start'
        >>> _EventType.CUSTOM.value
        'custom'
    """

    PIPELINE_START = "pipeline_start"
    PIPELINE_END = "pipeline_end"
    STAGE_START = "stage_start"
    STAGE_END = "stage_end"
    STAGE_ERROR = "stage_error"
    CUSTOM = "custom"


@dataclasses.dataclass(slots=True)
class _Event:
    """Internal event envelope pushed through the middleware queue.

    Attributes:
        type: The kind of event.
        stage: The handler instance that triggered this event, or ``None``
            for pipeline-level events.
        role: The role of the stage, or ``None`` for pipeline-level events.
        index: The zero-based index of the stage in the pipeline.
        timestamp: Monotonic timestamp when the event was created.
        duration: Elapsed seconds (set on ``STAGE_END`` / ``PIPELINE_END``).
        error: The exception (set on ``STAGE_ERROR``).
        event_name: Name string for ``CUSTOM`` events.
        event_data: Arbitrary payload dict for ``CUSTOM`` events.
        stages: Full stage list (set on pipeline-level events).
        roles: Full role list (set on pipeline-level events).
    """

    type: _EventType
    stage: StageType | None = None
    role: _Role | None = None
    index: int = -1
    timestamp: float = dataclasses.field(default_factory=time.monotonic)
    duration: float | None = None
    error: BaseException | None = None
    event_name: str | None = None
    event_data: dict[str, Any] | None = None
    # For pipeline-level events that carry stage lists.
    stages: list[StageType] | None = None
    roles: list[_Role] | None = None


# Sentinel for stopping the dispatcher thread.
_STOP = object()


class Middleware:
    """Base class for pipeline middleware/observers.

    Subclasses override only the hooks they care about — all default
    implementations are no-ops.  All callbacks run on a single dedicated
    dispatcher thread, so implementations do **not** need to be thread-safe.

    Examples:
        >>> from slonk.middleware import Middleware
        >>> class Counter(Middleware):
        ...     def __init__(self):
        ...         self.n = 0
        ...     def on_stage_start(self, stage, role, index):
        ...         self.n += 1
        >>> c = Counter()
        >>> c.on_stage_start(None, None, 0)
        >>> c.n
        1
    """

    def on_pipeline_start(self, stages: list[StageType], roles: list[_Role]) -> None:
        """Called once before any stage executes.

        Args:
            stages: The ordered list of handler instances.
            roles: The corresponding role for each stage.
        """

    def on_pipeline_end(self, stages: list[StageType], roles: list[_Role], duration: float) -> None:
        """Called once after all stages complete (including on error).

        Args:
            stages: The ordered list of handler instances.
            roles: The corresponding role for each stage.
            duration: Wall-clock seconds the pipeline took.
        """

    def on_stage_start(self, stage: StageType, role: _Role, index: int) -> None:
        """Called when a stage begins execution.

        Args:
            stage: The handler instance.
            role: Its assigned role.
            index: Zero-based position in the pipeline.
        """

    def on_stage_end(self, stage: StageType, role: _Role, index: int, duration: float) -> None:
        """Called when a stage completes successfully.

        Args:
            stage: The handler instance.
            role: Its assigned role.
            index: Zero-based position in the pipeline.
            duration: Wall-clock seconds the stage took.
        """

    def on_stage_error(
        self, stage: StageType, role: _Role, index: int, error: BaseException
    ) -> None:
        """Called when a stage raises an exception.

        Args:
            stage: The handler instance.
            role: Its assigned role.
            index: Zero-based position in the pipeline.
            error: The exception that was raised.
        """

    def on_event(
        self,
        stage: StageType,
        role: _Role,
        index: int,
        event: str,
        data: dict[str, Any] | None,
    ) -> None:
        """Called for custom events emitted by :meth:`SlonkBase.emit`.

        Args:
            stage: The handler instance.
            role: Its assigned role.
            index: Zero-based position in the pipeline.
            event: The event name string.
            data: Optional metadata dictionary.
        """


_DISPATCHER_QUEUE_SIZE = 4096


class _EventDispatcher:
    """Drains an event queue on a dedicated thread and fans out to middleware.

    Start with :meth:`start`, stop (and drain) with :meth:`stop`.

    Args:
        middleware: List of :class:`Middleware` instances to dispatch to.
    """

    def __init__(self, middleware: list[Middleware]) -> None:
        self.middleware = middleware
        self.queue: Queue[_Event | object] = Queue(maxsize=_DISPATCHER_QUEUE_SIZE)
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        """Spawn the background dispatcher thread."""
        self._thread = threading.Thread(target=self._dispatch_loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Push stop sentinel and block until the queue is fully drained."""
        self.queue.put(_STOP)
        if self._thread is not None:
            self._thread.join()
            self._thread = None

    def _dispatch_loop(self) -> None:
        """Drain events until ``_STOP`` sentinel is received."""
        while True:
            event = self.queue.get()
            if event is _STOP:
                return
            assert isinstance(event, _Event)
            self._dispatch(event)

    def _dispatch(self, event: _Event) -> None:
        """Fan out a single event to all registered middleware."""
        for mw in self.middleware:
            try:
                match event.type:
                    case _EventType.PIPELINE_START:
                        assert event.stages is not None
                        assert event.roles is not None
                        mw.on_pipeline_start(event.stages, event.roles)
                    case _EventType.PIPELINE_END:
                        assert event.stages is not None
                        assert event.roles is not None
                        assert event.duration is not None
                        mw.on_pipeline_end(event.stages, event.roles, event.duration)
                    case _EventType.STAGE_START:
                        assert event.stage is not None
                        assert event.role is not None
                        mw.on_stage_start(event.stage, event.role, event.index)
                    case _EventType.STAGE_END:
                        assert event.stage is not None
                        assert event.role is not None
                        assert event.duration is not None
                        mw.on_stage_end(event.stage, event.role, event.index, event.duration)
                    case _EventType.STAGE_ERROR:
                        assert event.stage is not None
                        assert event.role is not None
                        assert event.error is not None
                        mw.on_stage_error(event.stage, event.role, event.index, event.error)
                    case _EventType.CUSTOM:
                        assert event.stage is not None
                        assert event.role is not None
                        assert event.event_name is not None
                        mw.on_event(
                            event.stage,
                            event.role,
                            event.index,
                            event.event_name,
                            event.event_data,
                        )
            except Exception:
                # Middleware errors must not break the pipeline.
                pass
