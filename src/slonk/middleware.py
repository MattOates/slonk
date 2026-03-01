from __future__ import annotations

import dataclasses
import enum
import threading
import time
from queue import Queue
from typing import Any

from slonk.roles import StageType, _Role


class _EventType(enum.Enum):
    """Internal event types dispatched through the middleware queue."""

    PIPELINE_START = "pipeline_start"
    PIPELINE_END = "pipeline_end"
    STAGE_START = "stage_start"
    STAGE_END = "stage_end"
    STAGE_ERROR = "stage_error"
    CUSTOM = "custom"


@dataclasses.dataclass(slots=True)
class _Event:
    """Internal event envelope pushed through the middleware queue."""

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
    """

    def on_pipeline_start(self, stages: list[StageType], roles: list[_Role]) -> None:
        """Called once before any stage executes."""

    def on_pipeline_end(self, stages: list[StageType], roles: list[_Role], duration: float) -> None:
        """Called once after all stages complete (including on error)."""

    def on_stage_start(self, stage: StageType, role: _Role, index: int) -> None:
        """Called when a stage begins execution."""

    def on_stage_end(self, stage: StageType, role: _Role, index: int, duration: float) -> None:
        """Called when a stage completes successfully."""

    def on_stage_error(
        self, stage: StageType, role: _Role, index: int, error: BaseException
    ) -> None:
        """Called when a stage raises an exception."""

    def on_event(
        self,
        stage: StageType,
        role: _Role,
        index: int,
        event: str,
        data: dict[str, Any] | None,
    ) -> None:
        """Called for custom events emitted by ``SlonkBase.emit()``."""


_DISPATCHER_QUEUE_SIZE = 4096


class _EventDispatcher:
    """Drains an event queue on a dedicated thread and fans out to middleware.

    Start with ``start()``, stop (and drain) with ``stop()``.
    """

    def __init__(self, middleware: list[Middleware]) -> None:
        self.middleware = middleware
        self.queue: Queue[_Event | object] = Queue(maxsize=_DISPATCHER_QUEUE_SIZE)
        self._thread: threading.Thread | None = None

    def start(self) -> None:
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
