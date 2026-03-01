"""Built-in middleware implementations for common observability needs.

Slonk ships with three middleware classes out of the box:

* :class:`TimingMiddleware` — records stage and pipeline durations.
* :class:`LoggingMiddleware` — logs lifecycle events via :mod:`logging`.
* :class:`StatsMiddleware` — counts events per stage.

Examples:
    >>> from slonk.builtin_middleware import TimingMiddleware
    >>> tm = TimingMiddleware()
    >>> tm.pipeline_duration
    0.0
"""

from __future__ import annotations

import logging
from typing import Any

from slonk.middleware import Middleware
from slonk.roles import StageType, _Role


class TimingMiddleware(Middleware):
    """Records stage and pipeline durations.

    After the pipeline completes, inspect :attr:`stage_durations` and
    :attr:`pipeline_duration` to retrieve timing data.

    Attributes:
        stage_durations: Maps stage index to elapsed seconds.
        pipeline_duration: Total pipeline wall-clock seconds.

    Examples:
        >>> from slonk.builtin_middleware import TimingMiddleware
        >>> tm = TimingMiddleware()
        >>> tm.stage_durations
        {}
        >>> tm.pipeline_duration
        0.0
    """

    def __init__(self) -> None:
        self.stage_durations: dict[int, float] = {}
        self.pipeline_duration: float = 0.0

    def on_stage_end(self, stage: StageType, role: _Role, index: int, duration: float) -> None:
        """Record the duration of a completed stage.

        Args:
            stage: The handler instance.
            role: Its assigned role.
            index: Zero-based position in the pipeline.
            duration: Wall-clock seconds the stage took.
        """
        self.stage_durations[index] = duration

    def on_pipeline_end(self, stages: list[StageType], roles: list[_Role], duration: float) -> None:
        """Record the total pipeline duration.

        Args:
            stages: The ordered list of handler instances.
            roles: The corresponding role for each stage.
            duration: Wall-clock seconds the pipeline took.
        """
        self.pipeline_duration = duration


class LoggingMiddleware(Middleware):
    """Logs pipeline lifecycle events via the standard :mod:`logging` module.

    Stage start/end at INFO, errors at ERROR, custom events at DEBUG.

    Args:
        logger_name: The logger name to use (default ``"slonk"``).

    Examples:
        >>> from slonk.builtin_middleware import LoggingMiddleware
        >>> lm = LoggingMiddleware("my_app")
        >>> lm._logger.name
        'my_app'
    """

    def __init__(self, logger_name: str = "slonk") -> None:
        self._logger = logging.getLogger(logger_name)

    def on_pipeline_start(self, stages: list[StageType], roles: list[_Role]) -> None:
        """Log pipeline start at INFO level."""
        self._logger.info("Pipeline starting with %d stages", len(stages))

    def on_pipeline_end(self, stages: list[StageType], roles: list[_Role], duration: float) -> None:
        """Log pipeline completion at INFO level."""
        self._logger.info("Pipeline completed in %.4fs", duration)

    def on_stage_start(self, stage: StageType, role: _Role, index: int) -> None:
        """Log stage start at INFO level."""
        self._logger.info(
            "Stage %d (%s, role=%s) starting", index, type(stage).__name__, role.value
        )

    def on_stage_end(self, stage: StageType, role: _Role, index: int, duration: float) -> None:
        """Log stage completion at INFO level."""
        self._logger.info(
            "Stage %d (%s, role=%s) completed in %.4fs",
            index,
            type(stage).__name__,
            role.value,
            duration,
        )

    def on_stage_error(
        self, stage: StageType, role: _Role, index: int, error: BaseException
    ) -> None:
        """Log stage error at ERROR level."""
        self._logger.error(
            "Stage %d (%s, role=%s) failed: %s",
            index,
            type(stage).__name__,
            role.value,
            error,
        )

    def on_event(
        self,
        stage: StageType,
        role: _Role,
        index: int,
        event: str,
        data: dict[str, Any] | None,
    ) -> None:
        """Log custom event at DEBUG level."""
        self._logger.debug("Stage %d custom event %r: %s", index, event, data)


class StatsMiddleware(Middleware):
    """Collects event counts and metadata per stage.

    After the pipeline completes, inspect :attr:`stats` — a mapping of
    stage index to counter dict.  Each counter dict has keys like
    ``"start"``, ``"end"``, ``"error"``, and any custom event names.

    Attributes:
        stats: Maps stage index to ``{event_name: count}``.
        stage_metadata: Maps stage index to ``{"type": ..., "role": ...}``.

    Examples:
        >>> from slonk.builtin_middleware import StatsMiddleware
        >>> sm = StatsMiddleware()
        >>> sm.stats
        {}
    """

    def __init__(self) -> None:
        self.stats: dict[int, dict[str, int]] = {}
        self.stage_metadata: dict[int, dict[str, str]] = {}

    def _ensure(self, index: int) -> dict[str, int]:
        """Return the counter dict for *index*, creating it if needed.

        Args:
            index: The stage index.

        Returns:
            The counter dictionary.
        """
        if index not in self.stats:
            self.stats[index] = {}
        return self.stats[index]

    def on_stage_start(self, stage: StageType, role: _Role, index: int) -> None:
        """Increment the ``"start"`` counter and record stage metadata."""
        counters = self._ensure(index)
        counters["start"] = counters.get("start", 0) + 1
        self.stage_metadata[index] = {
            "type": type(stage).__name__,
            "role": role.value,
        }

    def on_stage_end(self, stage: StageType, role: _Role, index: int, duration: float) -> None:
        """Increment the ``"end"`` counter."""
        counters = self._ensure(index)
        counters["end"] = counters.get("end", 0) + 1

    def on_stage_error(
        self, stage: StageType, role: _Role, index: int, error: BaseException
    ) -> None:
        """Increment the ``"error"`` counter."""
        counters = self._ensure(index)
        counters["error"] = counters.get("error", 0) + 1

    def on_event(
        self,
        stage: StageType,
        role: _Role,
        index: int,
        event: str,
        data: dict[str, Any] | None,
    ) -> None:
        """Increment the counter for the custom *event* name."""
        counters = self._ensure(index)
        counters[event] = counters.get(event, 0) + 1
