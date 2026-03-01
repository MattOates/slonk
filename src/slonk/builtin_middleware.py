from __future__ import annotations

import logging
from typing import Any

from slonk.middleware import Middleware
from slonk.roles import StageType, _Role


class TimingMiddleware(Middleware):
    """Records stage and pipeline durations.

    After the pipeline completes, inspect ``stage_durations`` and
    ``pipeline_duration`` to retrieve timing data.

    ``stage_durations`` maps stage index -> elapsed seconds.
    """

    def __init__(self) -> None:
        self.stage_durations: dict[int, float] = {}
        self.pipeline_duration: float = 0.0

    def on_stage_end(self, stage: StageType, role: _Role, index: int, duration: float) -> None:
        self.stage_durations[index] = duration

    def on_pipeline_end(self, stages: list[StageType], roles: list[_Role], duration: float) -> None:
        self.pipeline_duration = duration


class LoggingMiddleware(Middleware):
    """Logs pipeline lifecycle events via the standard ``logging`` module.

    Stage start/end at INFO, errors at ERROR, custom events at DEBUG.
    """

    def __init__(self, logger_name: str = "slonk") -> None:
        self._logger = logging.getLogger(logger_name)

    def on_pipeline_start(self, stages: list[StageType], roles: list[_Role]) -> None:
        self._logger.info("Pipeline starting with %d stages", len(stages))

    def on_pipeline_end(self, stages: list[StageType], roles: list[_Role], duration: float) -> None:
        self._logger.info("Pipeline completed in %.4fs", duration)

    def on_stage_start(self, stage: StageType, role: _Role, index: int) -> None:
        self._logger.info(
            "Stage %d (%s, role=%s) starting", index, type(stage).__name__, role.value
        )

    def on_stage_end(self, stage: StageType, role: _Role, index: int, duration: float) -> None:
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
        self._logger.debug("Stage %d custom event %r: %s", index, event, data)


class StatsMiddleware(Middleware):
    """Collects event counts and metadata per stage.

    After the pipeline completes, inspect ``stats`` — a mapping of
    stage index -> counter dict.  Each counter dict has keys like
    ``"start"``, ``"end"``, ``"error"``, and any custom event names.

    ``stage_metadata`` maps stage index -> ``{"type": ..., "role": ...}``.
    """

    def __init__(self) -> None:
        self.stats: dict[int, dict[str, int]] = {}
        self.stage_metadata: dict[int, dict[str, str]] = {}

    def _ensure(self, index: int) -> dict[str, int]:
        if index not in self.stats:
            self.stats[index] = {}
        return self.stats[index]

    def on_stage_start(self, stage: StageType, role: _Role, index: int) -> None:
        counters = self._ensure(index)
        counters["start"] = counters.get("start", 0) + 1
        self.stage_metadata[index] = {
            "type": type(stage).__name__,
            "role": role.value,
        }

    def on_stage_end(self, stage: StageType, role: _Role, index: int, duration: float) -> None:
        counters = self._ensure(index)
        counters["end"] = counters.get("end", 0) + 1

    def on_stage_error(
        self, stage: StageType, role: _Role, index: int, error: BaseException
    ) -> None:
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
        counters = self._ensure(index)
        counters[event] = counters.get(event, 0) + 1
