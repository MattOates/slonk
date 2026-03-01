"""Tests for the middleware/observer system."""

from __future__ import annotations

import logging
from collections.abc import Iterable
from typing import Any

import pytest

from slonk import (
    LoggingMiddleware,
    Middleware,
    Slonk,
    SlonkBase,
    StageType,
    StatsMiddleware,
    TimingMiddleware,
    _Role,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class RecordingMiddleware(Middleware):
    """Captures every callback invocation for assertion in tests."""

    def __init__(self) -> None:
        self.events: list[tuple[str, dict[str, Any]]] = []

    def on_pipeline_start(self, stages: list[StageType], roles: list[_Role]) -> None:
        self.events.append(
            ("pipeline_start", {"num_stages": len(stages), "roles": [r.value for r in roles]})
        )

    def on_pipeline_end(self, stages: list[StageType], roles: list[_Role], duration: float) -> None:
        self.events.append(("pipeline_end", {"num_stages": len(stages), "duration": duration}))

    def on_stage_start(self, stage: StageType, role: _Role, index: int) -> None:
        self.events.append(
            ("stage_start", {"type": type(stage).__name__, "role": role.value, "index": index})
        )

    def on_stage_end(self, stage: StageType, role: _Role, index: int, duration: float) -> None:
        self.events.append(
            (
                "stage_end",
                {
                    "type": type(stage).__name__,
                    "role": role.value,
                    "index": index,
                    "duration": duration,
                },
            )
        )

    def on_stage_error(
        self, stage: StageType, role: _Role, index: int, error: BaseException
    ) -> None:
        self.events.append(
            (
                "stage_error",
                {
                    "type": type(stage).__name__,
                    "role": role.value,
                    "index": index,
                    "error": str(error),
                },
            )
        )

    def on_event(
        self,
        stage: StageType,
        role: _Role,
        index: int,
        event: str,
        data: dict[str, Any] | None,
    ) -> None:
        self.events.append(("custom", {"index": index, "event": event, "data": data}))


class EmittingSource(SlonkBase):
    """A Source that emits a custom event."""

    def process_source(self) -> Iterable[str]:
        self.emit("source_loaded", {"count": 3})
        return ["a", "b", "c"]


class EmittingTransform(SlonkBase):
    """A Transform that emits a custom event."""

    def process_transform(self, input_data: Iterable[str]) -> Iterable[str]:
        items = list(input_data)
        self.emit("items_counted", {"count": len(items)})
        return [x.upper() for x in items]


class FailingTransform(SlonkBase):
    """A Transform that always fails."""

    def process_transform(self, input_data: Iterable[str]) -> Iterable[str]:
        _items = list(input_data)
        raise RuntimeError("deliberate failure")


# ---------------------------------------------------------------------------
# Tests: Middleware lifecycle (sync + parallel via fixture)
# ---------------------------------------------------------------------------


class TestMiddlewareLifecycle:
    """Verify that pipeline_start/end and stage_start/end fire correctly."""

    def test_single_source_stage(self, run_pipeline: Any) -> None:
        """Source-only pipeline emits pipeline_start, stage_start/end, pipeline_end."""
        mw = RecordingMiddleware()
        p = Slonk() | (lambda: ["x", "y"])
        p.add_middleware(mw)
        result = list(run_pipeline(p))

        assert result == ["x", "y"]

        event_names = [e[0] for e in mw.events]
        assert event_names[0] == "pipeline_start"
        assert event_names[-1] == "pipeline_end"
        assert "stage_start" in event_names
        assert "stage_end" in event_names

    def test_source_transform_sink(self, run_pipeline: Any) -> None:
        """Three-stage pipeline emits correct lifecycle events in order."""
        collected: list[str] = []

        def my_sink(data: Iterable[str]) -> None:
            collected.extend(data)

        mw = RecordingMiddleware()
        p = Slonk() | (lambda: ["a", "b"]) | (lambda data: [x.upper() for x in data]) | my_sink
        p.add_middleware(mw)
        run_pipeline(p)

        assert collected == ["A", "B"]

        event_names = [e[0] for e in mw.events]
        assert event_names[0] == "pipeline_start"
        assert event_names[-1] == "pipeline_end"

        # Should have 3 stage_start and 3 stage_end events.
        assert event_names.count("stage_start") == 3
        assert event_names.count("stage_end") == 3

    def test_pipeline_with_seed_data(self, run_pipeline: Any) -> None:
        """Pipeline with seed data (Transform role for first stage)."""
        mw = RecordingMiddleware()
        p = Slonk() | (lambda data: [x.upper() for x in data])
        p.add_middleware(mw)
        result = list(run_pipeline(p, ["hello", "world"]))

        assert result == ["HELLO", "WORLD"]

        starts = [e for e in mw.events if e[0] == "stage_start"]
        assert len(starts) == 1
        assert starts[0][1]["role"] == "transform"


# ---------------------------------------------------------------------------
# Tests: Error handling
# ---------------------------------------------------------------------------


class TestMiddlewareErrors:
    """Verify stage_error events are emitted on failure."""

    def test_stage_error_event(self, run_pipeline: Any) -> None:
        """A failing stage emits stage_error before the exception propagates."""
        mw = RecordingMiddleware()
        p = Slonk() | (lambda: ["a", "b"]) | FailingTransform()
        p.add_middleware(mw)

        with pytest.raises(RuntimeError, match="deliberate failure"):
            run_pipeline(p)

        event_names = [e[0] for e in mw.events]
        assert "stage_error" in event_names
        assert "pipeline_end" in event_names  # Always emitted, even on error.

        error_event = next(e for e in mw.events if e[0] == "stage_error")
        assert "deliberate failure" in error_event[1]["error"]


# ---------------------------------------------------------------------------
# Tests: Custom events via SlonkBase.emit()
# ---------------------------------------------------------------------------


class TestCustomEvents:
    """Verify that handlers can emit custom events to middleware."""

    def test_emit_from_source(self, run_pipeline: Any) -> None:
        mw = RecordingMiddleware()
        p = Slonk()
        p.stages.append(EmittingSource())
        p.add_middleware(mw)

        result = list(run_pipeline(p))
        assert result == ["a", "b", "c"]

        custom = [e for e in mw.events if e[0] == "custom"]
        assert len(custom) == 1
        assert custom[0][1]["event"] == "source_loaded"
        assert custom[0][1]["data"] == {"count": 3}

    def test_emit_from_transform(self, run_pipeline: Any) -> None:
        mw = RecordingMiddleware()
        p = Slonk() | (lambda: ["x", "y"])
        p.stages.append(EmittingTransform())
        p.add_middleware(mw)

        result = list(run_pipeline(p))
        assert result == ["X", "Y"]

        custom = [e for e in mw.events if e[0] == "custom"]
        assert len(custom) == 1
        assert custom[0][1]["event"] == "items_counted"
        assert custom[0][1]["data"] == {"count": 2}

    def test_emit_noop_without_middleware(self) -> None:
        """emit() is a no-op when no middleware is registered — no error."""
        handler = EmittingSource()
        assert handler._event_queue is None
        handler.emit("test_event", {"key": "value"})  # Should not raise.


# ---------------------------------------------------------------------------
# Tests: Middleware registration API
# ---------------------------------------------------------------------------


class TestRegistrationAPI:
    """Test add_middleware / remove_middleware / per-run middleware."""

    def test_add_middleware_returns_self(self) -> None:
        p = Slonk()
        mw = Middleware()
        result = p.add_middleware(mw)
        assert result is p

    def test_remove_middleware_returns_self(self) -> None:
        p = Slonk()
        mw = Middleware()
        p.add_middleware(mw)
        result = p.remove_middleware(mw)
        assert result is p
        assert mw not in p._middleware

    def test_remove_middleware_raises_on_missing(self) -> None:
        p = Slonk()
        with pytest.raises(ValueError):
            p.remove_middleware(Middleware())

    def test_per_run_middleware(self) -> None:
        """Per-run middleware passed to run() receives events."""
        mw = RecordingMiddleware()
        p = Slonk() | (lambda: ["a"])
        result = list(p.run(middleware=[mw], parallel=False))
        assert result == ["a"]
        event_names = [e[0] for e in mw.events]
        assert "pipeline_start" in event_names

    def test_persistent_and_per_run_merged(self) -> None:
        """Both persistent and per-run middleware receive events."""
        persistent = RecordingMiddleware()
        per_run = RecordingMiddleware()
        p = Slonk() | (lambda: ["a"])
        p.add_middleware(persistent)
        list(p.run(middleware=[per_run], parallel=False))

        assert len(persistent.events) > 0
        assert len(per_run.events) > 0
        # Both should have the same event types.
        assert [e[0] for e in persistent.events] == [e[0] for e in per_run.events]

    def test_no_middleware_zero_overhead(self, run_pipeline: Any) -> None:
        """Without middleware, pipeline runs normally with no dispatcher."""
        p = Slonk() | (lambda: ["hello"])
        result = list(run_pipeline(p))
        assert result == ["hello"]


# ---------------------------------------------------------------------------
# Tests: Middleware cleanup
# ---------------------------------------------------------------------------


class TestMiddlewareCleanup:
    """Verify that middleware wiring is cleaned up after execution."""

    def test_event_queue_cleared_after_run(self) -> None:
        source = EmittingSource()
        p = Slonk()
        p.stages.append(source)
        mw = RecordingMiddleware()
        p.add_middleware(mw)
        list(p.run_sync())
        # After run, the event queue should be cleared.
        assert source._event_queue is None
        assert source._stage_index == -1

    def test_event_queue_cleared_after_parallel_run(self) -> None:
        source = EmittingSource()
        p = Slonk()
        p.stages.append(source)
        mw = RecordingMiddleware()
        p.add_middleware(mw)
        list(p.run_parallel())
        assert source._event_queue is None

    def test_event_queue_cleared_after_error(self) -> None:
        """Cleanup happens even when a stage errors."""
        transform = FailingTransform()
        p = Slonk() | (lambda: ["x"])
        p.stages.append(transform)
        mw = RecordingMiddleware()
        p.add_middleware(mw)
        with pytest.raises(RuntimeError):
            list(p.run_sync())
        assert transform._event_queue is None


# ---------------------------------------------------------------------------
# Tests: TimingMiddleware
# ---------------------------------------------------------------------------


class TestTimingMiddleware:
    def test_records_stage_durations(self, run_pipeline: Any) -> None:
        tm = TimingMiddleware()
        p = Slonk() | (lambda: ["a", "b"]) | (lambda data: [x.upper() for x in data])
        p.add_middleware(tm)
        result = list(run_pipeline(p))

        assert result == ["A", "B"]
        assert 0 in tm.stage_durations
        assert 1 in tm.stage_durations
        assert all(d >= 0.0 for d in tm.stage_durations.values())

    def test_records_pipeline_duration(self) -> None:
        tm = TimingMiddleware()
        p = Slonk() | (lambda: ["a"])
        p.add_middleware(tm)
        list(p.run_sync())

        assert tm.pipeline_duration > 0.0


# ---------------------------------------------------------------------------
# Tests: LoggingMiddleware
# ---------------------------------------------------------------------------


class TestLoggingMiddleware:
    def test_logs_pipeline_lifecycle(self, caplog: pytest.LogCaptureFixture) -> None:
        lm = LoggingMiddleware()
        p = Slonk() | (lambda: ["a"])
        p.add_middleware(lm)

        with caplog.at_level(logging.DEBUG, logger="slonk"):
            list(p.run_sync())

        messages = caplog.text
        assert "Pipeline starting" in messages
        assert "Pipeline completed" in messages
        assert "starting" in messages.lower()

    def test_logs_custom_events(self, caplog: pytest.LogCaptureFixture) -> None:
        lm = LoggingMiddleware()
        source = EmittingSource()
        p = Slonk()
        p.stages.append(source)
        p.add_middleware(lm)

        with caplog.at_level(logging.DEBUG, logger="slonk"):
            list(p.run_sync())

        assert "source_loaded" in caplog.text

    def test_custom_logger_name(self, caplog: pytest.LogCaptureFixture) -> None:
        lm = LoggingMiddleware(logger_name="my_app.pipeline")
        p = Slonk() | (lambda: ["a"])
        p.add_middleware(lm)

        with caplog.at_level(logging.DEBUG, logger="my_app.pipeline"):
            list(p.run_sync())

        assert any(r.name == "my_app.pipeline" for r in caplog.records)

    def test_logs_errors(self, caplog: pytest.LogCaptureFixture) -> None:
        lm = LoggingMiddleware()
        p = Slonk() | (lambda: ["a"]) | FailingTransform()
        p.add_middleware(lm)

        with caplog.at_level(logging.DEBUG, logger="slonk"), pytest.raises(RuntimeError):
            list(p.run_sync())

        assert "failed" in caplog.text.lower()


# ---------------------------------------------------------------------------
# Tests: StatsMiddleware
# ---------------------------------------------------------------------------


class TestStatsMiddleware:
    def test_counts_events(self, run_pipeline: Any) -> None:
        sm = StatsMiddleware()
        p = Slonk() | (lambda: ["a", "b"]) | (lambda data: [x.upper() for x in data])
        p.add_middleware(sm)
        list(run_pipeline(p))

        # Each stage should have start=1 and end=1.
        assert sm.stats[0]["start"] == 1
        assert sm.stats[0]["end"] == 1
        assert sm.stats[1]["start"] == 1
        assert sm.stats[1]["end"] == 1

    def test_records_stage_metadata(self, run_pipeline: Any) -> None:
        sm = StatsMiddleware()
        p = Slonk() | (lambda: ["a"])
        p.add_middleware(sm)
        list(run_pipeline(p))

        assert 0 in sm.stage_metadata
        assert sm.stage_metadata[0]["role"] == "source"

    def test_counts_custom_events(self, run_pipeline: Any) -> None:
        sm = StatsMiddleware()
        source = EmittingSource()
        p = Slonk()
        p.stages.append(source)
        p.add_middleware(sm)
        list(run_pipeline(p))

        assert sm.stats[0].get("source_loaded") == 1

    def test_counts_errors(self) -> None:
        sm = StatsMiddleware()
        p = Slonk() | (lambda: ["a"]) | FailingTransform()
        p.add_middleware(sm)

        with pytest.raises(RuntimeError):
            list(p.run_sync())

        assert sm.stats[1].get("error") == 1


# ---------------------------------------------------------------------------
# Tests: Middleware error isolation
# ---------------------------------------------------------------------------


class TestMiddlewareErrorIsolation:
    """Middleware errors must not break the pipeline."""

    def test_broken_middleware_does_not_break_pipeline(self, run_pipeline: Any) -> None:
        class BrokenMiddleware(Middleware):
            def on_stage_start(self, stage: StageType, role: _Role, index: int) -> None:
                raise RuntimeError("middleware bug")

        mw = BrokenMiddleware()
        p = Slonk() | (lambda: ["ok"])
        p.add_middleware(mw)

        # Pipeline should still succeed.
        result = list(run_pipeline(p))
        assert result == ["ok"]

    def test_broken_middleware_does_not_prevent_other_middleware(self, run_pipeline: Any) -> None:
        class BrokenMiddleware(Middleware):
            def on_pipeline_start(self, stages: list[StageType], roles: list[_Role]) -> None:
                raise RuntimeError("middleware bug")

        broken = BrokenMiddleware()
        recording = RecordingMiddleware()
        p = Slonk() | (lambda: ["ok"])
        p.add_middleware(broken)
        p.add_middleware(recording)

        result = list(run_pipeline(p))
        assert result == ["ok"]
        # The recording middleware should still receive events.
        assert len(recording.events) > 0


# ---------------------------------------------------------------------------
# Tests: Multiple middleware
# ---------------------------------------------------------------------------


class TestMultipleMiddleware:
    def test_all_middleware_receive_events(self, run_pipeline: Any) -> None:
        mw1 = RecordingMiddleware()
        mw2 = RecordingMiddleware()
        p = Slonk() | (lambda: ["a"])
        p.add_middleware(mw1)
        p.add_middleware(mw2)
        list(run_pipeline(p))

        assert len(mw1.events) > 0
        assert [e[0] for e in mw1.events] == [e[0] for e in mw2.events]

    def test_timing_and_stats_together(self, run_pipeline: Any) -> None:
        tm = TimingMiddleware()
        sm = StatsMiddleware()
        p = Slonk() | (lambda: ["a"]) | (lambda data: data)
        p.add_middleware(tm)
        p.add_middleware(sm)
        list(run_pipeline(p))

        assert tm.pipeline_duration > 0.0
        assert 0 in sm.stats
        assert 1 in sm.stats


# ---------------------------------------------------------------------------
# Tests: SlonkBase inheritance
# ---------------------------------------------------------------------------


class TestSlonkBaseInheritance:
    """Verify all handler classes inherit SlonkBase."""

    def test_path_handler(self) -> None:
        from slonk import PathHandler

        assert issubclass(PathHandler, SlonkBase)

    def test_shell_command_handler(self) -> None:
        from slonk import ShellCommandHandler

        assert issubclass(ShellCommandHandler, SlonkBase)

    def test_sql_alchemy_handler(self) -> None:
        from slonk import SQLAlchemyHandler

        assert issubclass(SQLAlchemyHandler, SlonkBase)

    def test_tee_handler(self) -> None:
        from slonk import TeeHandler

        assert issubclass(TeeHandler, SlonkBase)

    def test_callable_wrappers(self) -> None:
        from slonk import _CallableSink, _CallableSource, _CallableTransform

        assert issubclass(_CallableSource, SlonkBase)
        assert issubclass(_CallableTransform, SlonkBase)
        assert issubclass(_CallableSink, SlonkBase)

    def test_parallel_handler(self) -> None:
        from slonk import _ParallelHandler

        assert issubclass(_ParallelHandler, SlonkBase)
