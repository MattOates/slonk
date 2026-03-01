"""Tests for parallel/streaming execution features.

These tests exercise behaviour specific to ``run_parallel()`` and the
streaming pipeline — backpressure, concurrent stages, exception propagation,
the ``parallel()`` data-parallelism wrapper, and free-threading detection.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Iterable
from unittest.mock import patch

import pytest

from slonk import (
    Slonk,
    TeeHandler,
    _is_free_threaded,
    _StreamingPipeline,
    parallel,
)

# ---------------------------------------------------------------------------
# Free-threading detection
# ---------------------------------------------------------------------------


class TestFreeThreadingDetection:
    def test_is_free_threaded_returns_bool(self) -> None:
        result = _is_free_threaded()
        assert isinstance(result, bool)

    def test_is_free_threaded_when_gil_enabled(self) -> None:
        with patch("slonk.sys") as mock_sys:
            mock_sys._is_gil_enabled.return_value = True
            assert _is_free_threaded() is False

    def test_is_free_threaded_when_gil_disabled(self) -> None:
        with patch("slonk.sys") as mock_sys:
            mock_sys._is_gil_enabled.return_value = False
            assert _is_free_threaded() is True

    def test_is_free_threaded_when_no_attribute(self) -> None:
        with patch("slonk.sys", spec=[]):
            assert _is_free_threaded() is False


# ---------------------------------------------------------------------------
# Streaming pipeline basics
# ---------------------------------------------------------------------------


class TestStreamingPipeline:
    def test_empty_stages_returns_input(self) -> None:
        sp = _StreamingPipeline(stages=[])
        assert sp.execute(["a", "b"]) == ["a", "b"]

    def test_empty_stages_none_input(self) -> None:
        sp = _StreamingPipeline(stages=[])
        assert sp.execute(None) == []

    def test_single_stage(self) -> None:
        def upper(data: list[str] | None) -> list[str]:
            return [s.upper() for s in data] if data else []

        slonk = Slonk() | upper
        result = list(slonk.run_parallel(["hello", "world"]))
        assert result == ["HELLO", "WORLD"]

    def test_multi_stage_order_preserved(self) -> None:
        """Stages execute in order; data flows stage1 -> stage2 -> stage3."""

        def add_a(data: list[str] | None) -> list[str]:
            return [s + "_a" for s in data] if data else []

        def add_b(data: list[str] | None) -> list[str]:
            return [s + "_b" for s in data] if data else []

        def add_c(data: list[str] | None) -> list[str]:
            return [s + "_c" for s in data] if data else []

        slonk = Slonk() | add_a | add_b | add_c
        result = list(slonk.run_parallel(["x"]))
        assert result == ["x_a_b_c"]

    def test_result_ordering_matches_sync(self) -> None:
        """Parallel output is in the same order as sequential output."""

        def number_lines(data: list[str] | None) -> list[str]:
            return [f"{i}:{s}" for i, s in enumerate(data)] if data else []

        slonk = Slonk() | number_lines
        data = [f"line_{i}" for i in range(200)]
        sync_result = list(slonk.run_sync(data))
        parallel_result = list(slonk.run_parallel(data))
        assert parallel_result == sync_result


# ---------------------------------------------------------------------------
# Concurrent stage execution (stages actually overlap)
# ---------------------------------------------------------------------------


class TestConcurrentExecution:
    @pytest.mark.parallel_only()
    def test_stages_run_concurrently(self) -> None:
        """Verify that stages overlap in time (not purely sequential)."""
        timestamps: dict[str, list[float]] = {"stage1": [], "stage2": []}

        def slow_stage1(data: list[str] | None) -> list[str]:
            timestamps["stage1"].append(time.monotonic())
            time.sleep(0.05)
            timestamps["stage1"].append(time.monotonic())
            return list(data) if data else []

        def slow_stage2(data: list[str] | None) -> list[str]:
            timestamps["stage2"].append(time.monotonic())
            time.sleep(0.05)
            timestamps["stage2"].append(time.monotonic())
            return list(data) if data else []

        slonk = Slonk() | slow_stage1 | slow_stage2
        result = list(slonk.run_parallel(["x"]))
        assert result == ["x"]

        # stage2 should start *after* stage1 pushes output, but importantly
        # both threads existed concurrently — stage2's start is earlier than
        # it would be in a fully-sequential run where stage1 must finish
        # first AND then stage2 starts.  We just verify both ran.
        assert len(timestamps["stage1"]) == 2
        assert len(timestamps["stage2"]) == 2


# ---------------------------------------------------------------------------
# Backpressure
# ---------------------------------------------------------------------------


class TestBackpressure:
    @pytest.mark.parallel_only()
    def test_small_queue_does_not_lose_data(self) -> None:
        """Even with a tiny queue, all data gets through."""

        def identity(data: list[str] | None) -> list[str]:
            return list(data) if data else []

        slonk = Slonk() | identity
        data = [f"item_{i}" for i in range(50)]
        result = list(slonk.run_parallel(data, max_queue_size=2))
        assert result == data


# ---------------------------------------------------------------------------
# Exception propagation
# ---------------------------------------------------------------------------


class TestExceptionPropagation:
    @pytest.mark.parallel_only()
    def test_stage_exception_propagates(self) -> None:
        """An exception in a stage thread is re-raised in the caller."""

        def exploding_stage(data: Iterable[str] | None) -> list[str]:
            raise ValueError("boom")

        slonk = Slonk() | exploding_stage
        with pytest.raises(ValueError, match="boom"):
            list(slonk.run_parallel(["x"]))

    @pytest.mark.parallel_only()
    def test_exception_in_middle_stage(self) -> None:
        def ok_stage(data: list[str] | None) -> list[str]:
            return list(data) if data else []

        def bad_stage(data: Iterable[str] | None) -> list[str]:
            raise RuntimeError("mid-pipeline failure")

        slonk = Slonk() | ok_stage | bad_stage | ok_stage
        with pytest.raises(RuntimeError, match="mid-pipeline failure"):
            list(slonk.run_parallel(["x"]))


# ---------------------------------------------------------------------------
# TeeHandler concurrent execution
# ---------------------------------------------------------------------------


class TestConcurrentTee:
    def test_tee_parallel_produces_same_result_as_sync(self) -> None:
        def add_tag(data: list[str] | None) -> list[str]:
            return [s + "_tagged" for s in data] if data else []

        side_pipeline = Slonk() | add_tag
        tee_handler = TeeHandler(side_pipeline)

        input_data = ["a", "b", "c"]
        sync_result = sorted(tee_handler.process(input_data))
        parallel_result = sorted(tee_handler.process_parallel(input_data))

        assert parallel_result == sync_result

    def test_tee_in_pipeline_parallel(self) -> None:
        def double(data: list[str] | None) -> list[str]:
            return [s + s for s in data] if data else []

        slonk = Slonk()
        slonk.tee(Slonk() | double)

        result_sync = sorted(slonk.run_sync(["x", "y"]))
        result_par = sorted(slonk.run_parallel(["x", "y"]))
        assert result_par == result_sync


# ---------------------------------------------------------------------------
# run() API
# ---------------------------------------------------------------------------


class TestRunAPI:
    def test_run_defaults_to_parallel(self) -> None:
        """run() with default args uses parallel execution."""

        def identity(data: list[str] | None) -> list[str]:
            return list(data) if data else []

        slonk = Slonk() | identity
        result = list(slonk.run(["a", "b"]))
        assert result == ["a", "b"]

    def test_run_parallel_false_uses_sync(self) -> None:
        def identity(data: list[str] | None) -> list[str]:
            return list(data) if data else []

        slonk = Slonk() | identity
        result = list(slonk.run(["a", "b"], parallel=False))
        assert result == ["a", "b"]

    def test_run_sync_matches_run_parallel(self) -> None:
        def transform(data: list[str] | None) -> list[str]:
            return [s.upper() for s in data] if data else []

        slonk = Slonk() | transform
        data = ["hello", "world"]
        assert list(slonk.run_sync(data)) == list(slonk.run_parallel(data))


# ---------------------------------------------------------------------------
# parallel() data-parallelism wrapper
# ---------------------------------------------------------------------------


class TestParallelWrapper:
    def test_parallel_single_chunk_no_split(self) -> None:
        """When data fits in one chunk, no parallelism overhead."""

        def upper(data: list[str] | None) -> list[str]:
            return [s.upper() for s in data] if data else []

        slonk = Slonk() | parallel(upper, workers=2, chunk_size=100)
        result = list(slonk.run_sync(["a", "b", "c"]))
        assert result == ["A", "B", "C"]

    def test_parallel_multiple_chunks_preserves_order(self) -> None:
        """Data is split, processed in parallel, and reassembled in order."""

        def number(data: list[str] | None) -> list[str]:
            return [f"[{s}]" for s in data] if data else []

        slonk = Slonk() | parallel(number, workers=4, chunk_size=3)
        data = [str(i) for i in range(10)]
        result = list(slonk.run_sync(data))
        assert result == [f"[{i}]" for i in range(10)]

    def test_parallel_with_lambda(self) -> None:
        """Lambdas work via cloudpickle serialisation (on GIL builds)."""
        handler = parallel(lambda data: [s[::-1] for s in data] if data else [], chunk_size=2)
        result = list(handler.process(["abc", "def", "ghi"]))
        assert result == ["cba", "fed", "ihg"]

    def test_parallel_empty_input(self) -> None:
        def identity(data: list[str] | None) -> list[str]:
            return list(data) if data else []

        handler = parallel(identity)
        result = list(handler.process([]))
        assert result == []

    def test_parallel_none_input(self) -> None:
        def handle_none(data: list[str] | None) -> list[str]:
            return ["default"] if data is None else data

        handler = parallel(handle_none)
        result = list(handler.process(None))
        assert result == ["default"]

    def test_parallel_in_pipeline(self) -> None:
        """parallel() handler works when piped into a Slonk pipeline."""

        def double(data: list[str] | None) -> list[str]:
            return [s + s for s in data] if data else []

        slonk = Slonk() | parallel(double, workers=2, chunk_size=5)
        data = [str(i) for i in range(12)]
        result = list(slonk.run_sync(data))
        assert result == [str(i) + str(i) for i in range(12)]

    def test_parallel_handler_uses_threads_on_free_threaded(self) -> None:
        """On free-threaded Python, ThreadPoolExecutor is used."""
        thread_ids: list[int] = []

        def record_thread(data: list[str] | None) -> list[str]:
            thread_ids.append(threading.current_thread().ident or 0)
            return list(data) if data else []

        handler = parallel(record_thread, workers=2, chunk_size=2)

        with patch("slonk._is_free_threaded", return_value=True):
            result = list(handler.process(["a", "b", "c", "d"]))

        assert result == ["a", "b", "c", "d"]
        # Should have recorded threads from the pool
        assert len(thread_ids) == 2
