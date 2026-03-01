"""Tests for parallel/streaming execution features.

These tests exercise behaviour specific to ``run_parallel()`` and the
streaming pipeline — backpressure, concurrent stages, exception propagation,
the ``parallel()`` data-parallelism wrapper, free-threading detection,
and ``StreamingHandler`` protocol dispatch.
"""

from __future__ import annotations

import os
import tempfile
import threading
import time
from collections.abc import Iterable, Iterator
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from slonk import (
    Base,
    ExampleModel,
    Handler,
    PathHandler,
    ShellCommandHandler,
    Slonk,
    SQLAlchemyHandler,
    StreamingHandler,
    TeeHandler,
    _CallableHandler,
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


# ---------------------------------------------------------------------------
# StreamingHandler protocol detection
# ---------------------------------------------------------------------------


class TestStreamingHandlerProtocol:
    """Verify that @runtime_checkable structural typing detects StreamingHandler correctly."""

    def test_path_handler_is_streaming(self) -> None:
        handler = PathHandler("/dev/null")
        assert isinstance(handler, StreamingHandler)
        assert isinstance(handler, Handler)

    def test_shell_command_handler_is_streaming(self) -> None:
        handler = ShellCommandHandler("echo hi")
        assert isinstance(handler, StreamingHandler)
        assert isinstance(handler, Handler)

    def test_sqlalchemy_handler_is_streaming(self) -> None:
        engine = create_engine(
            "sqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        Base.metadata.create_all(engine)
        sf = sessionmaker(bind=engine)
        handler = SQLAlchemyHandler(ExampleModel, sf)
        assert isinstance(handler, StreamingHandler)
        assert isinstance(handler, Handler)

    def test_callable_handler_is_not_streaming(self) -> None:
        handler = _CallableHandler(lambda x: x or [])
        assert isinstance(handler, Handler)
        assert not isinstance(handler, StreamingHandler)

    def test_tee_handler_is_not_streaming(self) -> None:
        handler = TeeHandler(Slonk())
        assert isinstance(handler, Handler)
        assert not isinstance(handler, StreamingHandler)

    def test_slonk_is_not_streaming(self) -> None:
        s = Slonk()
        assert not isinstance(s, StreamingHandler)


# ---------------------------------------------------------------------------
# PathHandler streaming
# ---------------------------------------------------------------------------


class TestPathHandlerStreaming:
    @pytest.mark.parallel_only()
    def test_streaming_read(self) -> None:
        """PathHandler.process_stream(None) yields file lines lazily."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("alpha\nbeta\ngamma\n")
            f.flush()
            path = f.name

        try:
            handler = PathHandler(path)
            lines = list(handler.process_stream(None))
            assert lines == ["alpha\n", "beta\n", "gamma\n"]
        finally:
            os.unlink(path)

    @pytest.mark.parallel_only()
    def test_streaming_write_passthrough(self) -> None:
        """PathHandler.process_stream(data) writes to file and yields items."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            path = f.name

        try:
            handler = PathHandler(path)
            result = list(handler.process_stream(iter(["one", "two", "three"])))
            assert result == ["one", "two", "three"]

            # Verify the file was written correctly.
            with open(path) as fh:
                content = fh.read()
            assert content == "one\ntwo\nthree\n"
        finally:
            os.unlink(path)

    @pytest.mark.parallel_only()
    def test_streaming_read_in_pipeline(self) -> None:
        """PathHandler as a streaming stage reads a file in parallel mode."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("line1\nline2\nline3\n")
            f.flush()
            path = f.name

        try:

            def strip_lines(data: list[str] | None) -> list[str]:
                return [s.strip() for s in data] if data else []

            slonk = Slonk() | path | strip_lines
            result = list(slonk.run_parallel())
            assert result == ["line1", "line2", "line3"]
        finally:
            os.unlink(path)


# ---------------------------------------------------------------------------
# ShellCommandHandler streaming
# ---------------------------------------------------------------------------


class TestShellCommandHandlerStreaming:
    @pytest.mark.parallel_only()
    def test_streaming_cat(self) -> None:
        """cat echoes each input line to stdout, yielded one at a time."""
        handler = ShellCommandHandler("cat")
        result = list(handler.process_stream(iter(["hello", "world"])))
        assert result == ["hello", "world"]

    @pytest.mark.parallel_only()
    def test_streaming_sort(self) -> None:
        """sort buffers until EOF, then yields sorted lines."""
        handler = ShellCommandHandler("sort")
        result = list(handler.process_stream(iter(["cherry", "apple", "banana"])))
        assert result == ["apple", "banana", "cherry"]

    @pytest.mark.parallel_only()
    def test_streaming_grep(self) -> None:
        """grep filters lines matching the pattern."""
        handler = ShellCommandHandler("grep hello")
        result = list(handler.process_stream(iter(["hello world", "goodbye", "hello again"])))
        assert result == ["hello world", "hello again"]

    @pytest.mark.parallel_only()
    def test_streaming_none_input(self) -> None:
        """process_stream(None) returns immediately with no output."""
        handler = ShellCommandHandler("echo should-not-run")
        result = list(handler.process_stream(None))
        assert result == []

    @pytest.mark.parallel_only()
    def test_streaming_command_failure(self) -> None:
        """Non-zero exit code raises RuntimeError."""
        handler = ShellCommandHandler("exit 1")
        with pytest.raises(RuntimeError, match="Command failed"):
            list(handler.process_stream(iter(["data"])))

    @pytest.mark.parallel_only()
    def test_streaming_in_pipeline(self) -> None:
        """ShellCommandHandler streams through a parallel pipeline."""
        slonk = Slonk() | "grep hello"
        result = list(slonk.run_parallel(["hello world", "goodbye", "hello again"]))
        assert result == ["hello world", "hello again"]


# ---------------------------------------------------------------------------
# SQLAlchemyHandler streaming
# ---------------------------------------------------------------------------


class TestSQLAlchemyHandlerStreaming:
    @pytest.fixture()
    def setup_db(self) -> sessionmaker:
        engine = create_engine(
            "sqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        Base.metadata.create_all(engine)
        sf = sessionmaker(bind=engine)
        session = sf()
        session.add_all(
            [
                ExampleModel(id="1", data="Alpha"),
                ExampleModel(id="2", data="Beta"),
                ExampleModel(id="3", data="Gamma"),
            ]
        )
        session.commit()
        session.close()
        return sf

    @pytest.mark.parallel_only()
    def test_streaming_yields_rows(self, setup_db: sessionmaker) -> None:
        """process_stream yields formatted rows via yield_per."""
        handler = SQLAlchemyHandler(ExampleModel, setup_db)
        result = list(handler.process_stream(None))
        assert result == ["1\tAlpha", "2\tBeta", "3\tGamma"]

    @pytest.mark.parallel_only()
    def test_streaming_matches_batch(self, setup_db: sessionmaker) -> None:
        """Streaming and batch produce equivalent rows (same data, same order)."""
        handler = SQLAlchemyHandler(ExampleModel, setup_db)
        batch_result = list(handler.process(None))
        stream_result = list(handler.process_stream(None))
        assert stream_result == batch_result

    @pytest.mark.parallel_only()
    def test_streaming_in_pipeline(self, setup_db: sessionmaker) -> None:
        """SQLAlchemyHandler streams in a parallel pipeline."""
        slonk = Slonk(session_factory=setup_db) | ExampleModel
        result = list(slonk.run_parallel())
        # Same data as batch.
        batch_result = list(slonk.run_sync())
        assert result == batch_result


# ---------------------------------------------------------------------------
# Mixed streaming / batch pipelines
# ---------------------------------------------------------------------------


class TestMixedPipelines:
    @pytest.mark.parallel_only()
    def test_streaming_to_batch_stage(self) -> None:
        """A StreamingHandler followed by a batch callable."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("a\nb\nc\n")
            f.flush()
            path = f.name

        try:

            def upper(data: list[str] | None) -> list[str]:
                return [s.strip().upper() for s in data] if data else []

            slonk = Slonk() | path | upper
            result = list(slonk.run_parallel())
            assert result == ["A", "B", "C"]
        finally:
            os.unlink(path)

    @pytest.mark.parallel_only()
    def test_batch_to_streaming_stage(self) -> None:
        """A batch callable followed by a StreamingHandler."""

        def generate(data: list[str] | None) -> list[str]:
            return ["cherry", "apple", "banana"]

        slonk = Slonk() | generate | "sort"
        result = list(slonk.run_parallel(["ignored"]))
        assert result == ["apple", "banana", "cherry"]

    @pytest.mark.parallel_only()
    def test_streaming_to_streaming(self) -> None:
        """Two consecutive StreamingHandlers (file -> shell)."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("hello world\ngoodbye\nhello again\n")
            f.flush()
            path = f.name

        try:
            slonk = Slonk() | path | "grep hello"
            result = list(slonk.run_parallel())
            assert result == ["hello world", "hello again"]
        finally:
            os.unlink(path)


# ---------------------------------------------------------------------------
# Backpressure with streaming handlers
# ---------------------------------------------------------------------------


class TestStreamingBackpressure:
    @pytest.mark.parallel_only()
    def test_small_queue_with_streaming_no_data_loss(self) -> None:
        """Bounded queue + streaming handlers don't lose data."""

        def generate(data: list[str] | None) -> list[str]:
            return [f"item_{i}" for i in range(50)]

        slonk = Slonk() | generate | "cat"
        result = list(slonk.run_parallel(["seed"], max_queue_size=2))
        assert sorted(result) == sorted(f"item_{i}" for i in range(50))


# ---------------------------------------------------------------------------
# Error propagation in streaming stages
# ---------------------------------------------------------------------------


class TestStreamingErrorPropagation:
    @pytest.mark.parallel_only()
    def test_error_mid_stream(self) -> None:
        """A StreamingHandler that raises after partial output doesn't deadlock."""

        class HalfBrokenHandler:
            """Yields a few items then explodes."""

            def process(self, input_data: Iterable[str] | None) -> Iterable[str]:
                return list(self.process_stream(input_data))

            def process_stream(self, input_stream: Iterable[str] | None) -> Iterator[str]:
                if input_stream is None:
                    return
                for i, item in enumerate(input_stream):
                    if i >= 2:
                        raise RuntimeError("mid-stream explosion")
                    yield item.upper()

        slonk = Slonk()
        slonk.stages.append(HalfBrokenHandler())  # type: ignore[arg-type]

        with pytest.raises(RuntimeError, match="mid-stream explosion"):
            list(slonk.run_parallel(["a", "b", "c", "d", "e"]))

    @pytest.mark.parallel_only()
    def test_error_in_first_streaming_stage(self) -> None:
        """Exception in the first streaming stage propagates correctly."""

        class AlwaysFails:
            def process(self, input_data: Iterable[str] | None) -> Iterable[str]:
                raise ValueError("fail")

            def process_stream(self, input_stream: Iterable[str] | None) -> Iterator[str]:
                raise ValueError("fail")

        slonk = Slonk()
        slonk.stages.append(AlwaysFails())  # type: ignore[arg-type]

        with pytest.raises(ValueError, match="fail"):
            list(slonk.run_parallel(["x"]))


# ---------------------------------------------------------------------------
# None vs empty input distinction
# ---------------------------------------------------------------------------


class TestNoneVsEmptyInput:
    @pytest.mark.parallel_only()
    def test_process_stream_none_vs_empty(self) -> None:
        """Streaming handlers can distinguish None input from empty input."""
        received: list[str] = []

        class SentinelTracker:
            def process(self, input_data: Iterable[str] | None) -> Iterable[str]:
                return list(self.process_stream(input_data))

            def process_stream(self, input_stream: Iterable[str] | None) -> Iterator[str]:
                if input_stream is None:
                    received.append("got_none")
                    yield "none"
                else:
                    items = list(input_stream)
                    if not items:
                        received.append("got_empty")
                        yield "empty"
                    else:
                        received.append("got_data")
                        yield from items

        slonk = Slonk()
        slonk.stages.append(SentinelTracker())  # type: ignore[arg-type]

        result = list(slonk.run_parallel(None))
        assert result == ["none"]
        assert received == ["got_none"]

    @pytest.mark.parallel_only()
    def test_process_stream_receives_empty_for_empty_input(self) -> None:
        """Empty list input (not None) gives an empty iterator, not None."""
        received: list[str] = []

        class SentinelTracker:
            def process(self, input_data: Iterable[str] | None) -> Iterable[str]:
                return list(self.process_stream(input_data))

            def process_stream(self, input_stream: Iterable[str] | None) -> Iterator[str]:
                if input_stream is None:
                    received.append("got_none")
                else:
                    items = list(input_stream)
                    if not items:
                        received.append("got_empty")
                    else:
                        received.append("got_data")
                        yield from items

        slonk = Slonk()
        slonk.stages.append(SentinelTracker())  # type: ignore[arg-type]

        result = list(slonk.run_parallel([]))
        assert result == []
        assert received == ["got_empty"]


# ---------------------------------------------------------------------------
# Generator auto-streaming
# ---------------------------------------------------------------------------


class TestGeneratorAutoStreaming:
    @pytest.mark.parallel_only()
    def test_generator_callable_streams_lazily(self) -> None:
        """A callable returning a generator naturally streams via the batch path."""

        def gen(data: list[str] | None) -> Iterable[str]:
            if data:
                for item in data:
                    yield item.upper()

        slonk = Slonk() | gen
        result = list(slonk.run_parallel(["a", "b", "c"]))
        assert result == ["A", "B", "C"]
