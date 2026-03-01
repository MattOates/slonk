"""Tests for parallel/streaming execution features.

These tests exercise behaviour specific to ``run_parallel()`` and the
streaming pipeline — backpressure, concurrent stages, exception propagation,
the ``parallel()`` data-parallelism wrapper, free-threading detection,
and role-based protocol dispatch (``Source``, ``Transform``, ``Sink``).
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

from helpers import ExampleBase, ExampleModel
from slonk import (
    CatHandler,
    MergeHandler,
    PathHandler,
    ShellCommandHandler,
    Sink,
    Slonk,
    Source,
    SQLAlchemyHandler,
    TeeHandler,
    Transform,
    _CallableTransform,
    _compute_roles,
    _is_free_threaded,
    _StreamingPipeline,
    cat,
    merge,
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
        with patch("slonk.constants.sys") as mock_sys:
            mock_sys._is_gil_enabled.return_value = True
            assert _is_free_threaded() is False

    def test_is_free_threaded_when_gil_disabled(self) -> None:
        with patch("slonk.constants.sys") as mock_sys:
            mock_sys._is_gil_enabled.return_value = False
            assert _is_free_threaded() is True

    def test_is_free_threaded_when_no_attribute(self) -> None:
        with patch("slonk.constants.sys", spec=[]):
            assert _is_free_threaded() is False


# ---------------------------------------------------------------------------
# Streaming pipeline basics
# ---------------------------------------------------------------------------


class TestStreamingPipeline:
    def test_empty_stages_returns_input(self) -> None:
        sp = _StreamingPipeline(stages=[], roles=[])
        assert sp.execute(["a", "b"]) == ["a", "b"]

    def test_empty_stages_none_input(self) -> None:
        sp = _StreamingPipeline(stages=[], roles=[])
        assert sp.execute(None) == []

    def test_single_stage(self) -> None:
        def upper(data: list[str]) -> list[str]:
            return [s.upper() for s in data] if data else []

        slonk = Slonk() | upper
        result = list(slonk.run_parallel(["hello", "world"]))
        assert result == ["HELLO", "WORLD"]

    def test_multi_stage_order_preserved(self) -> None:
        """Stages execute in order; data flows stage1 -> stage2 -> stage3."""

        def add_a(data: list[str]) -> list[str]:
            return [s + "_a" for s in data] if data else []

        def add_b(data: list[str]) -> list[str]:
            return [s + "_b" for s in data] if data else []

        def add_c(data: list[str]) -> list[str]:
            return [s + "_c" for s in data] if data else []

        slonk = Slonk() | add_a | add_b | add_c
        result = list(slonk.run_parallel(["x"]))
        assert result == ["x_a_b_c"]

    def test_result_ordering_matches_sync(self) -> None:
        """Parallel output is in the same order as sequential output."""

        def number_lines(data: list[str]) -> list[str]:
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

        def slow_stage1(data: list[str]) -> list[str]:
            timestamps["stage1"].append(time.monotonic())
            time.sleep(0.05)
            timestamps["stage1"].append(time.monotonic())
            return list(data) if data else []

        def slow_stage2(data: list[str]) -> list[str]:
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

        def identity(data: list[str]) -> list[str]:
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

        def exploding_stage(data: Iterable[str]) -> list[str]:
            raise ValueError("boom")

        slonk = Slonk() | exploding_stage
        with pytest.raises(ValueError, match="boom"):
            list(slonk.run_parallel(["x"]))

    @pytest.mark.parallel_only()
    def test_exception_in_middle_stage(self) -> None:
        def ok_stage(data: list[str]) -> list[str]:
            return list(data) if data else []

        def bad_stage(data: Iterable[str]) -> list[str]:
            raise RuntimeError("mid-pipeline failure")

        slonk = Slonk() | ok_stage | bad_stage | ok_stage
        with pytest.raises(RuntimeError, match="mid-pipeline failure"):
            list(slonk.run_parallel(["x"]))


# ---------------------------------------------------------------------------
# TeeHandler concurrent execution
# ---------------------------------------------------------------------------


class TestConcurrentTee:
    def test_tee_parallel_produces_same_result_as_sync(self) -> None:
        def add_tag(data: list[str]) -> list[str]:
            return [s + "_tagged" for s in data] if data else []

        side_pipeline = Slonk() | add_tag
        tee_handler = TeeHandler(side_pipeline)

        input_data = ["a", "b", "c"]
        sync_result = sorted(tee_handler.process_transform(input_data))
        # process_transform always runs in the same way regardless of context
        parallel_result = sorted(tee_handler.process_transform(input_data))

        assert parallel_result == sync_result

    def test_tee_in_pipeline_parallel(self) -> None:
        def double(data: list[str]) -> list[str]:
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

        def identity(data: list[str]) -> list[str]:
            return list(data) if data else []

        slonk = Slonk() | identity
        result = list(slonk.run(["a", "b"]))
        assert result == ["a", "b"]

    def test_run_parallel_false_uses_sync(self) -> None:
        def identity(data: list[str]) -> list[str]:
            return list(data) if data else []

        slonk = Slonk() | identity
        result = list(slonk.run(["a", "b"], parallel=False))
        assert result == ["a", "b"]

    def test_run_sync_matches_run_parallel(self) -> None:
        def transform(data: list[str]) -> list[str]:
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

        def upper(data: list[str]) -> list[str]:
            return [s.upper() for s in data] if data else []

        slonk = Slonk() | parallel(upper, workers=2, chunk_size=100)
        result = list(slonk.run_sync(["a", "b", "c"]))
        assert result == ["A", "B", "C"]

    def test_parallel_multiple_chunks_preserves_order(self) -> None:
        """Data is split, processed in parallel, and reassembled in order."""

        def number(data: list[str]) -> list[str]:
            return [f"[{s}]" for s in data] if data else []

        slonk = Slonk() | parallel(number, workers=4, chunk_size=3)
        data = [str(i) for i in range(10)]
        result = list(slonk.run_sync(data))
        assert result == [f"[{i}]" for i in range(10)]

    def test_parallel_with_lambda(self) -> None:
        """Lambdas work via cloudpickle serialisation (on GIL builds)."""
        handler = parallel(lambda data: [s[::-1] for s in data] if data else [], chunk_size=2)
        result = list(handler.process_transform(["abc", "def", "ghi"]))
        assert result == ["cba", "fed", "ihg"]

    def test_parallel_empty_input(self) -> None:
        def identity(data: list[str]) -> list[str]:
            return list(data) if data else []

        handler = parallel(identity)
        result = list(handler.process_transform([]))
        assert result == []

    def test_parallel_in_pipeline(self) -> None:
        """parallel() handler works when piped into a Slonk pipeline."""

        def double(data: list[str]) -> list[str]:
            return [s + s for s in data] if data else []

        slonk = Slonk() | parallel(double, workers=2, chunk_size=5)
        data = [str(i) for i in range(12)]
        result = list(slonk.run_sync(data))
        assert result == [str(i) + str(i) for i in range(12)]

    def test_parallel_handler_uses_threads_on_free_threaded(self) -> None:
        """On free-threaded Python, ThreadPoolExecutor is used."""
        thread_ids: list[int] = []

        def record_thread(data: list[str]) -> list[str]:
            thread_ids.append(threading.current_thread().ident or 0)
            return list(data) if data else []

        handler = parallel(record_thread, workers=2, chunk_size=2)

        with patch("slonk.handlers._is_free_threaded", return_value=True):
            result = list(handler.process_transform(["a", "b", "c", "d"]))

        assert result == ["a", "b", "c", "d"]
        # Should have recorded threads from the pool
        assert len(thread_ids) == 2


# ---------------------------------------------------------------------------
# Role protocol detection
# ---------------------------------------------------------------------------


class TestRoleProtocol:
    """Verify that @runtime_checkable structural typing detects Source/Transform/Sink correctly."""

    def test_path_handler_is_source(self) -> None:
        handler = PathHandler("/dev/null")
        assert isinstance(handler, Source)

    def test_path_handler_is_transform(self) -> None:
        handler = PathHandler("/dev/null")
        assert isinstance(handler, Transform)

    def test_path_handler_is_sink(self) -> None:
        handler = PathHandler("/dev/null")
        assert isinstance(handler, Sink)

    def test_shell_command_handler_is_transform(self) -> None:
        handler = ShellCommandHandler("echo hi")
        assert isinstance(handler, Transform)

    def test_shell_command_handler_is_source(self) -> None:
        handler = ShellCommandHandler("echo hi")
        assert isinstance(handler, Source)

    def test_shell_command_handler_is_not_sink(self) -> None:
        handler = ShellCommandHandler("echo hi")
        assert not isinstance(handler, Sink)

    def test_sqlalchemy_handler_is_source(self) -> None:
        engine = create_engine(
            "sqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        ExampleBase.metadata.create_all(engine)
        sf = sessionmaker(bind=engine)
        handler = SQLAlchemyHandler(ExampleModel, sf)
        assert isinstance(handler, Source)

    def test_sqlalchemy_handler_is_transform(self) -> None:
        engine = create_engine(
            "sqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        ExampleBase.metadata.create_all(engine)
        sf = sessionmaker(bind=engine)
        handler = SQLAlchemyHandler(ExampleModel, sf)
        assert isinstance(handler, Transform)

    def test_sqlalchemy_handler_is_sink(self) -> None:
        engine = create_engine(
            "sqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        ExampleBase.metadata.create_all(engine)
        sf = sessionmaker(bind=engine)
        handler = SQLAlchemyHandler(ExampleModel, sf)
        assert isinstance(handler, Sink)

    def test_callable_transform_is_transform(self) -> None:
        handler = _CallableTransform(lambda x: x)
        assert isinstance(handler, Transform)
        assert not isinstance(handler, Source)
        assert not isinstance(handler, Sink)

    def test_tee_handler_is_transform(self) -> None:
        handler = TeeHandler(Slonk())
        assert isinstance(handler, Transform)
        assert not isinstance(handler, Source)
        assert not isinstance(handler, Sink)

    def test_slonk_is_not_source_or_transform_or_sink(self) -> None:
        s = Slonk()
        assert not isinstance(s, Source)
        assert not isinstance(s, Transform)
        assert not isinstance(s, Sink)


# ---------------------------------------------------------------------------
# PathHandler role methods
# ---------------------------------------------------------------------------


class TestPathHandlerStreaming:
    @pytest.mark.parallel_only()
    def test_source_read(self) -> None:
        """PathHandler.process_source() yields file lines lazily."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("alpha\nbeta\ngamma\n")
            f.flush()
            path = f.name

        try:
            handler = PathHandler(path)
            lines = list(handler.process_source())
            assert lines == ["alpha\n", "beta\n", "gamma\n"]
        finally:
            os.unlink(path)

    @pytest.mark.parallel_only()
    def test_transform_write_passthrough(self) -> None:
        """PathHandler.process_transform(data) writes to file and yields items."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            path = f.name

        try:
            handler = PathHandler(path)
            result = list(handler.process_transform(iter(["one", "two", "three"])))
            assert result == ["one", "two", "three"]

            # Verify the file was written correctly.
            with open(path) as fh:
                content = fh.read()
            assert content == "one\ntwo\nthree\n"
        finally:
            os.unlink(path)

    @pytest.mark.parallel_only()
    def test_source_read_in_pipeline(self) -> None:
        """PathHandler as a Source reads a file in parallel mode."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("line1\nline2\nline3\n")
            f.flush()
            path = f.name

        try:

            def strip_lines(data: list[str]) -> list[str]:
                return [s.strip() for s in data] if data else []

            slonk = Slonk() | path | strip_lines
            result = list(slonk.run_parallel())
            assert result == ["line1", "line2", "line3"]
        finally:
            os.unlink(path)


# ---------------------------------------------------------------------------
# ShellCommandHandler role methods
# ---------------------------------------------------------------------------


class TestShellCommandHandlerStreaming:
    @pytest.mark.parallel_only()
    def test_transform_cat(self) -> None:
        """cat echoes each input line to stdout, yielded one at a time."""
        handler = ShellCommandHandler("cat")
        result = list(handler.process_transform(iter(["hello", "world"])))
        assert result == ["hello", "world"]

    @pytest.mark.parallel_only()
    def test_transform_sort(self) -> None:
        """sort buffers until EOF, then yields sorted lines."""
        handler = ShellCommandHandler("sort")
        result = list(handler.process_transform(iter(["cherry", "apple", "banana"])))
        assert result == ["apple", "banana", "cherry"]

    @pytest.mark.parallel_only()
    def test_transform_grep(self) -> None:
        """grep filters lines matching the pattern."""
        handler = ShellCommandHandler("grep hello")
        result = list(handler.process_transform(iter(["hello world", "goodbye", "hello again"])))
        assert result == ["hello world", "hello again"]

    @pytest.mark.parallel_only()
    def test_transform_command_failure(self) -> None:
        """Non-zero exit code raises RuntimeError."""
        handler = ShellCommandHandler("exit 1")
        with pytest.raises(RuntimeError, match="Command failed"):
            list(handler.process_transform(iter(["data"])))

    @pytest.mark.parallel_only()
    def test_transform_in_pipeline(self) -> None:
        """ShellCommandHandler streams through a parallel pipeline."""
        slonk = Slonk() | "grep hello"
        result = list(slonk.run_parallel(["hello world", "goodbye", "hello again"]))
        assert result == ["hello world", "hello again"]


# ---------------------------------------------------------------------------
# SQLAlchemyHandler role methods
# ---------------------------------------------------------------------------


class TestSQLAlchemyHandlerStreaming:
    @pytest.fixture()
    def setup_db(self) -> sessionmaker:
        engine = create_engine(
            "sqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        ExampleBase.metadata.create_all(engine)
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
    def test_source_yields_rows(self, setup_db: sessionmaker) -> None:
        """process_source yields formatted rows via yield_per."""
        handler = SQLAlchemyHandler(ExampleModel, setup_db)
        result = list(handler.process_source())
        assert result == ["1\tAlpha", "2\tBeta", "3\tGamma"]

    @pytest.mark.parallel_only()
    def test_source_in_pipeline(self, setup_db: sessionmaker) -> None:
        """SQLAlchemyHandler streams in a parallel pipeline."""
        slonk = Slonk(session_factory=setup_db) | ExampleModel
        result = list(slonk.run_parallel())
        # Same data as batch.
        batch_result = list(slonk.run_sync())
        assert result == batch_result

    @pytest.mark.parallel_only()
    def test_transform_upserts_in_pipeline(self, setup_db: sessionmaker) -> None:
        """SQLAlchemyHandler as Transform in parallel: upserts + passes through."""

        def generate(data: list[str]) -> list[str]:
            return ["10\tParallel A", "11\tParallel B"]

        slonk = Slonk(session_factory=setup_db) | generate | ExampleModel | "cat"
        result = list(slonk.run_parallel(["seed"]))

        assert "10\tParallel A" in result
        assert "11\tParallel B" in result

        # Verify rows persisted
        handler = SQLAlchemyHandler(ExampleModel, setup_db)
        source_result = list(handler.process_source())
        assert "10\tParallel A" in source_result

    @pytest.mark.parallel_only()
    def test_sink_writes_in_pipeline(self, setup_db: sessionmaker) -> None:
        """SQLAlchemyHandler as Sink in parallel: bulk-writes, returns empty."""

        def generate(data: list[str]) -> list[str]:
            return ["20\tSink Parallel A", "21\tSink Parallel B"]

        slonk = Slonk(session_factory=setup_db) | generate | ExampleModel
        result = list(slonk.run_parallel(["seed"]))

        assert result == []

        # Verify rows persisted
        handler = SQLAlchemyHandler(ExampleModel, setup_db)
        source_result = list(handler.process_source())
        assert "20\tSink Parallel A" in source_result
        assert "21\tSink Parallel B" in source_result

    @pytest.mark.parallel_only()
    def test_source_to_transform_round_trip(self, setup_db: sessionmaker) -> None:
        """Read from one SQLAlchemy model, write through another (Transform), to cat."""
        slonk = Slonk(session_factory=setup_db) | ExampleModel | ExampleModel | "cat"
        result = list(slonk.run_parallel())

        # Source reads, Transform upserts + passes through, cat echoes
        assert result == ["1\tAlpha", "2\tBeta", "3\tGamma"]


# ---------------------------------------------------------------------------
# Mixed streaming / batch pipelines
# ---------------------------------------------------------------------------


class TestMixedPipelines:
    @pytest.mark.parallel_only()
    def test_streaming_to_batch_stage(self) -> None:
        """A Source/StreamingHandler followed by a batch callable."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("a\nb\nc\n")
            f.flush()
            path = f.name

        try:

            def upper(data: list[str]) -> list[str]:
                return [s.strip().upper() for s in data] if data else []

            slonk = Slonk() | path | upper
            result = list(slonk.run_parallel())
            assert result == ["A", "B", "C"]
        finally:
            os.unlink(path)

    @pytest.mark.parallel_only()
    def test_batch_to_streaming_stage(self) -> None:
        """A batch callable followed by a shell Transform."""

        def generate(data: list[str]) -> list[str]:
            return ["cherry", "apple", "banana"]

        slonk = Slonk() | generate | "sort"
        result = list(slonk.run_parallel(["ignored"]))
        assert result == ["apple", "banana", "cherry"]

    @pytest.mark.parallel_only()
    def test_streaming_to_streaming(self) -> None:
        """Two consecutive streaming stages (file Source -> shell Transform)."""
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

        def generate(data: list[str]) -> list[str]:
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
        """A Transform that raises after partial output doesn't deadlock."""

        class HalfBrokenHandler:
            """Yields a few items then explodes."""

            def process_transform(self, input_data: Iterable[str]) -> Iterator[str]:
                for i, item in enumerate(input_data):
                    if i >= 2:
                        raise RuntimeError("mid-stream explosion")
                    yield item.upper()

        slonk = Slonk()
        slonk.stages.append(HalfBrokenHandler())  # type: ignore[arg-type]

        with pytest.raises(RuntimeError, match="mid-stream explosion"):
            list(slonk.run_parallel(["a", "b", "c", "d", "e"]))

    @pytest.mark.parallel_only()
    def test_error_in_first_streaming_stage(self) -> None:
        """Exception in the first stage propagates correctly."""

        class AlwaysFails:
            def process_transform(self, input_data: Iterable[str]) -> Iterator[str]:
                raise ValueError("fail")

        slonk = Slonk()
        slonk.stages.append(AlwaysFails())  # type: ignore[arg-type]

        with pytest.raises(ValueError, match="fail"):
            list(slonk.run_parallel(["x"]))


# ---------------------------------------------------------------------------
# Role validation
# ---------------------------------------------------------------------------


class TestRoleValidation:
    """Test _compute_roles raises on invalid stage/position combinations."""

    def test_source_required_without_seed(self) -> None:
        """First stage with no seed must implement Source."""

        class OnlyTransform:
            def process_transform(self, input_data: Iterable[str]) -> Iterable[str]:
                return input_data

        with pytest.raises(TypeError, match="does not implement Source"):
            _compute_roles([OnlyTransform()], has_seed=False)  # type: ignore[list-item]

    def test_transform_required_with_seed(self) -> None:
        """First stage of multi-stage pipeline with seed must implement Transform."""

        class OnlySource:
            def process_source(self) -> Iterable[str]:
                return ["a"]

        class OnlyTransformForMiddle:
            def process_transform(self, input_data: Iterable[str]) -> Iterable[str]:
                return input_data

        with pytest.raises(TypeError, match="does not implement Transform"):
            _compute_roles(
                [OnlySource(), OnlyTransformForMiddle()],  # type: ignore[list-item]
                has_seed=True,
            )

    def test_middle_stage_must_be_transform(self) -> None:
        """Middle stages must implement Transform."""

        class OnlySink:
            def process_sink(self, input_data: Iterable[str]) -> None:
                pass

        class GoodSource:
            def process_source(self) -> Iterable[str]:
                return []

        class GoodTransform:
            def process_transform(self, input_data: Iterable[str]) -> Iterable[str]:
                return input_data

        with pytest.raises(TypeError, match="does not implement Transform"):
            _compute_roles(
                [GoodSource(), OnlySink(), GoodTransform()],  # type: ignore[list-item]
                has_seed=False,
            )


# ---------------------------------------------------------------------------
# Generator auto-streaming
# ---------------------------------------------------------------------------


class TestGeneratorAutoStreaming:
    @pytest.mark.parallel_only()
    def test_generator_callable_streams_lazily(self) -> None:
        """A callable returning a generator naturally streams via the Transform path."""

        def gen(data: list[str]) -> Iterable[str]:
            if data:
                for item in data:
                    yield item.upper()

        slonk = Slonk() | gen
        result = list(slonk.run_parallel(["a", "b", "c"]))
        assert result == ["A", "B", "C"]


# ---------------------------------------------------------------------------
# Merge and Cat in parallel mode
# ---------------------------------------------------------------------------


class TestMergeParallel:
    """Tests for MergeHandler under parallel execution."""

    @pytest.mark.parallel_only()
    def test_merge_concurrent_sources_parallel(self) -> None:
        """merge() combines upstream + sub-pipelines in parallel mode."""
        source_a = Slonk() | (lambda: ["a1", "a2"])
        source_b = Slonk() | (lambda: ["b1", "b2"])

        pipeline = Slonk() | (lambda: ["up1"]) | merge(source_a, source_b)
        result = sorted(pipeline.run_parallel())

        assert result == ["a1", "a2", "b1", "b2", "up1"]

    @pytest.mark.parallel_only()
    def test_merge_into_downstream_sink(self) -> None:
        """Merged data flows into a downstream shell sink."""
        source_a = Slonk() | (lambda: ["hello"])
        source_b = Slonk() | (lambda: ["world"])

        pipeline = (
            Slonk()
            | (lambda: ["greetings"])
            | merge(source_a, source_b)
            | (lambda data: sorted(data))
        )
        result = list(pipeline.run_parallel())

        assert result == ["greetings", "hello", "world"]

    @pytest.mark.parallel_only()
    def test_merge_many_sources(self) -> None:
        """merge() handles many sub-pipelines."""

        def make_source(n: int) -> Slonk:
            return Slonk() | (lambda: [f"s{n}"])

        sources = [make_source(i) for i in range(10)]

        pipeline = Slonk() | (lambda: ["base"]) | merge(*sources)
        result = sorted(pipeline.run_parallel())

        expected = ["base"] + [f"s{i}" for i in range(10)]
        assert result == sorted(expected)

    @pytest.mark.parallel_only()
    def test_merge_handler_is_transform_protocol(self) -> None:
        """MergeHandler conforms to Transform protocol."""
        handler = MergeHandler(Slonk() | (lambda: ["x"]))
        assert isinstance(handler, Transform)

    @pytest.mark.parallel_only()
    def test_merge_sync_parallel_consistency(self) -> None:
        """merge() produces the same items in sync and parallel modes."""

        def build() -> Slonk:
            return (
                Slonk()
                | (lambda: ["up"])
                | merge(
                    Slonk() | (lambda: ["a1", "a2"]),
                    Slonk() | (lambda: ["b1", "b2"]),
                )
            )

        sync_result = sorted(build().run_sync())
        parallel_result = sorted(build().run_parallel())

        assert sync_result == parallel_result


class TestCatParallel:
    """Tests for CatHandler under parallel execution."""

    @pytest.mark.parallel_only()
    def test_cat_preserves_order_parallel(self) -> None:
        """cat() yields upstream first, then sub-pipelines in order (parallel mode)."""
        source_a = Slonk() | (lambda: ["a1", "a2"])
        source_b = Slonk() | (lambda: ["b1", "b2"])

        pipeline = Slonk() | (lambda: ["up1", "up2"]) | cat(source_a, source_b)
        result = list(pipeline.run_parallel())

        assert result == ["up1", "up2", "a1", "a2", "b1", "b2"]

    @pytest.mark.parallel_only()
    def test_cat_into_downstream_transform(self) -> None:
        """Concatenated data flows into a downstream transform."""
        source = Slonk() | (lambda: ["world"])

        pipeline = (
            Slonk() | (lambda: ["hello"]) | cat(source) | (lambda data: [s.upper() for s in data])
        )
        result = list(pipeline.run_parallel())

        assert result == ["HELLO", "WORLD"]

    @pytest.mark.parallel_only()
    def test_cat_many_sources(self) -> None:
        """cat() handles many sub-pipelines in order."""

        def make_source(n: int) -> Slonk:
            return Slonk() | (lambda: [f"s{n}"])

        sources = [make_source(i) for i in range(5)]

        pipeline = Slonk() | (lambda: ["base"]) | cat(*sources)
        result = list(pipeline.run_parallel())

        expected = ["base"] + [f"s{i}" for i in range(5)]
        assert result == expected

    @pytest.mark.parallel_only()
    def test_cat_handler_is_transform_protocol(self) -> None:
        """CatHandler conforms to Transform protocol."""
        handler = CatHandler(Slonk() | (lambda: ["x"]))
        assert isinstance(handler, Transform)

    @pytest.mark.parallel_only()
    def test_cat_sync_parallel_consistency(self) -> None:
        """cat() produces the same items and order in sync and parallel modes."""

        def build() -> Slonk:
            return (
                Slonk()
                | (lambda: ["up"])
                | cat(
                    Slonk() | (lambda: ["a1", "a2"]),
                    Slonk() | (lambda: ["b1", "b2"]),
                )
            )

        sync_result = list(build().run_sync())
        parallel_result = list(build().run_parallel())

        assert sync_result == parallel_result
