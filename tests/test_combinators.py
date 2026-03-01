"""Tests for combinator handlers and convenience factories (Phases 2-5).

Covers FilterHandler, MapHandler, FlattenHandler, HeadHandler, SkipHandler,
TailHandler, BatchHandler, their Slonk methods, factory functions, and
Slonk.__repr__.
"""

from __future__ import annotations

import time
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

import pytest

from slonk import (
    BatchHandler,
    FilterHandler,
    FlattenHandler,
    HeadHandler,
    MapHandler,
    ShellCommandHandler,
    Sink,
    SkipHandler,
    Slonk,
    Source,
    TailHandler,
    Transform,
    batch,
    filter,
    flatten,
    head,
    map,
    skip,
    tail,
)

if TYPE_CHECKING:
    from conftest import PipelineRunner


# ======================================================================
# FilterHandler
# ======================================================================


class TestFilterHandler:
    """Tests for FilterHandler."""

    def test_handler_is_transform(self) -> None:
        handler = FilterHandler(lambda x: x)
        assert isinstance(handler, Transform)

    def test_handler_is_not_source(self) -> None:
        handler = FilterHandler(lambda x: x)
        assert not isinstance(handler, Source)

    def test_handler_is_not_sink(self) -> None:
        handler = FilterHandler(lambda x: x)
        assert not isinstance(handler, Sink)

    def test_filter_keeps_matching_items(self) -> None:
        handler = FilterHandler(lambda x: x > 3)
        result = list(handler.process_transform([1, 2, 3, 4, 5]))
        assert result == [4, 5]

    def test_filter_drops_all(self) -> None:
        handler = FilterHandler(lambda x: False)
        result = list(handler.process_transform([1, 2, 3]))
        assert result == []

    def test_filter_keeps_all(self) -> None:
        handler = FilterHandler(lambda x: True)
        result = list(handler.process_transform([1, 2, 3]))
        assert result == [1, 2, 3]

    def test_filter_empty_input(self) -> None:
        handler = FilterHandler(lambda x: True)
        result = list(handler.process_transform([]))
        assert result == []

    def test_filter_preserves_order(self) -> None:
        handler = FilterHandler(lambda x: x % 2 == 0)
        result = list(handler.process_transform([5, 4, 3, 2, 1]))
        assert result == [4, 2]

    def test_filter_with_string_predicate(self) -> None:
        handler = FilterHandler(lambda s: "error" in s.lower())
        result = list(
            handler.process_transform(["INFO: ok", "ERROR: bad", "WARN: meh", "Error: oops"])
        )
        assert result == ["ERROR: bad", "Error: oops"]

    def test_filter_truthy_values(self) -> None:
        """Predicate returning truthy non-bool values should keep items."""
        handler = FilterHandler(lambda x: x)
        result = list(handler.process_transform([0, 1, "", "a", None, [], [1]]))
        assert result == [1, "a", [1]]

    def test_filter_in_pipeline(self, run_pipeline: PipelineRunner) -> None:
        pipeline = Slonk() | (lambda: [1, 2, 3, 4, 5])
        pipeline.filter(lambda x: x > 2)
        result = list(run_pipeline(pipeline))
        assert result == [3, 4, 5]

    def test_filter_method_returns_self(self) -> None:
        pipeline = Slonk()
        result = pipeline.filter(lambda x: x)
        assert result is pipeline

    def test_filter_factory_creates_pipeline(self) -> None:
        pipeline = filter(lambda x: x > 0)
        assert isinstance(pipeline, Slonk)
        assert len(pipeline.stages) == 1
        assert isinstance(pipeline.stages[0], FilterHandler)

    def test_filter_factory_in_pipeline(self, run_pipeline: PipelineRunner) -> None:
        pipeline = Slonk() | (lambda: [1, 2, 3, 4, 5]) | filter(lambda x: x % 2 == 0)
        result = list(run_pipeline(pipeline))
        assert result == [2, 4]

    def test_filter_chained_with_map(self, run_pipeline: PipelineRunner) -> None:
        pipeline = Slonk() | (lambda: [1, 2, 3, 4, 5])
        pipeline.filter(lambda x: x > 2).map(lambda x: x * 10)
        result = list(run_pipeline(pipeline))
        assert result == [30, 40, 50]


# ======================================================================
# MapHandler
# ======================================================================


class TestMapHandler:
    """Tests for MapHandler."""

    def test_handler_is_transform(self) -> None:
        handler = MapHandler(lambda x: x)
        assert isinstance(handler, Transform)

    def test_handler_is_not_source(self) -> None:
        handler = MapHandler(lambda x: x)
        assert not isinstance(handler, Source)

    def test_handler_is_not_sink(self) -> None:
        handler = MapHandler(lambda x: x)
        assert not isinstance(handler, Sink)

    def test_map_applies_function(self) -> None:
        handler = MapHandler(lambda x: x * 2)
        result = list(handler.process_transform([1, 2, 3]))
        assert result == [2, 4, 6]

    def test_map_identity(self) -> None:
        handler = MapHandler(lambda x: x)
        result = list(handler.process_transform([1, 2, 3]))
        assert result == [1, 2, 3]

    def test_map_empty_input(self) -> None:
        handler = MapHandler(lambda x: x * 2)
        result = list(handler.process_transform([]))
        assert result == []

    def test_map_preserves_order(self) -> None:
        handler = MapHandler(lambda x: -x)
        result = list(handler.process_transform([3, 1, 2]))
        assert result == [-3, -1, -2]

    def test_map_type_change(self) -> None:
        handler = MapHandler(str)
        result = list(handler.process_transform([1, 2, 3]))
        assert result == ["1", "2", "3"]

    def test_map_to_complex_type(self) -> None:
        handler = MapHandler(lambda x: {"value": x})
        result = list(handler.process_transform([1, 2]))
        assert result == [{"value": 1}, {"value": 2}]

    def test_map_in_pipeline(self, run_pipeline: PipelineRunner) -> None:
        pipeline = Slonk() | (lambda: ["a", "b", "c"])
        pipeline.map(str.upper)
        result = list(run_pipeline(pipeline))
        assert result == ["A", "B", "C"]

    def test_map_method_returns_self(self) -> None:
        pipeline = Slonk()
        result = pipeline.map(lambda x: x)
        assert result is pipeline

    def test_map_factory_creates_pipeline(self) -> None:
        pipeline = map(str.upper)
        assert isinstance(pipeline, Slonk)
        assert len(pipeline.stages) == 1
        assert isinstance(pipeline.stages[0], MapHandler)

    def test_map_factory_in_pipeline(self, run_pipeline: PipelineRunner) -> None:
        pipeline = Slonk() | (lambda: [1, 2, 3]) | map(lambda x: x + 10)
        result = list(run_pipeline(pipeline))
        assert result == [11, 12, 13]


# ======================================================================
# FlattenHandler
# ======================================================================


class TestFlattenHandler:
    """Tests for FlattenHandler."""

    def test_handler_is_transform(self) -> None:
        handler = FlattenHandler()
        assert isinstance(handler, Transform)

    def test_handler_is_not_source(self) -> None:
        handler = FlattenHandler()
        assert not isinstance(handler, Source)

    def test_flatten_nested_lists(self) -> None:
        handler = FlattenHandler()
        result = list(handler.process_transform([[1, 2], [3, 4], [5]]))
        assert result == [1, 2, 3, 4, 5]

    def test_flatten_empty_input(self) -> None:
        handler = FlattenHandler()
        result = list(handler.process_transform([]))
        assert result == []

    def test_flatten_empty_sublists(self) -> None:
        handler = FlattenHandler()
        result = list(handler.process_transform([[], [1], [], [2, 3], []]))
        assert result == [1, 2, 3]

    def test_flatten_strings_not_iterated(self) -> None:
        """Strings are atoms — not split into characters."""
        handler = FlattenHandler()
        result = list(handler.process_transform(["hello", "world"]))
        assert result == ["hello", "world"]

    def test_flatten_bytes_not_iterated(self) -> None:
        """Bytes are atoms — not split into individual bytes."""
        handler = FlattenHandler()
        result = list(handler.process_transform([b"hello", b"world"]))
        assert result == [b"hello", b"world"]

    def test_flatten_mixed_types(self) -> None:
        """Non-iterable items pass through as-is."""
        handler = FlattenHandler()
        result = list(handler.process_transform([[1, 2], "hello", 42, [3]]))
        assert result == [1, 2, "hello", 42, 3]

    def test_flatten_non_iterable_passthrough(self) -> None:
        handler = FlattenHandler()
        result = list(handler.process_transform([1, 2, 3]))
        assert result == [1, 2, 3]

    def test_flatten_one_level_only(self) -> None:
        """Only one level of nesting is flattened."""
        handler = FlattenHandler()
        result = list(handler.process_transform([[[1, 2]], [[3]]]))
        assert result == [[1, 2], [3]]

    def test_flatten_preserves_order(self) -> None:
        handler = FlattenHandler()
        result = list(handler.process_transform([[3, 2], [1]]))
        assert result == [3, 2, 1]

    def test_flatten_in_pipeline(self, run_pipeline: PipelineRunner) -> None:
        pipeline = Slonk() | (lambda: [[1, 2], [3, 4]])
        pipeline.flatten()
        result = list(run_pipeline(pipeline))
        assert result == [1, 2, 3, 4]

    def test_flatten_method_returns_self(self) -> None:
        pipeline = Slonk()
        result = pipeline.flatten()
        assert result is pipeline

    def test_flatten_factory_creates_pipeline(self) -> None:
        pipeline = flatten()
        assert isinstance(pipeline, Slonk)
        assert len(pipeline.stages) == 1
        assert isinstance(pipeline.stages[0], FlattenHandler)

    def test_flatten_factory_in_pipeline(self, run_pipeline: PipelineRunner) -> None:
        pipeline = Slonk() | (lambda: [[10, 20], [30]]) | flatten()
        result = list(run_pipeline(pipeline))
        assert result == [10, 20, 30]

    def test_batch_then_flatten_roundtrip(self, run_pipeline: PipelineRunner) -> None:
        """batch(n) | flatten() should reconstruct the original stream."""
        pipeline = Slonk() | (lambda: [1, 2, 3, 4, 5])
        pipeline.batch(2).flatten()
        result = list(run_pipeline(pipeline))
        assert result == [1, 2, 3, 4, 5]


# ======================================================================
# HeadHandler
# ======================================================================


class TestHeadHandler:
    """Tests for HeadHandler."""

    def test_handler_is_transform(self) -> None:
        handler = HeadHandler(3)
        assert isinstance(handler, Transform)

    def test_head_takes_first_n(self) -> None:
        handler = HeadHandler(3)
        result = list(handler.process_transform([1, 2, 3, 4, 5]))
        assert result == [1, 2, 3]

    def test_head_fewer_than_n(self) -> None:
        handler = HeadHandler(10)
        result = list(handler.process_transform([1, 2]))
        assert result == [1, 2]

    def test_head_zero(self) -> None:
        handler = HeadHandler(0)
        result = list(handler.process_transform([1, 2, 3]))
        assert result == []

    def test_head_empty_input(self) -> None:
        handler = HeadHandler(5)
        result = list(handler.process_transform([]))
        assert result == []

    def test_head_preserves_order(self) -> None:
        handler = HeadHandler(3)
        result = list(handler.process_transform([5, 4, 3, 2, 1]))
        assert result == [5, 4, 3]

    def test_head_exact_n(self) -> None:
        handler = HeadHandler(3)
        result = list(handler.process_transform([1, 2, 3]))
        assert result == [1, 2, 3]

    def test_head_in_pipeline(self, run_pipeline: PipelineRunner) -> None:
        pipeline = Slonk() | (lambda: [10, 20, 30, 40, 50])
        pipeline.head(3)
        result = list(run_pipeline(pipeline))
        assert result == [10, 20, 30]

    def test_head_method_returns_self(self) -> None:
        pipeline = Slonk()
        result = pipeline.head(5)
        assert result is pipeline

    def test_head_factory_creates_pipeline(self) -> None:
        pipeline = head(3)
        assert isinstance(pipeline, Slonk)
        assert len(pipeline.stages) == 1
        assert isinstance(pipeline.stages[0], HeadHandler)

    def test_head_factory_in_pipeline(self, run_pipeline: PipelineRunner) -> None:
        pipeline = Slonk() | (lambda: list(range(100))) | head(5)
        result = list(run_pipeline(pipeline))
        assert result == [0, 1, 2, 3, 4]

    def test_head_does_not_deadlock_parallel(self) -> None:
        """HeadHandler drains remaining input — must not deadlock in parallel mode."""
        pipeline = Slonk() | (lambda: list(range(1000)))
        pipeline.head(3)
        result = list(pipeline.run_parallel())
        assert result == [0, 1, 2]


# ======================================================================
# SkipHandler
# ======================================================================


class TestSkipHandler:
    """Tests for SkipHandler."""

    def test_handler_is_transform(self) -> None:
        handler = SkipHandler(3)
        assert isinstance(handler, Transform)

    def test_skip_drops_first_n(self) -> None:
        handler = SkipHandler(2)
        result = list(handler.process_transform([1, 2, 3, 4, 5]))
        assert result == [3, 4, 5]

    def test_skip_zero(self) -> None:
        handler = SkipHandler(0)
        result = list(handler.process_transform([1, 2, 3]))
        assert result == [1, 2, 3]

    def test_skip_more_than_available(self) -> None:
        handler = SkipHandler(10)
        result = list(handler.process_transform([1, 2, 3]))
        assert result == []

    def test_skip_empty_input(self) -> None:
        handler = SkipHandler(3)
        result = list(handler.process_transform([]))
        assert result == []

    def test_skip_preserves_order(self) -> None:
        handler = SkipHandler(2)
        result = list(handler.process_transform([5, 4, 3, 2, 1]))
        assert result == [3, 2, 1]

    def test_skip_exact_n(self) -> None:
        handler = SkipHandler(3)
        result = list(handler.process_transform([1, 2, 3]))
        assert result == []

    def test_skip_in_pipeline(self, run_pipeline: PipelineRunner) -> None:
        pipeline = Slonk() | (lambda: [10, 20, 30, 40, 50])
        pipeline.skip(2)
        result = list(run_pipeline(pipeline))
        assert result == [30, 40, 50]

    def test_skip_method_returns_self(self) -> None:
        pipeline = Slonk()
        result = pipeline.skip(5)
        assert result is pipeline

    def test_skip_factory_creates_pipeline(self) -> None:
        pipeline = skip(3)
        assert isinstance(pipeline, Slonk)
        assert len(pipeline.stages) == 1
        assert isinstance(pipeline.stages[0], SkipHandler)

    def test_skip_factory_in_pipeline(self, run_pipeline: PipelineRunner) -> None:
        pipeline = Slonk() | (lambda: list(range(10))) | skip(7)
        result = list(run_pipeline(pipeline))
        assert result == [7, 8, 9]


# ======================================================================
# TailHandler
# ======================================================================


class TestTailHandler:
    """Tests for TailHandler."""

    def test_handler_is_transform(self) -> None:
        handler = TailHandler(3)
        assert isinstance(handler, Transform)

    def test_tail_takes_last_n(self) -> None:
        handler = TailHandler(3)
        result = list(handler.process_transform([1, 2, 3, 4, 5]))
        assert result == [3, 4, 5]

    def test_tail_fewer_than_n(self) -> None:
        handler = TailHandler(10)
        result = list(handler.process_transform([1, 2]))
        assert result == [1, 2]

    def test_tail_empty_input(self) -> None:
        handler = TailHandler(5)
        result = list(handler.process_transform([]))
        assert result == []

    def test_tail_preserves_order(self) -> None:
        handler = TailHandler(3)
        result = list(handler.process_transform([5, 4, 3, 2, 1]))
        assert result == [3, 2, 1]

    def test_tail_exact_n(self) -> None:
        handler = TailHandler(3)
        result = list(handler.process_transform([1, 2, 3]))
        assert result == [1, 2, 3]

    def test_tail_one(self) -> None:
        handler = TailHandler(1)
        result = list(handler.process_transform([1, 2, 3, 4, 5]))
        assert result == [5]

    def test_tail_in_pipeline(self, run_pipeline: PipelineRunner) -> None:
        pipeline = Slonk() | (lambda: [10, 20, 30, 40, 50])
        pipeline.tail(2)
        result = list(run_pipeline(pipeline))
        assert result == [40, 50]

    def test_tail_method_returns_self(self) -> None:
        pipeline = Slonk()
        result = pipeline.tail(5)
        assert result is pipeline

    def test_tail_factory_creates_pipeline(self) -> None:
        pipeline = tail(3)
        assert isinstance(pipeline, Slonk)
        assert len(pipeline.stages) == 1
        assert isinstance(pipeline.stages[0], TailHandler)

    def test_tail_factory_in_pipeline(self, run_pipeline: PipelineRunner) -> None:
        pipeline = Slonk() | (lambda: list(range(10))) | tail(3)
        result = list(run_pipeline(pipeline))
        assert result == [7, 8, 9]


# ======================================================================
# BatchHandler
# ======================================================================


class TestBatchHandler:
    """Tests for BatchHandler."""

    def test_handler_is_transform(self) -> None:
        handler = BatchHandler(3)
        assert isinstance(handler, Transform)

    def test_handler_is_not_source(self) -> None:
        handler = BatchHandler(3)
        assert not isinstance(handler, Source)

    def test_batch_even_split(self) -> None:
        handler = BatchHandler(2)
        result = list(handler.process_transform([1, 2, 3, 4]))
        assert result == [[1, 2], [3, 4]]

    def test_batch_with_remainder(self) -> None:
        handler = BatchHandler(2)
        result = list(handler.process_transform([1, 2, 3, 4, 5]))
        assert result == [[1, 2], [3, 4], [5]]

    def test_batch_size_one(self) -> None:
        handler = BatchHandler(1)
        result = list(handler.process_transform([1, 2, 3]))
        assert result == [[1], [2], [3]]

    def test_batch_larger_than_input(self) -> None:
        handler = BatchHandler(10)
        result = list(handler.process_transform([1, 2, 3]))
        assert result == [[1, 2, 3]]

    def test_batch_empty_input(self) -> None:
        handler = BatchHandler(5)
        result = list(handler.process_transform([]))
        assert result == []

    def test_batch_preserves_order(self) -> None:
        handler = BatchHandler(3)
        result = list(handler.process_transform([5, 4, 3, 2, 1]))
        assert result == [[5, 4, 3], [2, 1]]

    def test_batch_invalid_size_zero(self) -> None:
        with pytest.raises(ValueError, match="Batch size must be >= 1"):
            BatchHandler(0)

    def test_batch_invalid_size_negative(self) -> None:
        with pytest.raises(ValueError, match="Batch size must be >= 1"):
            BatchHandler(-1)

    def test_batch_in_pipeline(self, run_pipeline: PipelineRunner) -> None:
        pipeline = Slonk() | (lambda: [1, 2, 3, 4, 5])
        pipeline.batch(2)
        result = list(run_pipeline(pipeline))
        assert result == [[1, 2], [3, 4], [5]]

    def test_batch_method_returns_self(self) -> None:
        pipeline = Slonk()
        result = pipeline.batch(3)
        assert result is pipeline

    def test_batch_factory_creates_pipeline(self) -> None:
        pipeline = batch(3)
        assert isinstance(pipeline, Slonk)
        assert len(pipeline.stages) == 1
        assert isinstance(pipeline.stages[0], BatchHandler)

    def test_batch_factory_in_pipeline(self, run_pipeline: PipelineRunner) -> None:
        pipeline = Slonk() | (lambda: list(range(7))) | batch(3)
        result = list(run_pipeline(pipeline))
        assert result == [[0, 1, 2], [3, 4, 5], [6]]


# ======================================================================
# Slonk.__repr__
# ======================================================================


class TestSlonkRepr:
    """Tests for Slonk.__repr__."""

    def test_empty_pipeline(self) -> None:
        assert repr(Slonk()) == "Slonk()"

    def test_single_callable(self) -> None:
        def my_func(data: Iterable[Any]) -> Iterable[Any]:
            return data

        pipeline = Slonk() | my_func
        assert "my_func" in repr(pipeline)

    def test_lambda_stage(self) -> None:
        pipeline = Slonk() | (lambda data: data)
        assert "lambda" in repr(pipeline)

    def test_shell_command(self) -> None:
        pipeline = Slonk() | "grep ERROR"
        assert "grep ERROR" in repr(pipeline)

    def test_path_stage(self) -> None:
        pipeline = Slonk() | "/tmp/test.txt"
        r = repr(pipeline)
        assert "test.txt" in r

    def test_handler_class_name(self) -> None:
        pipeline = Slonk()
        pipeline.filter(lambda x: x)
        assert "FilterHandler" in repr(pipeline)

    def test_nested_slonk(self) -> None:
        inner = Slonk() | "sort"
        outer = Slonk() | inner
        r = repr(outer)
        assert "Slonk(sort)" in r

    def test_multi_stage(self) -> None:
        pipeline = Slonk() | "echo hello" | "grep hello" | "sort"
        r = repr(pipeline)
        assert "echo hello" in r
        assert "grep hello" in r
        assert "sort" in r
        assert "|" in r

    def test_repr_format(self) -> None:
        pipeline = Slonk() | "echo hi"
        assert repr(pipeline).startswith("Slonk(")
        assert repr(pipeline).endswith(")")

    def test_combinator_stages_show_class_name(self) -> None:
        pipeline = Slonk()
        pipeline.filter(lambda x: x).map(lambda x: x).head(5)
        r = repr(pipeline)
        assert "FilterHandler" in r
        assert "MapHandler" in r
        assert "HeadHandler" in r


# ======================================================================
# ShellCommandHandler as Source (Phase 6)
# ======================================================================


class TestShellCommandSource:
    """Tests for ShellCommandHandler.process_source()."""

    def test_shell_source_echo(self) -> None:
        handler = ShellCommandHandler("echo hello")
        result = list(handler.process_source())
        assert result == ["hello"]

    def test_shell_source_multiline(self) -> None:
        handler = ShellCommandHandler("printf 'a\\nb\\nc'")
        result = list(handler.process_source())
        assert result == ["a", "b", "c"]

    def test_shell_source_seq(self) -> None:
        handler = ShellCommandHandler("seq 5")
        result = list(handler.process_source())
        assert result == ["1", "2", "3", "4", "5"]

    def test_shell_source_empty_output(self) -> None:
        handler = ShellCommandHandler("true")
        result = list(handler.process_source())
        assert result == []

    def test_shell_source_command_failure(self) -> None:
        handler = ShellCommandHandler("false")
        with pytest.raises(RuntimeError, match="Command failed"):
            list(handler.process_source())

    def test_shell_source_is_source_protocol(self) -> None:
        handler = ShellCommandHandler("echo hello")
        assert isinstance(handler, Source)

    def test_shell_source_in_pipeline(self, run_pipeline: PipelineRunner) -> None:
        pipeline = Slonk() | "echo hello"
        result = list(run_pipeline(pipeline))
        assert result == ["hello"]

    def test_shell_source_piped_to_transform(self, run_pipeline: PipelineRunner) -> None:
        pipeline = Slonk() | "seq 5" | (lambda data: [int(x) * 2 for x in data])
        result = list(run_pipeline(pipeline))
        assert result == [2, 4, 6, 8, 10]


# ======================================================================
# Timeout (Phase 7)
# ======================================================================


class TestTimeout:
    """Tests for the timeout parameter on run() and run_parallel()."""

    def test_pipeline_completes_within_timeout(self) -> None:
        pipeline = Slonk() | (lambda: ["a", "b", "c"])
        result = list(pipeline.run(timeout=10.0))
        assert result == ["a", "b", "c"]

    def test_timeout_none_means_no_limit(self) -> None:
        pipeline = Slonk() | (lambda: [1, 2, 3])
        result = list(pipeline.run(timeout=None))
        assert result == [1, 2, 3]

    def test_timeout_raises_on_slow_pipeline(self) -> None:
        def slow_transform(data: Iterable[Any]) -> Iterable[str]:
            for item in data:
                time.sleep(1)
                yield str(item)

        pipeline = Slonk() | (lambda: list(range(100))) | slow_transform
        with pytest.raises(TimeoutError, match="did not complete"):
            pipeline.run(timeout=0.2)

    def test_timeout_on_run_parallel_directly(self) -> None:
        def slow_transform(data: Iterable[Any]) -> Iterable[str]:
            for item in data:
                time.sleep(1)
                yield str(item)

        pipeline = Slonk() | (lambda: list(range(100))) | slow_transform
        with pytest.raises(TimeoutError, match="did not complete"):
            pipeline.run_parallel(timeout=0.2)

    def test_timeout_sync_mode_ignores_param(self) -> None:
        """Timeout is parallel-mode only; sync ignores it without error."""
        pipeline = Slonk() | (lambda: [1, 2, 3])
        result = list(pipeline.run(parallel=False, timeout=0.001))
        assert result == [1, 2, 3]


# ======================================================================
# Combined / integration combinator tests
# ======================================================================


class TestCombinatorIntegration:
    """Integration tests combining multiple combinators."""

    def test_filter_then_map(self, run_pipeline: PipelineRunner) -> None:
        pipeline = Slonk() | (lambda: list(range(10)))
        pipeline.filter(lambda x: x % 2 == 0).map(lambda x: x * 10)
        result = list(run_pipeline(pipeline))
        assert result == [0, 20, 40, 60, 80]

    def test_map_then_filter(self, run_pipeline: PipelineRunner) -> None:
        pipeline = Slonk() | (lambda: [1, 2, 3, 4, 5])
        pipeline.map(lambda x: x * 2).filter(lambda x: x > 5)
        result = list(run_pipeline(pipeline))
        assert result == [6, 8, 10]

    def test_head_then_tail(self, run_pipeline: PipelineRunner) -> None:
        pipeline = Slonk() | (lambda: list(range(10)))
        pipeline.head(7).tail(3)
        result = list(run_pipeline(pipeline))
        assert result == [4, 5, 6]

    def test_skip_then_head(self, run_pipeline: PipelineRunner) -> None:
        pipeline = Slonk() | (lambda: list(range(10)))
        pipeline.skip(3).head(4)
        result = list(run_pipeline(pipeline))
        assert result == [3, 4, 5, 6]

    def test_batch_map_flatten(self, run_pipeline: PipelineRunner) -> None:
        """batch | map(each batch) | flatten should transform each batch."""
        pipeline = Slonk() | (lambda: [1, 2, 3, 4])
        pipeline.batch(2).map(lambda batch: [x * 10 for x in batch]).flatten()
        result = list(run_pipeline(pipeline))
        assert result == [10, 20, 30, 40]

    def test_full_chain(self, run_pipeline: PipelineRunner) -> None:
        """Source | filter | map | head — a realistic combinator chain."""
        pipeline = Slonk() | (lambda: list(range(100)))
        pipeline.filter(lambda x: x % 3 == 0).map(lambda x: x * 2).head(5)
        result = list(run_pipeline(pipeline))
        assert result == [0, 6, 12, 18, 24]

    def test_generic_non_string_pipeline(self, run_pipeline: PipelineRunner) -> None:
        """Pipeline processes non-string types end to end."""
        pipeline = Slonk() | (lambda: [{"name": "alice", "age": 30}, {"name": "bob", "age": 25}])
        pipeline.filter(lambda d: d["age"] >= 30).map(lambda d: d["name"])
        result = list(run_pipeline(pipeline))
        assert result == ["alice"]

    def test_factory_chain_with_pipe(self, run_pipeline: PipelineRunner) -> None:
        """Factory functions can be chained with |."""
        pipeline = (
            Slonk()
            | (lambda: [1, 2, 3, 4, 5, 6])
            | filter(lambda x: x > 2)
            | map(lambda x: x * 100)
            | head(3)
        )
        result = list(run_pipeline(pipeline))
        assert result == [300, 400, 500]
