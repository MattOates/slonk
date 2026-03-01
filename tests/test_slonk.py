from __future__ import annotations

import os
import tempfile
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

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
    cat,
    merge,
    tee,
)

if TYPE_CHECKING:
    from conftest import PipelineRunner

from helpers import ExampleBase, ExampleModel


class TestPathHandler:
    def test_init(self) -> None:
        handler = PathHandler("/tmp/test.txt")
        assert str(handler.upath) == "/tmp/test.txt"

    def test_write_and_read(self) -> None:
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            handler = PathHandler(tmp_path)
            test_data = ["line1", "line2", "line3"]

            # Test write
            handler.write(test_data)

            # Test read
            result = list(handler.read())
            expected = ["line1\n", "line2\n", "line3\n"]
            assert result == expected
        finally:
            os.unlink(tmp_path)

    def test_process_source(self) -> None:
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            # Pre-populate the file
            with open(tmp_path, "w") as f:
                f.write("existing\ncontent\n")

            handler = PathHandler(tmp_path)
            result = list(handler.process_source())

            assert result == ["existing\n", "content\n"]
        finally:
            os.unlink(tmp_path)

    def test_process_transform(self) -> None:
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            handler = PathHandler(tmp_path)
            test_data = ["test", "data"]

            result = list(handler.process_transform(test_data))

            # Should return the input data (passthrough)
            assert result == test_data

            # Should also write to file
            with open(tmp_path) as f:
                content = f.read()
                assert content == "test\ndata\n"
        finally:
            os.unlink(tmp_path)

    def test_process_sink(self) -> None:
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            handler = PathHandler(tmp_path)
            test_data = ["sink", "data"]

            result = handler.process_sink(test_data)

            # Sink returns None
            assert result is None

            # Should write to file
            with open(tmp_path) as f:
                content = f.read()
                assert content == "sink\ndata\n"
        finally:
            os.unlink(tmp_path)


class TestShellCommandHandler:
    def test_init(self) -> None:
        handler = ShellCommandHandler("echo test")
        assert handler.command == "echo test"

    def test_run_command_success(self) -> None:
        handler = ShellCommandHandler("echo test")
        result = handler._run_command("ignored input")
        assert result == "test"

    def test_run_command_failure(self) -> None:
        handler = ShellCommandHandler("exit 1")
        with pytest.raises(RuntimeError, match="Command failed with error"):
            handler._run_command("")

    def test_process_transform(self) -> None:
        handler = ShellCommandHandler("cat")
        test_data = ["hello", "world"]

        result = list(handler.process_transform(test_data))
        assert result == ["hello", "world"]

    def test_process_transform_with_grep(self) -> None:
        handler = ShellCommandHandler("grep hello")
        test_data = ["hello world", "goodbye world", "hello again"]

        result = list(handler.process_transform(test_data))
        assert result == ["hello world", "hello again"]


class TestSQLAlchemyHandler:
    @pytest.fixture()
    def setup_db(self) -> sessionmaker:
        engine = create_engine(
            "sqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        ExampleBase.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)

        session = Session()
        session.add_all(
            [
                ExampleModel(id="1", data="Hello World"),
                ExampleModel(id="2", data="Goodbye World"),
                ExampleModel(id="3", data="Test Data"),
            ]
        )
        session.commit()
        session.close()

        return Session

    def test_init(self, setup_db: sessionmaker) -> None:
        handler = SQLAlchemyHandler(ExampleModel, setup_db)
        assert handler.model == ExampleModel

    def test_process_source(self, setup_db: sessionmaker) -> None:
        handler = SQLAlchemyHandler(ExampleModel, setup_db)
        result = list(handler.process_source())

        assert len(result) == 3
        assert "1\tHello World" in result
        assert "2\tGoodbye World" in result
        assert "3\tTest Data" in result

    def test_process_transform_upserts_and_passes_through(self, setup_db: sessionmaker) -> None:
        handler = SQLAlchemyHandler(ExampleModel, setup_db)
        new_data = ["4\tNew Item", "5\tAnother Item"]

        result = list(handler.process_transform(new_data))

        # Passthrough: all items returned unchanged
        assert result == new_data

        # Verify rows were upserted
        source_result = list(handler.process_source())
        assert "4\tNew Item" in source_result
        assert "5\tAnother Item" in source_result

    def test_process_transform_upserts_existing(self, setup_db: sessionmaker) -> None:
        handler = SQLAlchemyHandler(ExampleModel, setup_db)
        # Upsert an existing row with new data
        updated_data = ["1\tUpdated Hello"]

        result = list(handler.process_transform(updated_data))

        assert result == updated_data
        source_result = list(handler.process_source())
        assert "1\tUpdated Hello" in source_result
        assert "1\tHello World" not in source_result

    def test_process_sink_writes_without_passthrough(self, setup_db: sessionmaker) -> None:
        handler = SQLAlchemyHandler(ExampleModel, setup_db)
        new_data = ["10\tSink Item A", "11\tSink Item B"]

        result = handler.process_sink(new_data)

        # Sink returns None
        assert result is None

        # Verify rows were written
        source_result = list(handler.process_source())
        assert "10\tSink Item A" in source_result
        assert "11\tSink Item B" in source_result

    def test_process_sink_upserts_existing(self, setup_db: sessionmaker) -> None:
        handler = SQLAlchemyHandler(ExampleModel, setup_db)
        updated_data = ["2\tSink Updated"]

        handler.process_sink(updated_data)

        source_result = list(handler.process_source())
        assert "2\tSink Updated" in source_result
        assert "2\tGoodbye World" not in source_result

    def test_parse_record_bad_format(self, setup_db: sessionmaker) -> None:
        handler = SQLAlchemyHandler(ExampleModel, setup_db)
        with pytest.raises(ValueError, match="Expected"):
            handler._parse_record("no-tab-here")


class TestSlonk:
    def test_init(self) -> None:
        slonk = Slonk()
        assert slonk.stages == []

    def test_is_path(self) -> None:
        slonk = Slonk()
        # Local paths
        assert slonk._is_path("/absolute/path")
        assert slonk._is_path("./relative/path")
        assert slonk._is_path("../parent/path")
        assert slonk._is_path("file://path")
        # Cloud/remote paths
        assert slonk._is_path("s3://bucket/path")
        assert slonk._is_path("gs://bucket/path")
        assert slonk._is_path("ftp://server/file")
        assert slonk._is_path("http://example.com/data.csv")
        assert slonk._is_path("https://example.com/data.csv")
        assert slonk._is_path("sftp://server/file")
        # Not paths
        assert not slonk._is_path("echo command")
        assert not slonk._is_path("grep pattern")

    def test_or_with_local_path(self) -> None:
        slonk = Slonk()
        result = slonk | "/tmp/test.txt"

        assert result is slonk
        assert len(slonk.stages) == 1
        assert isinstance(slonk.stages[0], PathHandler)

    def test_or_with_shell_command(self) -> None:
        slonk = Slonk()
        result = slonk | "echo test"

        assert result is slonk
        assert len(slonk.stages) == 1
        assert isinstance(slonk.stages[0], ShellCommandHandler)

    def test_or_with_slonk(self) -> None:
        slonk1 = Slonk()
        slonk2 = Slonk()

        result = slonk1 | slonk2

        assert result is slonk1
        assert len(slonk1.stages) == 1
        assert slonk1.stages[0] is slonk2

    def test_or_with_callable(self) -> None:
        def test_func(data: list[str]) -> list[str]:
            return [line.upper() for line in data]

        slonk = Slonk()
        result = slonk | test_func

        assert result is slonk
        assert len(slonk.stages) == 1

    def test_or_with_unsupported_type(self) -> None:
        slonk = Slonk()

        with pytest.raises(TypeError, match="Unsupported type"):
            slonk | 42

    def test_run_simple_pipeline(self, run_pipeline: PipelineRunner) -> None:
        def double_lines(data: list[str]) -> list[str]:
            return [line + line for line in data]

        slonk = Slonk() | double_lines
        result = list(run_pipeline(slonk, ["hello", "world"]))

        assert result == ["hellohello", "worldworld"]

    def test_run_empty_pipeline(self, run_pipeline: PipelineRunner) -> None:
        slonk = Slonk()
        result = list(run_pipeline(slonk, ["test"]))

        assert result == ["test"]

    def test_run_multi_stage_pipeline(self, run_pipeline: PipelineRunner) -> None:
        def add_prefix(data: list[str]) -> list[str]:
            return [f"prefix_{line}" for line in data]

        def add_suffix(data: list[str]) -> list[str]:
            return [f"{line}_suffix" for line in data]

        slonk = Slonk() | add_prefix | add_suffix
        result = list(run_pipeline(slonk, ["test"]))

        assert result == ["prefix_test_suffix"]

    @patch("slonk.handlers.UPath")
    def test_or_with_cloud_path(self, mock_upath: MagicMock) -> None:
        mock_instance = MagicMock()
        mock_upath.return_value = mock_instance

        slonk = Slonk()
        result = slonk | "s3://bucket/file.txt"

        assert result is slonk
        assert len(slonk.stages) == 1
        assert isinstance(slonk.stages[0], PathHandler)

    def test_or_with_sqlalchemy_model_requires_session(self) -> None:
        slonk = Slonk()
        with pytest.raises(ValueError, match="session_factory"):
            slonk | ExampleModel

    def test_or_with_sqlalchemy_model(self) -> None:
        engine = create_engine("sqlite:///:memory:")
        Session = sessionmaker(bind=engine)
        slonk = Slonk(session_factory=Session)
        result = slonk | ExampleModel

        assert result is slonk
        assert len(slonk.stages) == 1
        assert isinstance(slonk.stages[0], SQLAlchemyHandler)


class TestTeeHandler:
    def test_init(self) -> None:
        pipeline = Slonk()
        tee_handler = TeeHandler(pipeline)
        assert tee_handler.pipeline is pipeline

    def test_process_transform(self) -> None:
        def add_b(data: list[str]) -> list[str]:
            return [line + "_b" for line in data]

        tee_pipeline = Slonk() | add_b
        tee_handler = TeeHandler(tee_pipeline)

        result = list(tee_handler.process_transform(["test"]))

        # Should include both original data and tee pipeline results
        assert "test" in result
        assert "test_b" in result


class TestTeeFunction:
    def test_tee_creates_slonk_with_tee_stage(self) -> None:
        pipeline = Slonk()
        result = tee(pipeline)

        assert isinstance(result, Slonk)
        assert len(result.stages) == 1
        assert isinstance(result.stages[0], TeeHandler)
        assert result.stages[0].pipeline is pipeline


class TestRoleProtocols:
    """Verify that @runtime_checkable structural typing detects roles correctly."""

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

    def test_tee_handler_is_transform(self) -> None:
        handler = TeeHandler(Slonk())
        assert isinstance(handler, Transform)

    def test_tee_handler_is_not_source(self) -> None:
        handler = TeeHandler(Slonk())
        assert not isinstance(handler, Source)


class TestRoleValidation:
    """Verify that role validation catches invalid pipeline configurations."""

    def test_transform_only_as_first_stage_no_seed_raises(self) -> None:
        """A Transform-only handler as first stage with no seed data should error."""

        class TransformOnly:
            def process_transform(self, input_data: list[str]) -> list[str]:
                return list(input_data)

        slonk = Slonk()
        slonk.stages.append(TransformOnly())  # type: ignore[arg-type]
        with pytest.raises(TypeError, match="does not implement Source"):
            list(slonk.run(None))

    def test_source_only_in_middle_raises(self) -> None:
        """A Source-only handler in a middle position should error."""

        class SourceOnly:
            def process_source(self) -> list[str]:
                return ["a"]

        def identity(data: list[str]) -> list[str]:
            return list(data)

        slonk = Slonk() | identity
        slonk.stages.append(SourceOnly())  # type: ignore[arg-type]
        slonk.stages.append(ShellCommandHandler("cat"))

        with pytest.raises(TypeError, match="middle stage"):
            list(slonk.run(["data"]))


class TestIntegration:
    def test_shell_command_pipeline(self, run_pipeline: PipelineRunner) -> None:
        slonk = Slonk() | "echo hello"
        # ShellCommandHandler is Transform-only, needs seed data
        result = list(run_pipeline(slonk, ["input"]))

        assert len(result) == 1
        assert "hello" in result[0]

    def test_file_write_and_read_pipeline(self, run_pipeline: PipelineRunner) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = os.path.join(tmpdir, "test.txt")

            # Write pipeline — PathHandler as Sink (last stage with seed data)
            write_slonk = Slonk() | test_file
            run_pipeline(write_slonk, ["hello", "world"])

            # Read pipeline — PathHandler as Source (first stage, no seed)
            read_slonk = Slonk() | test_file
            result = list(run_pipeline(read_slonk))

            assert result == ["hello\n", "world\n"]

    def test_combined_pipeline(self, run_pipeline: PipelineRunner) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = os.path.join(tmpdir, "test.txt")

            def add_prefix(data: list[str]) -> list[str]:
                return [f">> {line}" for line in data]

            # Combined pipeline: transform -> save (transform passthrough) -> shell
            slonk = Slonk() | add_prefix | test_file | "grep '>>'"

            result = list(run_pipeline(slonk, ["line1", "line2"]))

            output = "\n".join(result)
            assert ">> line1" in output
            assert ">> line2" in output

    @pytest.fixture()
    def setup_test_db(self) -> sessionmaker:
        engine = create_engine(
            "sqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        ExampleBase.metadata.create_all(engine)
        return sessionmaker(bind=engine)

    def test_sql_to_shell_pipeline(
        self, setup_test_db: sessionmaker, run_pipeline: PipelineRunner
    ) -> None:
        session = setup_test_db()
        session.add_all(
            [
                ExampleModel(id="1", data="Hello World"),
                ExampleModel(id="2", data="Goodbye World"),
                ExampleModel(id="3", data="Hello Again"),
            ]
        )
        session.commit()
        session.close()

        slonk = Slonk(session_factory=setup_test_db) | ExampleModel | "grep Hello"
        result = list(run_pipeline(slonk))

        output = "\n".join(result)
        assert "Hello World" in output
        assert "Hello Again" in output
        assert "Goodbye World" not in output

    def test_sql_transform_in_middle(
        self, setup_test_db: sessionmaker, run_pipeline: PipelineRunner
    ) -> None:
        """SQLAlchemyHandler as Transform in the middle: upserts + passes through."""
        ExampleBase.metadata.create_all(setup_test_db().get_bind())

        def generate_data(data: list[str]) -> list[str]:
            return ["100\tGenerated A", "101\tGenerated B"]

        slonk = Slonk(session_factory=setup_test_db) | generate_data | ExampleModel | "cat"
        result = list(run_pipeline(slonk, ["seed"]))

        assert "100\tGenerated A" in result
        assert "101\tGenerated B" in result

        # Verify the rows were actually written to the DB
        handler = SQLAlchemyHandler(ExampleModel, setup_test_db)
        source_result = list(handler.process_source())
        assert "100\tGenerated A" in source_result
        assert "101\tGenerated B" in source_result

    def test_sql_sink_at_end(
        self, setup_test_db: sessionmaker, run_pipeline: PipelineRunner
    ) -> None:
        """SQLAlchemyHandler as Sink at end: bulk-writes, returns empty."""
        ExampleBase.metadata.create_all(setup_test_db().get_bind())

        def generate_data(data: list[str]) -> list[str]:
            return ["200\tSink Write A", "201\tSink Write B"]

        slonk = Slonk(session_factory=setup_test_db) | generate_data | ExampleModel
        result = list(run_pipeline(slonk, ["seed"]))

        # Sink returns empty
        assert result == []

        # Verify data was written
        handler = SQLAlchemyHandler(ExampleModel, setup_test_db)
        source_result = list(handler.process_source())
        assert "200\tSink Write A" in source_result
        assert "201\tSink Write B" in source_result


class TestMergeHandler:
    """Tests for MergeHandler and the merge() factory."""

    def test_merge_two_sources_with_upstream(self, run_pipeline: PipelineRunner) -> None:
        """Upstream data + two sub-pipelines are all present in the output."""
        source_a = Slonk() | (lambda: ["a1", "a2"])
        source_b = Slonk() | (lambda: ["b1", "b2"])

        pipeline = Slonk() | (lambda: ["upstream1", "upstream2"]) | merge(source_a, source_b)
        result = sorted(run_pipeline(pipeline))

        assert result == ["a1", "a2", "b1", "b2", "upstream1", "upstream2"]

    def test_merge_empty_upstream(self, run_pipeline: PipelineRunner) -> None:
        """When upstream produces nothing, only sub-pipeline data is emitted."""
        source_a = Slonk() | (lambda: ["a1"])
        source_b = Slonk() | (lambda: ["b1"])

        pipeline = Slonk() | (lambda: []) | merge(source_a, source_b)
        result = sorted(run_pipeline(pipeline))

        assert result == ["a1", "b1"]

    def test_merge_empty_sub_pipelines(self, run_pipeline: PipelineRunner) -> None:
        """When sub-pipelines produce nothing, only upstream data is emitted."""
        empty_a = Slonk() | (lambda: [])
        empty_b = Slonk() | (lambda: [])

        pipeline = Slonk() | (lambda: ["up1", "up2"]) | merge(empty_a, empty_b)
        result = sorted(run_pipeline(pipeline))

        assert result == ["up1", "up2"]

    def test_merge_single_sub_pipeline(self, run_pipeline: PipelineRunner) -> None:
        """Merge with a single sub-pipeline works correctly."""
        source = Slonk() | (lambda: ["extra"])

        pipeline = Slonk() | (lambda: ["main"]) | merge(source)
        result = sorted(run_pipeline(pipeline))

        assert result == ["extra", "main"]

    def test_merge_no_sub_pipelines(self, run_pipeline: PipelineRunner) -> None:
        """Merge with no sub-pipelines is a passthrough."""
        pipeline = Slonk() | (lambda: ["pass1", "pass2"]) | merge()
        result = sorted(run_pipeline(pipeline))

        assert result == ["pass1", "pass2"]

    def test_merge_handler_is_transform(self) -> None:
        """MergeHandler implements the Transform protocol."""
        handler = MergeHandler()
        assert isinstance(handler, Transform)

    def test_merge_factory_creates_pipeline(self) -> None:
        """The merge() factory returns a Slonk with a MergeHandler stage."""
        pipeline = merge(Slonk() | (lambda: ["a"]))
        assert len(pipeline.stages) == 1
        assert isinstance(pipeline.stages[0], MergeHandler)

    def test_merge_method_returns_self(self) -> None:
        """Slonk.merge() returns self for fluent chaining."""
        s = Slonk()
        result = s.merge(Slonk() | (lambda: ["a"]))
        assert result is s

    def test_merge_error_propagation(self, run_pipeline: PipelineRunner) -> None:
        """Errors in sub-pipelines propagate to the caller."""

        def fail() -> list[str]:
            raise RuntimeError("boom")

        failing = Slonk() | fail

        pipeline = Slonk() | (lambda: ["ok"]) | merge(failing)

        with pytest.raises(RuntimeError, match="boom"):
            list(run_pipeline(pipeline))


class TestCatHandler:
    """Tests for CatHandler and the cat() factory."""

    def test_cat_preserves_order(self, run_pipeline: PipelineRunner) -> None:
        """Upstream first, then sub-pipelines in listed order."""
        source_a = Slonk() | (lambda: ["a1", "a2"])
        source_b = Slonk() | (lambda: ["b1", "b2"])

        pipeline = Slonk() | (lambda: ["upstream1", "upstream2"]) | cat(source_a, source_b)
        result = list(run_pipeline(pipeline))

        assert result == ["upstream1", "upstream2", "a1", "a2", "b1", "b2"]

    def test_cat_empty_upstream(self, run_pipeline: PipelineRunner) -> None:
        """When upstream is empty, only sub-pipeline data appears."""
        source_a = Slonk() | (lambda: ["a1"])
        source_b = Slonk() | (lambda: ["b1"])

        pipeline = Slonk() | (lambda: []) | cat(source_a, source_b)
        result = list(run_pipeline(pipeline))

        assert result == ["a1", "b1"]

    def test_cat_empty_sub_pipelines(self, run_pipeline: PipelineRunner) -> None:
        """When sub-pipelines are empty, only upstream data appears."""
        empty_a = Slonk() | (lambda: [])
        empty_b = Slonk() | (lambda: [])

        pipeline = Slonk() | (lambda: ["up1", "up2"]) | cat(empty_a, empty_b)
        result = list(run_pipeline(pipeline))

        assert result == ["up1", "up2"]

    def test_cat_single_sub_pipeline(self, run_pipeline: PipelineRunner) -> None:
        """Cat with a single sub-pipeline: upstream then sub-pipeline."""
        source = Slonk() | (lambda: ["extra"])

        pipeline = Slonk() | (lambda: ["main"]) | cat(source)
        result = list(run_pipeline(pipeline))

        assert result == ["main", "extra"]

    def test_cat_no_sub_pipelines(self, run_pipeline: PipelineRunner) -> None:
        """Cat with no sub-pipelines is a passthrough."""
        pipeline = Slonk() | (lambda: ["pass1", "pass2"]) | cat()
        result = list(run_pipeline(pipeline))

        assert result == ["pass1", "pass2"]

    def test_cat_handler_is_transform(self) -> None:
        """CatHandler implements the Transform protocol."""
        handler = CatHandler()
        assert isinstance(handler, Transform)

    def test_cat_factory_creates_pipeline(self) -> None:
        """The cat() factory returns a Slonk with a CatHandler stage."""
        pipeline = cat(Slonk() | (lambda: ["a"]))
        assert len(pipeline.stages) == 1
        assert isinstance(pipeline.stages[0], CatHandler)

    def test_cat_method_returns_self(self) -> None:
        """Slonk.cat() returns self for fluent chaining."""
        s = Slonk()
        result = s.cat(Slonk() | (lambda: ["a"]))
        assert result is s

    def test_cat_with_downstream_transform(self, run_pipeline: PipelineRunner) -> None:
        """Cat output can flow into a downstream transform."""
        source_a = Slonk() | (lambda: ["hello"])

        pipeline = (
            Slonk() | (lambda: ["world"]) | cat(source_a) | (lambda data: [s.upper() for s in data])
        )
        result = list(run_pipeline(pipeline))

        assert result == ["WORLD", "HELLO"]
