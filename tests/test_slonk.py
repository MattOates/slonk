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
    Base,
    ExampleModel,
    PathHandler,
    ShellCommandHandler,
    Slonk,
    SQLAlchemyHandler,
    TeeHandler,
    tee,
)

if TYPE_CHECKING:
    from conftest import PipelineRunner


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

    def test_process_with_input_data(self) -> None:
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            handler = PathHandler(tmp_path)
            test_data = ["test", "data"]

            result = list(handler.process(test_data))

            # Should return the input data
            assert result == test_data

            # Should also write to file
            with open(tmp_path) as f:
                content = f.read()
                assert content == "test\ndata\n"
        finally:
            os.unlink(tmp_path)

    def test_process_without_input_data(self) -> None:
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as tmp:
            tmp_path = tmp.name

        try:
            # Pre-populate the file
            with open(tmp_path, "w") as f:
                f.write("existing\ncontent\n")

            handler = PathHandler(tmp_path)
            result = list(handler.process(None))

            assert result == ["existing\n", "content\n"]
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

    def test_process_with_input_data(self) -> None:
        handler = ShellCommandHandler("cat")
        test_data = ["hello", "world"]

        result = list(handler.process(test_data))

        assert len(result) == 1
        assert "hello\nworld" in result[0]

    def test_process_without_input_data(self) -> None:
        handler = ShellCommandHandler("echo test")
        result = list(handler.process(None))
        assert result == []

    def test_process_with_grep(self) -> None:
        handler = ShellCommandHandler("grep hello")
        test_data = ["hello world", "goodbye world", "hello again"]

        result = list(handler.process(test_data))

        assert len(result) == 1
        output = result[0]
        assert "hello world" in output
        assert "hello again" in output
        assert "goodbye world" not in output


class TestSQLAlchemyHandler:
    @pytest.fixture()
    def setup_db(self) -> sessionmaker:
        # Use StaticPool + check_same_thread=False so the in-memory DB is
        # accessible from the streaming pipeline's worker threads.
        engine = create_engine(
            "sqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)

        # Add test data
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

    def test_process(self, setup_db: sessionmaker) -> None:
        handler = SQLAlchemyHandler(ExampleModel, setup_db)
        result = list(handler.process(None))

        assert len(result) == 3
        assert "1\tHello World" in result
        assert "2\tGoodbye World" in result
        assert "3\tTest Data" in result

    def test_process_with_input_data(self, setup_db: sessionmaker) -> None:
        handler = SQLAlchemyHandler(ExampleModel, setup_db)
        # Input data is ignored for SQL handler
        result = list(handler.process(["ignored", "input"]))

        assert len(result) == 3


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
        def double_lines(data: list[str] | None) -> list[str]:
            if data is None:
                return []
            return [line + line for line in data]

        slonk = Slonk() | double_lines
        result = list(run_pipeline(slonk, ["hello", "world"]))

        assert result == ["hellohello", "worldworld"]

    def test_run_empty_pipeline(self, run_pipeline: PipelineRunner) -> None:
        slonk = Slonk()
        result = list(run_pipeline(slonk, ["test"]))

        assert result == ["test"]

    def test_run_multi_stage_pipeline(self, run_pipeline: PipelineRunner) -> None:
        def add_prefix(data: list[str] | None) -> list[str]:
            if data is None:
                return []
            return [f"prefix_{line}" for line in data]

        def add_suffix(data: list[str] | None) -> list[str]:
            if data is None:
                return []
            return [f"{line}_suffix" for line in data]

        slonk = Slonk() | add_prefix | add_suffix
        result = list(run_pipeline(slonk, ["test"]))

        assert result == ["prefix_test_suffix"]

    @patch("slonk.UPath")
    def test_or_with_cloud_path(self, mock_upath: MagicMock) -> None:
        # Mock UPath to avoid actual cloud operations
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

    def test_process_with_input_data(self) -> None:
        def add_b(data: list[str] | None) -> list[str]:
            return [line + "_b" for line in data] if data else []

        tee_pipeline = Slonk() | add_b
        tee_handler = TeeHandler(tee_pipeline)

        result = list(tee_handler.process(["test"]))

        # Should include both original data and tee pipeline results
        assert "test" in result
        assert "test_b" in result

    def test_process_without_input_data(self) -> None:
        pipeline = Slonk()
        tee_handler = TeeHandler(pipeline)

        result = list(tee_handler.process(None))

        assert result == []


class TestTeeFunction:
    def test_tee_creates_slonk_with_tee_stage(self) -> None:
        pipeline = Slonk()
        result = tee(pipeline)

        assert isinstance(result, Slonk)
        assert len(result.stages) == 1
        assert isinstance(result.stages[0], TeeHandler)
        assert result.stages[0].pipeline is pipeline


class TestIntegration:
    def test_shell_command_pipeline(self, run_pipeline: PipelineRunner) -> None:
        slonk = Slonk() | "echo hello"
        result = list(run_pipeline(slonk, ["input"]))

        assert len(result) == 1
        assert "hello" in result[0]

    def test_file_write_and_read_pipeline(self, run_pipeline: PipelineRunner) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = os.path.join(tmpdir, "test.txt")

            # Write pipeline
            write_slonk = Slonk() | test_file
            run_pipeline(write_slonk, ["hello", "world"])

            # Read pipeline
            read_slonk = Slonk() | test_file
            result = list(run_pipeline(read_slonk))

            assert result == ["hello\n", "world\n"]

    def test_combined_pipeline(self, run_pipeline: PipelineRunner) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = os.path.join(tmpdir, "test.txt")

            def add_prefix(data: list[str] | None) -> list[str]:
                return [f">> {line}" for line in data] if data else []

            # Combined pipeline: transform -> save -> shell process
            slonk = Slonk() | add_prefix | test_file | "grep '>>'"

            result = list(run_pipeline(slonk, ["line1", "line2"]))

            # Sync mode: batch ShellCommandHandler returns one combined string.
            # Parallel mode: streaming ShellCommandHandler yields one line per item.
            output = "\n".join(result)
            assert ">> line1" in output
            assert ">> line2" in output

    @pytest.fixture()
    def setup_test_db(self) -> sessionmaker:
        # Use StaticPool + check_same_thread=False so the in-memory DB is
        # accessible from the streaming pipeline's worker threads.
        engine = create_engine(
            "sqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        Base.metadata.create_all(engine)
        return sessionmaker(bind=engine)

    def test_sql_to_shell_pipeline(
        self, setup_test_db: sessionmaker, run_pipeline: PipelineRunner
    ) -> None:
        # Add test data
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

        # Sync mode: batch ShellCommandHandler returns one combined string.
        # Parallel mode: streaming ShellCommandHandler yields one line per item.
        output = "\n".join(result)
        assert "Hello World" in output
        assert "Hello Again" in output
        assert "Goodbye World" not in output
