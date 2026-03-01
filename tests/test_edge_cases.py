from __future__ import annotations

import os
import tempfile
from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from slonk import PathHandler, ShellCommandHandler, Slonk

if TYPE_CHECKING:
    from conftest import PipelineRunner


class TestErrorHandling:
    def test_path_handler_file_not_found(self) -> None:
        handler = PathHandler("/nonexistent/path/file.txt")

        with pytest.raises(FileNotFoundError):
            list(handler.read())

    def test_path_handler_permission_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = os.path.join(tmpdir, "test.txt")

            # Create file and make it read-only
            Path(test_file).touch()
            os.chmod(test_file, 0o444)  # read-only

            handler = PathHandler(test_file)

            # Writing should fail due to permissions
            with pytest.raises(PermissionError):
                handler.write(["test"])

    def test_shell_command_handler_command_not_found(self) -> None:
        handler = ShellCommandHandler("nonexistent_command_xyz")

        with pytest.raises(RuntimeError, match="Command failed with error"):
            handler._run_command("")

    def test_shell_command_handler_empty_input(self) -> None:
        handler = ShellCommandHandler("cat")
        result = list(handler.process([]))

        assert result == [""]

    def test_slonk_chaining_with_none_output(self, run_pipeline: PipelineRunner) -> None:
        def return_none(data: object) -> None:
            return None

        def process_none(data: object) -> list[str]:
            return ["processed"] if data is None else list(data)  # type: ignore[arg-type]

        slonk = Slonk() | return_none | process_none
        result = list(run_pipeline(slonk, ["input"]))

        assert result == ["processed"]

    def test_slonk_with_generator_stages(self, run_pipeline: PipelineRunner) -> None:
        def generator_stage(data: list[str] | None) -> Iterator[str]:
            if data:
                for item in data:
                    yield f"gen_{item}"

        def list_stage(data: list[str] | None) -> list[str]:
            return list(data) if data else []

        slonk = Slonk() | generator_stage | list_stage
        result = list(run_pipeline(slonk, ["a", "b"]))

        assert result == ["gen_a", "gen_b"]

    def test_empty_data_through_pipeline(self, run_pipeline: PipelineRunner) -> None:
        def add_prefix(data: list[str] | None) -> list[str]:
            return [f"prefix_{item}" for item in data] if data else []

        slonk = Slonk() | add_prefix
        result = list(run_pipeline(slonk, []))

        assert result == []

    def test_slonk_run_with_none_input(self, run_pipeline: PipelineRunner) -> None:
        def handle_none(data: list[str] | None) -> list[str]:
            return ["default"] if data is None else data

        slonk = Slonk() | handle_none
        result = list(run_pipeline(slonk, None))

        assert result == ["default"]


class TestEdgeCases:
    def test_large_data_processing(self, run_pipeline: PipelineRunner) -> None:
        def identity(data: list[str] | None) -> list[str]:
            return list(data) if data else []

        # Test with a larger dataset
        large_data = [f"line_{i}" for i in range(1000)]
        slonk = Slonk() | identity
        result = list(run_pipeline(slonk, large_data))

        assert len(result) == 1000
        assert result[0] == "line_0"
        assert result[-1] == "line_999"

    def test_unicode_handling(self) -> None:
        with tempfile.NamedTemporaryFile(mode="w", delete=False, encoding="utf-8") as tmp:
            tmp_path = tmp.name

        try:
            handler = PathHandler(tmp_path)
            unicode_data = ["Hello 世界", "Café ☕", "🚀 Rocket"]

            handler.write(unicode_data)
            result = list(handler.read())

            assert "Hello 世界\n" in result
            assert "Café ☕\n" in result
            assert "🚀 Rocket\n" in result
        finally:
            os.unlink(tmp_path)

    def test_nested_slonk_pipelines(self, run_pipeline: PipelineRunner) -> None:
        def add_a(data: list[str] | None) -> list[str]:
            return [f"{item}_a" for item in data] if data else []

        def add_b(data: list[str] | None) -> list[str]:
            return [f"{item}_b" for item in data] if data else []

        # Create nested pipeline
        inner_pipeline = Slonk() | add_b
        outer_pipeline = Slonk() | add_a | inner_pipeline

        result = list(run_pipeline(outer_pipeline, ["test"]))
        assert result == ["test_a_b"]

    def test_multiline_shell_output(self) -> None:
        # Test shell command that produces multiline output
        handler = ShellCommandHandler("echo -e 'line1\\nline2\\nline3'")
        result = list(handler.process(["ignored"]))

        assert len(result) == 1
        output = result[0]
        assert "line1" in output
        assert "line2" in output
        assert "line3" in output

    def test_binary_data_handling(self, run_pipeline: PipelineRunner) -> None:
        # Test that the system handles binary-like data gracefully
        def binary_to_hex(data: list[str] | None) -> list[str]:
            return (
                [item.encode().hex() if isinstance(item, str) else str(item) for item in data]
                if data
                else []
            )

        slonk = Slonk() | binary_to_hex
        result = list(run_pipeline(slonk, ["hello", "world"]))

        assert result[0] == "68656c6c6f"  # "hello" in hex
        assert result[1] == "776f726c64"  # "world" in hex


class TestSlonkPipelineTypes:
    def test_path_detection(self) -> None:
        slonk = Slonk()

        # Local path formats
        assert slonk._is_path("/absolute/path")
        assert slonk._is_path("./relative/path")
        assert slonk._is_path("../parent/path")
        assert slonk._is_path("file:///absolute/path")

        # Cloud/remote path formats
        assert slonk._is_path("s3://bucket/key")
        assert slonk._is_path("gs://bucket/key")
        assert slonk._is_path("az://container/blob")
        assert slonk._is_path("ftp://server/file")
        assert slonk._is_path("sftp://server/file")
        assert slonk._is_path("http://example.com/data.csv")
        assert slonk._is_path("https://example.com/data.csv")
        assert slonk._is_path("memory://some/path")

        # Not paths — shell commands
        assert not slonk._is_path("echo command")
        assert not slonk._is_path("grep pattern")

        # Unknown URI schemes are not paths
        assert not slonk._is_path("mailto://user@example.com")

    def test_callable_with_annotations(self, run_pipeline: PipelineRunner) -> None:
        def annotated_func(data: list[str]) -> list[str]:
            return [item.upper() for item in data] if data else []

        slonk = Slonk() | annotated_func
        result = list(run_pipeline(slonk, ["hello", "world"]))

        assert result == ["HELLO", "WORLD"]

    def test_lambda_function(self, run_pipeline: PipelineRunner) -> None:
        slonk = Slonk() | (lambda data: [item[::-1] for item in data] if data else [])
        result = list(run_pipeline(slonk, ["hello", "world"]))

        assert result == ["olleh", "dlrow"]
