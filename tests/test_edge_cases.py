import pytest
import tempfile
import os
from pathlib import Path
from slonk import Slonk, LocalPathHandler, ShellCommandHandler


class TestErrorHandling:
    
    def test_local_path_handler_file_not_found(self):
        handler = LocalPathHandler("/nonexistent/path/file.txt")
        
        with pytest.raises(FileNotFoundError):
            list(handler.read())
    
    def test_local_path_handler_permission_error(self):
        # Test with a directory that should cause permission issues
        handler = LocalPathHandler("/root/restricted_file.txt")
        
        # This might not work on all systems, so we'll create a more reliable test
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = os.path.join(tmpdir, "test.txt")
            
            # Create file and make it read-only
            Path(test_file).touch()
            os.chmod(test_file, 0o444)  # read-only
            
            handler = LocalPathHandler(test_file)
            
            # Writing should fail due to permissions
            with pytest.raises(PermissionError):
                handler.write(["test"])
    
    def test_shell_command_handler_command_not_found(self):
        handler = ShellCommandHandler("nonexistent_command_xyz")
        
        with pytest.raises(RuntimeError, match="Command failed with error"):
            handler._run_command("")
    
    def test_shell_command_handler_empty_input(self):
        handler = ShellCommandHandler("cat")
        result = list(handler.process([]))
        
        assert result == [""]
    
    def test_slonk_chaining_with_none_output(self):
        def return_none(data):
            return None
        
        def process_none(data):
            return ["processed"] if data is None else data
        
        slonk = Slonk() | return_none | process_none
        result = list(slonk.run(["input"]))
        
        assert result == ["processed"]
    
    def test_slonk_with_generator_stages(self):
        def generator_stage(data):
            if data:
                for item in data:
                    yield f"gen_{item}"
        
        def list_stage(data):
            return list(data) if data else []
        
        slonk = Slonk() | generator_stage | list_stage
        result = list(slonk.run(["a", "b"]))
        
        assert result == ["gen_a", "gen_b"]
    
    def test_empty_data_through_pipeline(self):
        def add_prefix(data):
            return [f"prefix_{item}" for item in data] if data else []
        
        slonk = Slonk() | add_prefix
        result = list(slonk.run([]))
        
        assert result == []
    
    def test_slonk_run_with_none_input(self):
        def handle_none(data):
            return ["default"] if data is None else data
        
        slonk = Slonk() | handle_none
        result = list(slonk.run(None))
        
        assert result == ["default"]


class TestEdgeCases:
    
    def test_large_data_processing(self):
        def identity(data):
            return data
        
        # Test with a larger dataset
        large_data = [f"line_{i}" for i in range(1000)]
        slonk = Slonk() | identity
        result = list(slonk.run(large_data))
        
        assert len(result) == 1000
        assert result[0] == "line_0"
        assert result[-1] == "line_999"
    
    def test_unicode_handling(self):
        with tempfile.NamedTemporaryFile(mode='w', delete=False, encoding='utf-8') as tmp:
            tmp_path = tmp.name
        
        try:
            handler = LocalPathHandler(tmp_path)
            unicode_data = ["Hello 世界", "Café ☕", "🚀 Rocket"]
            
            handler.write(unicode_data)
            result = list(handler.read())
            
            assert "Hello 世界\n" in result
            assert "Café ☕\n" in result
            assert "🚀 Rocket\n" in result
        finally:
            os.unlink(tmp_path)
    
    def test_nested_slonk_pipelines(self):
        def add_a(data):
            return [f"{item}_a" for item in data] if data else []
        
        def add_b(data):
            return [f"{item}_b" for item in data] if data else []
        
        # Create nested pipeline
        inner_pipeline = Slonk() | add_b
        outer_pipeline = Slonk() | add_a | inner_pipeline
        
        result = list(outer_pipeline.run(["test"]))
        assert result == ["test_a_b"]
    
    def test_multiline_shell_output(self):
        # Test shell command that produces multiline output
        handler = ShellCommandHandler("echo -e 'line1\\nline2\\nline3'")
        result = list(handler.process(["ignored"]))
        
        assert len(result) == 1
        output = result[0]
        assert "line1" in output
        assert "line2" in output
        assert "line3" in output
    
    def test_binary_data_handling(self):
        # Test that the system handles binary-like data gracefully
        def binary_to_hex(data):
            return [item.encode().hex() if isinstance(item, str) else str(item) for item in data] if data else []
        
        slonk = Slonk() | binary_to_hex
        result = list(slonk.run(["hello", "world"]))
        
        assert result[0] == "68656c6c6f"  # "hello" in hex
        assert result[1] == "776f726c64"  # "world" in hex


class TestSlonkPipelineTypes:
    
    def test_string_to_path_detection(self):
        slonk = Slonk()
        
        # Test various path formats
        assert slonk._is_local_path("/absolute/path")
        assert slonk._is_local_path("./relative/path")
        assert slonk._is_local_path("../parent/path")
        assert slonk._is_local_path("file:///absolute/path")
        
        assert not slonk._is_local_path("echo command")
        assert not slonk._is_local_path("grep pattern")
        assert not slonk._is_local_path("s3://bucket/key")
    
    def test_cloud_path_detection(self):
        slonk = Slonk()
        
        # Test various cloud path formats
        assert slonk._is_cloud_path("s3://bucket/key")
        assert slonk._is_cloud_path("gs://bucket/key")
        assert slonk._is_cloud_path("azure://container/blob")
        assert slonk._is_cloud_path("wasb://container@account.blob.core.windows.net/blob")
        
        assert not slonk._is_cloud_path("/local/path")
        assert not slonk._is_cloud_path("./relative/path")
        assert not slonk._is_cloud_path("echo command")
        assert not slonk._is_cloud_path("ftp://server/file")  # Not a supported cloud scheme
    
    def test_callable_with_annotations(self):
        def annotated_func(data: list) -> list:
            return [item.upper() for item in data] if data else []
        
        slonk = Slonk() | annotated_func
        result = list(slonk.run(["hello", "world"]))
        
        assert result == ["HELLO", "WORLD"]
    
    def test_lambda_function(self):
        slonk = Slonk() | (lambda data: [item[::-1] for item in data] if data else [])
        result = list(slonk.run(["hello", "world"]))
        
        assert result == ["olleh", "dlrow"]
