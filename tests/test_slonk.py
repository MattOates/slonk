import pytest
import tempfile
import os
from pathlib import Path
from unittest.mock import patch, MagicMock
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from slonk import (
    LocalPathHandler, 
    ShellCommandHandler, 
    SQLAlchemyHandler,
    Slonk,
    TeeHandler,
    tee,
    Base,
    ExampleModel
)


class TestLocalPathHandler:
    
    def test_init(self):
        handler = LocalPathHandler("/tmp/test.txt")
        assert handler.path == Path("/tmp/test.txt")
    
    def test_write_and_read(self):
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as tmp:
            tmp_path = tmp.name
        
        try:
            handler = LocalPathHandler(tmp_path)
            test_data = ["line1", "line2", "line3"]
            
            # Test write
            handler.write(test_data)
            
            # Test read
            result = list(handler.read())
            expected = ["line1\n", "line2\n", "line3\n"]
            assert result == expected
        finally:
            os.unlink(tmp_path)
    
    def test_process_with_input_data(self):
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as tmp:
            tmp_path = tmp.name
        
        try:
            handler = LocalPathHandler(tmp_path)
            test_data = ["test", "data"]
            
            result = list(handler.process(test_data))
            
            # Should return the input data
            assert result == test_data
            
            # Should also write to file
            with open(tmp_path, 'r') as f:
                content = f.read()
                assert content == "test\ndata\n"
        finally:
            os.unlink(tmp_path)
    
    def test_process_without_input_data(self):
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as tmp:
            tmp_path = tmp.name
        
        try:
            # Pre-populate the file
            with open(tmp_path, 'w') as f:
                f.write("existing\ncontent\n")
            
            handler = LocalPathHandler(tmp_path)
            result = list(handler.process(None))
            
            assert result == ["existing\n", "content\n"]
        finally:
            os.unlink(tmp_path)


class TestShellCommandHandler:
    
    def test_init(self):
        handler = ShellCommandHandler("echo test")
        assert handler.command == "echo test"
    
    def test_run_command_success(self):
        handler = ShellCommandHandler("echo test")
        result = handler._run_command("ignored input")
        assert result == "test"
    
    def test_run_command_failure(self):
        handler = ShellCommandHandler("exit 1")
        with pytest.raises(RuntimeError, match="Command failed with error"):
            handler._run_command("")
    
    def test_process_with_input_data(self):
        handler = ShellCommandHandler("cat")
        test_data = ["hello", "world"]
        
        result = list(handler.process(test_data))
        
        assert len(result) == 1
        assert "hello\nworld" in result[0]
    
    def test_process_without_input_data(self):
        handler = ShellCommandHandler("echo test")
        result = list(handler.process(None))
        assert result == []
    
    def test_process_with_grep(self):
        handler = ShellCommandHandler("grep hello")
        test_data = ["hello world", "goodbye world", "hello again"]
        
        result = list(handler.process(test_data))
        
        assert len(result) == 1
        output = result[0]
        assert "hello world" in output
        assert "hello again" in output
        assert "goodbye world" not in output


class TestSQLAlchemyHandler:
    
    @pytest.fixture
    def setup_db(self):
        # Create in-memory database for testing
        engine = create_engine('sqlite:///:memory:')
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        
        # Add test data
        session = Session()
        session.add_all([
            ExampleModel(id="1", data="Hello World"),
            ExampleModel(id="2", data="Goodbye World"),
            ExampleModel(id="3", data="Test Data")
        ])
        session.commit()
        session.close()
        
        return Session
    
    def test_init(self, setup_db):
        handler = SQLAlchemyHandler(ExampleModel)
        assert handler.model == ExampleModel
    
    def test_process(self, setup_db):
        handler = SQLAlchemyHandler(ExampleModel)
        result = list(handler.process(None))
        
        assert len(result) == 3
        assert "1\tHello World" in result
        assert "2\tGoodbye World" in result
        assert "3\tTest Data" in result
    
    def test_process_with_input_data(self, setup_db):
        handler = SQLAlchemyHandler(ExampleModel)
        # Input data is ignored for SQL handler
        result = list(handler.process(["ignored", "input"]))
        
        assert len(result) == 3


class TestSlonk:
    
    def test_init(self):
        slonk = Slonk()
        assert slonk.stages == []
    
    def test_is_local_path(self):
        slonk = Slonk()
        assert slonk._is_local_path("/absolute/path")
        assert slonk._is_local_path("./relative/path")
        assert slonk._is_local_path("file://path")
        assert not slonk._is_local_path("s3://bucket/path")
        assert not slonk._is_local_path("echo command")
    
    def test_is_cloud_path(self):
        slonk = Slonk()
        assert slonk._is_cloud_path("s3://bucket/path")
        assert slonk._is_cloud_path("gs://bucket/path")
        assert slonk._is_cloud_path("azure://bucket/path")
        assert slonk._is_cloud_path("wasb://bucket/path")
        assert not slonk._is_cloud_path("/local/path")
        assert not slonk._is_cloud_path("echo command")
    
    def test_or_with_local_path(self):
        slonk = Slonk()
        result = slonk | "/tmp/test.txt"
        
        assert result is slonk
        assert len(slonk.stages) == 1
        assert isinstance(slonk.stages[0], LocalPathHandler)
    
    def test_or_with_shell_command(self):
        slonk = Slonk()
        result = slonk | "echo test"
        
        assert result is slonk
        assert len(slonk.stages) == 1
        assert isinstance(slonk.stages[0], ShellCommandHandler)
    
    def test_or_with_slonk(self):
        slonk1 = Slonk()
        slonk2 = Slonk()
        
        result = slonk1 | slonk2
        
        assert result is slonk1
        assert len(slonk1.stages) == 1
        assert slonk1.stages[0] is slonk2
    
    def test_or_with_callable(self):
        def test_func(data):
            return [line.upper() for line in data]
        
        slonk = Slonk()
        result = slonk | test_func
        
        assert result is slonk
        assert len(slonk.stages) == 1
        assert slonk.stages[0] is test_func
    
    def test_or_with_unsupported_type(self):
        slonk = Slonk()
        
        with pytest.raises(TypeError, match="Unsupported type"):
            slonk | 42
    
    def test_run_simple_pipeline(self):
        def double_lines(data):
            if data is None:
                return []
            return [line + line for line in data]
        
        slonk = Slonk() | double_lines
        result = list(slonk.run(["hello", "world"]))
        
        assert result == ["hellohello", "worldworld"]
    
    def test_run_empty_pipeline(self):
        slonk = Slonk()
        result = list(slonk.run(["test"]))
        
        assert result == ["test"]
    
    def test_run_multi_stage_pipeline(self):
        def add_prefix(data):
            if data is None:
                return []
            return [f"prefix_{line}" for line in data]
        
        def add_suffix(data):
            if data is None:
                return []
            return [f"{line}_suffix" for line in data]
        
        slonk = Slonk() | add_prefix | add_suffix
        result = list(slonk.run(["test"]))
        
        assert result == ["prefix_test_suffix"]
    
    @patch('slonk.cloudpathlib.CloudPath')
    def test_or_with_cloud_path(self, mock_cloud_path):
        # Mock cloudpathlib to avoid actual cloud operations
        mock_instance = MagicMock()
        mock_cloud_path.return_value = mock_instance
        
        slonk = Slonk()
        result = slonk | "s3://bucket/file.txt"
        
        assert result is slonk
        assert len(slonk.stages) == 1
        # Can't easily test CloudPathHandler without mocking more cloudpathlib internals


class TestTeeHandler:
    
    def test_init(self):
        pipeline = Slonk()
        tee_handler = TeeHandler(pipeline)
        assert tee_handler.pipeline is pipeline
    
    def test_process_with_input_data(self):
        def add_a(data):
            return [line + "_a" for line in data] if data else []
        
        def add_b(data):
            return [line + "_b" for line in data] if data else []
        
        tee_pipeline = Slonk() | add_b
        tee_handler = TeeHandler(tee_pipeline)
        
        result = list(tee_handler.process(["test"]))
        
        # Should include both original data and tee pipeline results
        assert "test" in result
        assert "test_b" in result
    
    def test_process_without_input_data(self):
        pipeline = Slonk()
        tee_handler = TeeHandler(pipeline)
        
        result = list(tee_handler.process(None))
        
        assert result == []


class TestTeeFunction:
    
    def test_tee_creates_slonk_with_tee_stage(self):
        pipeline = Slonk()
        result = tee(pipeline)
        
        assert isinstance(result, Slonk)
        assert len(result.stages) == 1
        assert isinstance(result.stages[0], TeeHandler)
        assert result.stages[0].pipeline is pipeline


class TestIntegration:
    
    def test_shell_command_pipeline(self):
        slonk = Slonk() | "echo hello"
        result = list(slonk.run(["input"]))
        
        assert len(result) == 1
        assert "hello" in result[0]
    
    def test_file_write_and_read_pipeline(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = os.path.join(tmpdir, "test.txt")
            
            # Write pipeline
            write_slonk = Slonk() | test_file
            write_slonk.run(["hello", "world"])
            
            # Read pipeline
            read_slonk = Slonk() | test_file
            result = list(read_slonk.run())
            
            assert result == ["hello\n", "world\n"]
    
    def test_combined_pipeline(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = os.path.join(tmpdir, "test.txt")
            
            def add_prefix(data):
                return [f">> {line}" for line in data] if data else []
            
            # Combined pipeline: transform -> save -> shell process
            slonk = (Slonk() 
                    | add_prefix 
                    | test_file 
                    | "grep '>>'")
            
            result = list(slonk.run(["line1", "line2"]))
            
            assert len(result) == 1
            output = result[0]
            assert ">> line1" in output
            assert ">> line2" in output
    
    @pytest.fixture
    def setup_test_db(self):
        # Create a separate engine for testing to avoid conflicts
        engine = create_engine('sqlite:///:memory:')
        Base.metadata.create_all(engine)
        TestSession = sessionmaker(bind=engine)
        
        # Patch the global Session in the module
        import slonk
        original_session = slonk.Session
        slonk.Session = TestSession
        
        # Add test data
        session = TestSession()
        session.add_all([
            ExampleModel(id="1", data="Hello World"),
            ExampleModel(id="2", data="Goodbye World"),
            ExampleModel(id="3", data="Hello Again")
        ])
        session.commit()
        session.close()
        
        yield TestSession
        
        # Restore original session
        slonk.Session = original_session
    
    def test_sql_to_shell_pipeline(self, setup_test_db):
        slonk = Slonk() | ExampleModel | "grep Hello"
        result = list(slonk.run())
        
        assert len(result) == 1
        output = result[0]
        assert "Hello World" in output
        assert "Hello Again" in output
        assert "Goodbye World" not in output
