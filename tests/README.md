# Test Suite for Slonk

This directory contains comprehensive tests for the Slonk pipeline framework.

## Test Structure

- `test_slonk.py` - Main test suite covering all core functionality
- `test_cloud_handler.py` - Tests for CloudPathHandler with mocked cloud operations
- `test_edge_cases.py` - Edge cases and error handling tests

## Running Tests

### Prerequisites

Install test dependencies:
```bash
pip install pytest pytest-cov
```

Or if using rye:
```bash
rye sync --dev
```

### Running All Tests

```bash
pytest
```

### Running with Coverage

```bash
pytest --cov=slonk --cov-report=html --cov-report=term
```

### Running Specific Test Files

```bash
pytest tests/test_slonk.py
pytest tests/test_cloud_handler.py
pytest tests/test_edge_cases.py
```

### Running Specific Test Classes or Methods

```bash
pytest tests/test_slonk.py::TestSlonk::test_or_with_local_path
pytest tests/test_slonk.py::TestLocalPathHandler
```

## Test Categories

### Unit Tests
- **LocalPathHandler**: File I/O operations
- **ShellCommandHandler**: Shell command execution
- **SQLAlchemyHandler**: Database operations
- **CloudPathHandler**: Cloud storage operations (mocked)
- **Slonk**: Core pipeline functionality
- **TeeHandler**: Pipeline branching

### Integration Tests
- End-to-end pipeline execution
- File write and read combinations
- SQL to shell command pipelines
- Multi-stage transformations

### Edge Case Tests
- Error handling and exception cases
- Unicode and special character handling
- Large data processing
- Permission errors
- Command failures

## Test Coverage

The test suite aims for high coverage of:
- All handler classes and their methods
- Pipeline construction via the `|` operator
- Data flow through multi-stage pipelines
- Error conditions and edge cases
- Integration between different handler types

## Mocking Strategy

- **Cloud Operations**: CloudPathHandler tests use mocked cloudpathlib to avoid actual cloud service calls
- **Database Operations**: Uses in-memory SQLite databases for isolation
- **File Operations**: Uses temporary files and directories for safe testing
- **Shell Commands**: Uses safe, cross-platform commands where possible

## CI/CD Considerations

These tests are designed to run in CI environments:
- No external dependencies (cloud services, databases)
- Cross-platform compatibility
- Temporary file cleanup
- Reasonable execution time
