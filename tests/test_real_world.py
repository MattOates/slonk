from __future__ import annotations

import os
import tempfile
from typing import TYPE_CHECKING

from slonk import Slonk

if TYPE_CHECKING:
    from conftest import PipelineRunner


class TestRealWorldScenarios:
    """Test scenarios that simulate real-world usage patterns."""

    def test_data_transformation_pipeline(self, run_pipeline: PipelineRunner) -> None:
        """Test a typical data transformation pipeline."""

        def parse_csv_line(data: list[str] | None) -> list[str]:
            """Simulate parsing CSV data."""
            if not data:
                return []
            result = []
            for line in data:
                if "," in line:
                    parts = line.strip().split(",")
                    result.append(f"{parts[0]}|{parts[1]}")
                else:
                    result.append(line.strip())
            return result

        def filter_valid_records(data: list[str] | None) -> list[str]:
            """Filter out empty or invalid records."""
            if not data:
                return []
            return [line for line in data if line and "|" in line]

        slonk = Slonk() | parse_csv_line | filter_valid_records

        input_data = [
            "john,doe",
            "jane,smith",
            "invalid_record",
            "bob,wilson",
            "",
        ]

        result = list(run_pipeline(slonk, input_data))

        assert len(result) == 3
        assert "john|doe" in result
        assert "jane|smith" in result
        assert "bob|wilson" in result
        assert "invalid_record" not in result

    def test_log_processing_pipeline(self, run_pipeline: PipelineRunner) -> None:
        """Test a log processing pipeline using shell commands."""
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = os.path.join(tmpdir, "access.log")

            # Create sample log file
            log_data = [
                "2024-01-01 10:00:00 INFO User login successful",
                "2024-01-01 10:01:00 ERROR Database connection failed",
                "2024-01-01 10:02:00 INFO User logout successful",
                "2024-01-01 10:03:00 ERROR Invalid authentication",
                "2024-01-01 10:04:00 WARN Low disk space",
            ]

            with open(log_file, "w") as f:
                for line in log_data:
                    f.write(line + "\n")

            # Pipeline: read log file -> filter ERROR lines via shell pipe
            # Note: grep | wc -l is combined in a single shell command because
            # each ShellCommandHandler is a separate subprocess and strips
            # trailing newlines from stdout between stages.
            slonk = Slonk() | log_file | "grep ERROR | wc -l"

            result = list(run_pipeline(slonk))

            # Should count 2 ERROR lines
            assert len(result) == 1
            assert result[0].strip() == "2"

    def test_file_backup_pipeline(self, run_pipeline: PipelineRunner) -> None:
        """Test a file backup and verification pipeline."""
        with tempfile.TemporaryDirectory() as tmpdir:
            source_file = os.path.join(tmpdir, "source.txt")
            backup_file = os.path.join(tmpdir, "backup.txt")

            # Create source file
            with open(source_file, "w") as f:
                f.write("Important data\nLine 2\nLine 3\n")

            def copy_file(data: list[str] | None) -> list[str]:
                """Custom copy function that returns the data."""
                if data:
                    # Write to backup file
                    with open(backup_file, "w") as f:
                        for line in data:
                            f.write(line)
                    return list(data)
                return []

            # Pipeline: read source -> copy to backup -> verify
            slonk = Slonk() | source_file | copy_file | f"diff {source_file} {backup_file}"

            result = list(run_pipeline(slonk))

            # If diff returns empty output, files are identical
            assert len(result) == 1
            assert result[0] == ""  # diff returns empty string for identical files

            # Verify backup file exists and has correct content
            assert os.path.exists(backup_file)
            with open(backup_file) as f:
                backup_content = f.read()
            assert "Important data" in backup_content

    def test_data_aggregation_pipeline(self, run_pipeline: PipelineRunner) -> None:
        """Test data aggregation from multiple sources."""

        def generate_numbers(data: object) -> list[str]:
            """Generate a sequence of numbers."""
            return [str(i) for i in range(1, 6)]  # 1,2,3,4,5

        def calculate_sum(data: list[str] | None) -> list[str]:
            """Calculate sum of numbers."""
            if not data:
                return []
            total = sum(int(line.strip()) for line in data if line.strip().isdigit())
            return [f"Sum: {total}"]

        def calculate_average(data: list[str] | None) -> list[str]:
            """Calculate average of numbers."""
            if not data:
                return []
            numbers = [int(line.strip()) for line in data if line.strip().isdigit()]
            if numbers:
                avg = sum(numbers) / len(numbers)
                return [f"Average: {avg:.1f}"]
            return []

        # Test sum pipeline
        sum_pipeline = Slonk() | generate_numbers | calculate_sum
        sum_result = list(run_pipeline(sum_pipeline))

        assert sum_result == ["Sum: 15"]  # 1+2+3+4+5 = 15

        # Test average pipeline
        avg_pipeline = Slonk() | generate_numbers | calculate_average
        avg_result = list(run_pipeline(avg_pipeline))

        assert avg_result == ["Average: 3.0"]  # (1+2+3+4+5)/5 = 3.0

    def test_configuration_processing_pipeline(self, run_pipeline: PipelineRunner) -> None:
        """Test processing configuration files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_file = os.path.join(tmpdir, "config.ini")

            # Create sample config file
            config_content = [
                "[database]",
                "host=localhost",
                "port=5432",
                "username=admin",
                "[api]",
                "endpoint=https://api.example.com",
                "timeout=30",
                "[logging]",
                "level=INFO",
                "file=/var/log/app.log",
            ]

            with open(config_file, "w") as f:
                for line in config_content:
                    f.write(line + "\n")

            def extract_database_config(data: list[str] | None) -> list[str]:
                """Extract database configuration."""
                if not data:
                    return []

                result = []
                in_database_section = False

                for line in data:
                    line = line.strip()
                    if line == "[database]":
                        in_database_section = True
                        continue
                    elif line.startswith("[") and in_database_section:
                        break
                    elif in_database_section and "=" in line:
                        result.append(line)

                return result

            # Pipeline: read config -> extract database settings
            slonk = Slonk() | config_file | extract_database_config

            result = list(run_pipeline(slonk))

            assert len(result) == 3
            assert "host=localhost" in result
            assert "port=5432" in result
            assert "username=admin" in result

    def test_text_analysis_pipeline(self, run_pipeline: PipelineRunner) -> None:
        """Test text analysis and word counting."""

        def tokenize_words(data: list[str] | None) -> list[str]:
            """Split text into individual words."""
            if not data:
                return []

            words = []
            for line in data:
                # Simple tokenization
                words.extend(line.strip().split())
            return words

        def count_word_lengths(data: list[str] | None) -> list[str]:
            """Count words by length."""
            if not data:
                return []

            length_counts: dict[int, int] = {}
            for word in data:
                length = len(word)
                length_counts[length] = length_counts.get(length, 0) + 1

            result = []
            for length in sorted(length_counts.keys()):
                result.append(f"Length {length}: {length_counts[length]} words")

            return result

        slonk = Slonk() | tokenize_words | count_word_lengths

        input_text = [
            "The quick brown fox jumps over the lazy dog",
            "This is a test of word counting functionality",
        ]

        result = list(run_pipeline(slonk, input_text))

        # Should have counts for different word lengths
        assert len(result) > 0
        assert any("Length 3:" in line for line in result)  # "The", "fox", "dog", etc.
        assert any("Length 4:" in line for line in result)  # "over", "lazy", "test", etc.


class TestPerformanceScenarios:
    """Test performance-related scenarios."""

    def test_large_dataset_processing(self, run_pipeline: PipelineRunner) -> None:
        """Test processing larger datasets efficiently."""

        def process_batch(data: list[str] | None) -> list[str]:
            """Process data in batches."""
            if not data:
                return []

            # Simple processing: add line numbers
            return [f"{i + 1}: {line}" for i, line in enumerate(data)]

        # Create a moderately large dataset
        large_data = [f"data_line_{i}" for i in range(100)]

        slonk = Slonk() | process_batch
        result = list(run_pipeline(slonk, large_data))

        assert len(result) == 100
        assert result[0] == "1: data_line_0"
        assert result[99] == "100: data_line_99"

    def test_streaming_data_simulation(self, run_pipeline: PipelineRunner) -> None:
        """Test handling streaming-like data processing."""

        def process_stream_chunk(data: list[str] | None) -> list[str]:
            """Process data as if it's streaming."""
            if not data:
                return []

            processed = []
            for line in data:
                # Simulate some processing
                if line.strip():
                    processed.append(f"processed: {line.strip()}")

            return processed

        def aggregate_results(data: list[str] | None) -> list[str]:
            """Aggregate processed results."""
            if not data:
                return []

            return [f"Total processed items: {len(data)}"]

        slonk = Slonk() | process_stream_chunk | aggregate_results

        # Simulate streaming chunks
        chunk1 = ["item1", "item2", ""]
        result = list(run_pipeline(slonk, chunk1))

        assert result == ["Total processed items: 2"]
