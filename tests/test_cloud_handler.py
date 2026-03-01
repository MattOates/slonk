from unittest.mock import MagicMock, patch

import pytest

from slonk import CloudPathHandler


class TestCloudPathHandler:
    @patch("slonk.cloudpathlib.CloudPath")
    def test_init(self, mock_cloud_path):
        mock_instance = MagicMock()
        mock_cloud_path.return_value = mock_instance

        handler = CloudPathHandler("s3://bucket/file.txt")

        assert handler.url == "s3://bucket/file.txt"
        assert handler.cloud_path is mock_instance
        mock_cloud_path.assert_called_once_with("s3://bucket/file.txt")

    @patch("slonk.cloudpathlib.CloudPath")
    def test_write(self, mock_cloud_path):
        # Setup mock
        mock_file = MagicMock()
        mock_instance = MagicMock()
        mock_instance.open.return_value.__enter__.return_value = mock_file
        mock_cloud_path.return_value = mock_instance

        handler = CloudPathHandler("s3://bucket/file.txt")
        test_data = ["line1", "line2", "line3"]

        handler.write(test_data)

        # Verify file operations
        mock_instance.open.assert_called_once_with("w")
        assert mock_file.write.call_count == 3
        mock_file.write.assert_any_call("line1\n")
        mock_file.write.assert_any_call("line2\n")
        mock_file.write.assert_any_call("line3\n")

    @patch("slonk.cloudpathlib.CloudPath")
    def test_read(self, mock_cloud_path):
        # Setup mock
        mock_file = MagicMock()
        mock_file.readlines.return_value = ["line1\n", "line2\n", "line3\n"]
        mock_instance = MagicMock()
        mock_instance.open.return_value.__enter__.return_value = mock_file
        mock_cloud_path.return_value = mock_instance

        handler = CloudPathHandler("s3://bucket/file.txt")

        result = handler.read()

        mock_instance.open.assert_called_once_with("r")
        assert result == ["line1\n", "line2\n", "line3\n"]

    @patch("slonk.cloudpathlib.CloudPath")
    def test_process_with_input_data(self, mock_cloud_path):
        # Setup mock for write operation
        mock_file = MagicMock()
        mock_instance = MagicMock()
        mock_instance.open.return_value.__enter__.return_value = mock_file
        mock_cloud_path.return_value = mock_instance

        handler = CloudPathHandler("s3://bucket/file.txt")
        test_data = ["test", "data"]

        result = list(handler.process(test_data))

        # Should return the input data
        assert result == test_data

        # Should also write to cloud path
        mock_instance.open.assert_called_once_with("w")
        assert mock_file.write.call_count == 2
        mock_file.write.assert_any_call("test\n")
        mock_file.write.assert_any_call("data\n")

    @patch("slonk.cloudpathlib.CloudPath")
    def test_process_without_input_data(self, mock_cloud_path):
        # Setup mock for read operation
        mock_file = MagicMock()
        mock_file.readlines.return_value = ["existing\n", "content\n"]
        mock_instance = MagicMock()
        mock_instance.open.return_value.__enter__.return_value = mock_file
        mock_cloud_path.return_value = mock_instance

        handler = CloudPathHandler("s3://bucket/file.txt")

        result = handler.process(None)

        mock_instance.open.assert_called_once_with("r")
        assert result == ["existing\n", "content\n"]

    @patch("slonk.cloudpathlib.CloudPath")
    def test_cloud_path_error_handling(self, mock_cloud_path):
        # Test when cloudpathlib raises an exception
        mock_cloud_path.side_effect = Exception("Cloud service error")

        with pytest.raises(Exception, match="Cloud service error"):
            CloudPathHandler("s3://bucket/file.txt")
