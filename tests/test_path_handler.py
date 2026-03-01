from unittest.mock import MagicMock, patch

import pytest

from slonk import PathHandler


class TestPathHandler:
    @patch("slonk.UPath")
    def test_init_cloud_path(self, mock_upath: MagicMock) -> None:
        mock_instance = MagicMock()
        mock_upath.return_value = mock_instance

        handler = PathHandler("s3://bucket/file.txt")

        assert handler.upath is mock_instance
        mock_upath.assert_called_once_with("s3://bucket/file.txt")

    @patch("slonk.UPath")
    def test_init_local_path(self, mock_upath: MagicMock) -> None:
        mock_instance = MagicMock()
        mock_upath.return_value = mock_instance

        handler = PathHandler("/tmp/local/file.txt")

        assert handler.upath is mock_instance
        mock_upath.assert_called_once_with("/tmp/local/file.txt")

    @patch("slonk.UPath")
    def test_write(self, mock_upath: MagicMock) -> None:
        mock_file = MagicMock()
        mock_instance = MagicMock()
        mock_instance.open.return_value.__enter__.return_value = mock_file
        mock_upath.return_value = mock_instance

        handler = PathHandler("s3://bucket/file.txt")
        test_data = ["line1", "line2", "line3"]

        handler.write(test_data)

        mock_instance.open.assert_called_once_with("w")
        assert mock_file.write.call_count == 3
        mock_file.write.assert_any_call("line1\n")
        mock_file.write.assert_any_call("line2\n")
        mock_file.write.assert_any_call("line3\n")

    @patch("slonk.UPath")
    def test_read(self, mock_upath: MagicMock) -> None:
        mock_file = MagicMock()
        mock_file.readlines.return_value = ["line1\n", "line2\n", "line3\n"]
        mock_instance = MagicMock()
        mock_instance.open.return_value.__enter__.return_value = mock_file
        mock_upath.return_value = mock_instance

        handler = PathHandler("s3://bucket/file.txt")

        result = handler.read()

        mock_instance.open.assert_called_once_with("r")
        assert result == ["line1\n", "line2\n", "line3\n"]

    @patch("slonk.UPath")
    def test_process_transform(self, mock_upath: MagicMock) -> None:
        mock_file = MagicMock()
        mock_instance = MagicMock()
        mock_instance.open.return_value.__enter__.return_value = mock_file
        mock_upath.return_value = mock_instance

        handler = PathHandler("s3://bucket/file.txt")
        test_data = ["test", "data"]

        result = list(handler.process_transform(test_data))

        # Should return the input data (passthrough)
        assert result == test_data

        # Should also write to path
        mock_instance.open.assert_called_once_with("w")
        assert mock_file.write.call_count == 2
        mock_file.write.assert_any_call("test\n")
        mock_file.write.assert_any_call("data\n")

    @patch("slonk.UPath")
    def test_process_source(self, mock_upath: MagicMock) -> None:
        mock_file = MagicMock()
        # process_source uses `yield from file` so mock __iter__
        mock_file.__iter__ = MagicMock(return_value=iter(["existing\n", "content\n"]))
        mock_instance = MagicMock()
        mock_instance.open.return_value.__enter__.return_value = mock_file
        mock_upath.return_value = mock_instance

        handler = PathHandler("s3://bucket/file.txt")

        result = list(handler.process_source())

        mock_instance.open.assert_called_once_with("r")
        assert result == ["existing\n", "content\n"]

    @patch("slonk.UPath")
    def test_process_sink(self, mock_upath: MagicMock) -> None:
        mock_file = MagicMock()
        mock_instance = MagicMock()
        mock_instance.open.return_value.__enter__.return_value = mock_file
        mock_upath.return_value = mock_instance

        handler = PathHandler("s3://bucket/file.txt")
        test_data = ["sink", "data"]

        result = handler.process_sink(test_data)

        assert result is None
        mock_instance.open.assert_called_once_with("w")
        assert mock_file.write.call_count == 2
        mock_file.write.assert_any_call("sink\n")
        mock_file.write.assert_any_call("data\n")

    @patch("slonk.UPath")
    def test_path_error_handling(self, mock_upath: MagicMock) -> None:
        mock_upath.side_effect = Exception("Filesystem error")

        with pytest.raises(Exception, match="Filesystem error"):
            PathHandler("s3://bucket/file.txt")

    @patch("slonk.UPath")
    def test_init_ftp_path(self, mock_upath: MagicMock) -> None:
        """UPath supports ftp:// and other protocols beyond cloud storage."""
        mock_instance = MagicMock()
        mock_upath.return_value = mock_instance

        handler = PathHandler("ftp://server/file.txt")

        assert handler.upath is mock_instance
        mock_upath.assert_called_once_with("ftp://server/file.txt")

    @patch("slonk.UPath")
    def test_init_http_path(self, mock_upath: MagicMock) -> None:
        mock_instance = MagicMock()
        mock_upath.return_value = mock_instance

        handler = PathHandler("https://example.com/data.csv")

        assert handler.upath is mock_instance
        mock_upath.assert_called_once_with("https://example.com/data.csv")
