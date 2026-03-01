"""Shared constants and runtime helpers for the slonk pipeline library.

This module defines sentinel values, default configuration, and a helper
for detecting free-threaded Python builds at runtime.

Examples:
    >>> from slonk.constants import _DONE, _DEFAULT_MAX_QUEUE_SIZE
    >>> _DONE is not None
    True
    >>> _DEFAULT_MAX_QUEUE_SIZE
    1024
"""

from __future__ import annotations

import sys

# Known UPath/fsspec protocols — any URI with one of these schemes is treated as a path.
_KNOWN_PATH_PROTOCOLS = frozenset(
    {
        "file",
        "local",
        "memory",
        "s3",
        "s3a",
        "gs",
        "gcs",
        "az",
        "adl",
        "abfs",
        "abfss",
        "ftp",
        "sftp",
        "ssh",
        "http",
        "https",
        "hdfs",
        "smb",
        "github",
        "hf",
        "webdav",
        "webdav+http",
        "webdav+https",
        "data",
    }
)

# Sentinel signalling end-of-stream between threaded stages.
_DONE = object()

# Default bounded-queue size for streaming pipeline backpressure.
_DEFAULT_MAX_QUEUE_SIZE = 1024


def _is_free_threaded() -> bool:
    """Return ``True`` when running on a free-threaded (no-GIL) Python build.

    On free-threaded builds ``sys._is_gil_enabled()`` returns ``False``.
    On standard builds the function either doesn't exist or returns ``True``.

    Examples:
        >>> from slonk.constants import _is_free_threaded
        >>> isinstance(_is_free_threaded(), bool)
        True
    """
    try:
        return not sys._is_gil_enabled()  # type: ignore[attr-defined]
    except AttributeError:
        return False
