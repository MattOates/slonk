"""Built-in handler classes and callable wrappers for pipeline stages.

This module provides the concrete handler implementations that Slonk
uses when you pipe strings, paths, callables, SQLAlchemy models, or
:func:`parallel` wrappers into a pipeline:

* :class:`PathHandler` — read/write local and remote files via UPath.
* :class:`ShellCommandHandler` — pipe data through shell commands.
* :class:`SQLAlchemyHandler` — query rows from a SQLAlchemy model.
* :class:`_CallableSource`, :class:`_CallableTransform`,
  :class:`_CallableSink` — thin wrappers that adapt plain callables
  to the role-based protocol.
* :class:`_ParallelHandler` / :func:`parallel` — data-parallel
  execution across threads (free-threaded) or processes (GIL).
"""

from __future__ import annotations

import contextlib
import inspect
import subprocess
import threading
from collections.abc import Callable, Iterable
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from typing import Any, get_type_hints

import cloudpickle
from sqlalchemy import select
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker
from upath import UPath

from slonk.base import SlonkBase
from slonk.constants import _is_free_threaded
from slonk.roles import _Role


class PathHandler(SlonkBase):
    """Unified path handler for local and remote filesystems via UPath.

    Implements :class:`~slonk.roles.Source` (read a file),
    :class:`~slonk.roles.Transform` (write to file and pass data through),
    and :class:`~slonk.roles.Sink` (write to file, discard data).

    Args:
        path: A local filesystem path or URI understood by
            `universal-pathlib <https://github.com/fsspec/universal_pathlib>`_
            (e.g. ``"s3://bucket/key"``).

    Examples:
        >>> from slonk.handlers import PathHandler
        >>> h = PathHandler("/tmp/example.txt")
        >>> str(h.upath)
        '/tmp/example.txt'
    """

    def __init__(self, path: str) -> None:
        self.upath = UPath(path)

    # -- Source ------------------------------------------------------------

    def process_source(self) -> Iterable[str]:
        """Read lines from the file.

        Returns a generator so items stream lazily in parallel mode.
        """
        with self.upath.open("r") as file:
            yield from file

    # -- Transform ---------------------------------------------------------

    def process_transform(self, input_data: Iterable[str]) -> Iterable[str]:
        """Write input to file and pass each item through.

        Args:
            input_data: Items to write and forward.

        Yields:
            Each item unchanged after writing.
        """
        with self.upath.open("w") as file:
            for line in input_data:
                file.write(line + "\n")
                yield line

    # -- Sink --------------------------------------------------------------

    def process_sink(self, input_data: Iterable[str]) -> None:
        """Write input to file (final stage, no passthrough).

        Args:
            input_data: Items to write.
        """
        with self.upath.open("w") as file:
            for line in input_data:
                file.write(line + "\n")

    # -- legacy convenience methods ----------------------------------------

    def write(self, data: Iterable[str]) -> None:
        """Write *data* to the file (legacy helper).

        Args:
            data: Lines to write.
        """
        with self.upath.open("w") as file:
            for line in data:
                file.write(line + "\n")

    def read(self) -> Iterable[str]:
        """Read all lines from the file (legacy helper).

        Returns:
            A list of lines.
        """
        with self.upath.open("r") as file:
            return file.readlines()


class ShellCommandHandler(SlonkBase):
    """Runs a shell command, piping data through stdin/stdout.

    Implements :class:`~slonk.roles.Transform` — stdin is fed from the
    input iterable via a writer thread, and stdout lines are yielded as
    they arrive.

    Args:
        command: The shell command string to execute.

    Examples:
        >>> from slonk.handlers import ShellCommandHandler
        >>> h = ShellCommandHandler("sort")
        >>> h.command
        'sort'
    """

    def __init__(self, command: str) -> None:
        self.command = command

    # -- Transform ---------------------------------------------------------

    def process_transform(self, input_data: Iterable[str]) -> Iterable[str]:
        """Pipe *input_data* through the shell command, yielding stdout lines.

        A writer thread feeds stdin so that the command can begin producing
        output before all input has been consumed (true streaming for
        commands like ``grep``).  Commands that buffer (like ``sort``)
        naturally hold output until EOF on stdin.

        Args:
            input_data: Items to write to the command's stdin.

        Yields:
            Non-empty lines from the command's stdout.

        Raises:
            RuntimeError: If the command exits with a non-zero return code.
        """
        proc = subprocess.Popen(
            self.command,
            shell=True,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        writer_error: BaseException | None = None

        def _feed_stdin() -> None:
            """Write input items to the process's stdin, then close it."""
            nonlocal writer_error
            try:
                assert proc.stdin is not None
                for item in input_data:
                    proc.stdin.write((item + "\n").encode())
                proc.stdin.close()
            except BaseException as exc:
                writer_error = exc
                if proc.stdin is not None:
                    with contextlib.suppress(OSError):
                        proc.stdin.close()

        writer = threading.Thread(target=_feed_stdin, daemon=True)
        writer.start()

        # Yield stdout lines as they arrive.
        assert proc.stdout is not None
        for raw_line in proc.stdout:
            line = raw_line.decode().rstrip("\n")
            if line:
                yield line

        writer.join()
        proc.wait()

        if writer_error is not None:
            raise writer_error

        if proc.returncode != 0:
            assert proc.stderr is not None
            stderr = proc.stderr.read().decode()
            raise RuntimeError(f"Command failed with error: {stderr}")

    # -- legacy helper used by tests ---------------------------------------

    def _run_command(self, input_string: str) -> str:
        """Run the command synchronously with *input_string* on stdin.

        Args:
            input_string: Data to send to the command's stdin.

        Returns:
            The stripped stdout output.

        Raises:
            RuntimeError: If the command exits with a non-zero return code.
        """
        process = subprocess.Popen(
            self.command,
            shell=True,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        stdout, stderr = process.communicate(input=input_string.encode())
        if process.returncode != 0:
            raise RuntimeError(f"Command failed with error: {stderr.decode()}")
        return stdout.decode().strip()


class SQLAlchemyHandler(SlonkBase):
    """Queries all rows from a SQLAlchemy model and formats them as strings.

    Implements :class:`~slonk.roles.Source` — uses ``yield_per()`` to fetch
    rows in chunks, yielding formatted strings one at a time for natural
    streaming.

    Args:
        model: A SQLAlchemy declarative model class.
        session_factory: A :class:`~sqlalchemy.orm.sessionmaker` used to
            create database sessions.
    """

    _YIELD_PER_CHUNK = 100

    def __init__(
        self, model: type[DeclarativeBase], session_factory: sessionmaker[Session]
    ) -> None:
        self.model = model
        self.session_factory = session_factory

    # -- Source ------------------------------------------------------------

    def process_source(self) -> Iterable[str]:
        """Yield formatted rows using chunked fetching via ``yield_per``.

        Each row is formatted as ``"<id>\\t<data>"``.

        Yields:
            Tab-separated id and data for each row.
        """
        session = self.session_factory()
        try:
            stmt = select(self.model)
            for record in session.execute(stmt).yield_per(self._YIELD_PER_CHUNK).scalars():
                yield f"{record.id}\t{record.data}"  # type: ignore[attr-defined]
        finally:
            session.close()


# ---------------------------------------------------------------------------
# Callable handler wrappers (inferred from function signature)
# ---------------------------------------------------------------------------


def _infer_callable_role(func: Any) -> _Role:
    """Inspect *func*'s type hints to determine its pipeline role.

    Rules:

    - No parameters (or only ``self``) and returns something -> Source
    - Accepts an input parameter and returns ``None`` -> Sink
    - Accepts an input parameter and returns something -> Transform (default)

    Args:
        func: The callable to inspect.

    Returns:
        The inferred :class:`~slonk.roles._Role`.

    Examples:
        >>> from slonk.handlers import _infer_callable_role
        >>> from slonk.roles import _Role
        >>> def my_source() -> list[str]:
        ...     return ["a"]
        >>> _infer_callable_role(my_source) == _Role.SOURCE
        True
        >>> def my_sink(data: list[str]) -> None:
        ...     pass
        >>> _infer_callable_role(my_sink) == _Role.SINK
        True
        >>> def my_transform(data: list[str]) -> list[str]:
        ...     return data
        >>> _infer_callable_role(my_transform) == _Role.TRANSFORM
        True
    """
    try:
        hints = get_type_hints(func)
    except Exception:
        hints = {}

    sig = inspect.signature(func)
    params = [
        p
        for p in sig.parameters.values()
        if p.name != "self" and p.kind not in (p.VAR_POSITIONAL, p.VAR_KEYWORD)
    ]

    has_input = len(params) > 0
    return_hint = hints.get("return", inspect.Parameter.empty)

    if not has_input:
        return _Role.SOURCE

    if return_hint is type(None):
        return _Role.SINK

    return _Role.TRANSFORM


class _CallableSource(SlonkBase):
    """Wraps a no-argument callable as a :class:`~slonk.roles.Source`.

    Args:
        func: A callable that takes no arguments and returns an iterable
            of strings.

    Examples:
        >>> from slonk.handlers import _CallableSource
        >>> src = _CallableSource(lambda: ["x", "y"])
        >>> list(src.process_source())
        ['x', 'y']
    """

    def __init__(self, func: Any) -> None:
        self.func = func

    def process_source(self) -> Iterable[str]:
        """Invoke the wrapped callable and return its result."""
        result: Iterable[str] = self.func()
        return result


class _CallableTransform(SlonkBase):
    """Wraps a callable as a :class:`~slonk.roles.Transform`.

    Args:
        func: A callable that takes an iterable of strings and returns
            an iterable of strings.

    Examples:
        >>> from slonk.handlers import _CallableTransform
        >>> t = _CallableTransform(lambda data: [s.upper() for s in data])
        >>> list(t.process_transform(["hello"]))
        ['HELLO']
    """

    def __init__(self, func: Any) -> None:
        self.func = func

    def process_transform(self, input_data: Iterable[str]) -> Iterable[str]:
        """Invoke the wrapped callable with *input_data*."""
        result: Iterable[str] = self.func(input_data)
        return result


class _CallableSink(SlonkBase):
    """Wraps a callable that returns ``None`` as a :class:`~slonk.roles.Sink`.

    Args:
        func: A callable that takes an iterable of strings and returns
            ``None``.

    Examples:
        >>> collected = []
        >>> from slonk.handlers import _CallableSink
        >>> sink = _CallableSink(lambda data: collected.extend(data))
        >>> sink.process_sink(["a", "b"])
        >>> collected
        ['a', 'b']
    """

    def __init__(self, func: Any) -> None:
        self.func = func

    def process_sink(self, input_data: Iterable[str]) -> None:
        """Invoke the wrapped callable with *input_data*."""
        self.func(input_data)


def _wrap_callable(func: Any) -> _CallableSource | _CallableTransform | _CallableSink:
    """Create the appropriate callable wrapper based on signature inference.

    Args:
        func: The callable to wrap.

    Returns:
        A :class:`_CallableSource`, :class:`_CallableTransform`, or
        :class:`_CallableSink` instance.

    Examples:
        >>> from slonk.handlers import _wrap_callable, _CallableTransform
        >>> wrapper = _wrap_callable(lambda data: [s.upper() for s in data])
        >>> isinstance(wrapper, _CallableTransform)
        True
    """
    role = _infer_callable_role(func)
    if role is _Role.SOURCE:
        return _CallableSource(func)
    if role is _Role.SINK:
        return _CallableSink(func)
    return _CallableTransform(func)


# ---------------------------------------------------------------------------
# Parallel data-parallelism wrapper
# ---------------------------------------------------------------------------


class _ParallelHandler(SlonkBase):
    """Splits input into chunks and processes them across a pool of workers.

    Implements :class:`~slonk.roles.Transform`.

    On free-threaded Python a :class:`~concurrent.futures.ThreadPoolExecutor`
    is used (no serialisation overhead).  On standard Python a
    :class:`~concurrent.futures.ProcessPoolExecutor` is used with
    ``cloudpickle`` for serialising the callable.

    Args:
        func: The callable to apply to each chunk.
        workers: Number of parallel workers.
        chunk_size: Maximum items per chunk.
    """

    def __init__(
        self,
        func: Callable[..., Iterable[str]],
        workers: int = 4,
        chunk_size: int = 100,
    ) -> None:
        self.func = func
        self.workers = workers
        self.chunk_size = chunk_size

    def process_transform(self, input_data: Iterable[str]) -> Iterable[str]:
        """Split input into chunks and process in parallel.

        Args:
            input_data: Items to distribute across workers.

        Returns:
            Concatenated results from all workers, preserving chunk order.
        """
        items = list(input_data)
        if not items:
            return self.func([])

        # Split into chunks.
        chunks: list[list[str]] = [
            items[i : i + self.chunk_size] for i in range(0, len(items), self.chunk_size)
        ]

        if len(chunks) == 1:
            return self.func(items)

        if _is_free_threaded():
            return self._run_threaded(chunks)
        return self._run_multiprocess(chunks)

    def _run_threaded(self, chunks: list[list[str]]) -> list[str]:
        """Execute chunks using threads (free-threaded Python)."""
        results: list[str] = []
        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            futures = [pool.submit(self.func, chunk) for chunk in chunks]
            for future in futures:
                results.extend(future.result())
        return results

    def _run_multiprocess(self, chunks: list[list[str]]) -> list[str]:
        """Execute chunks using processes with cloudpickle serialisation."""
        pickled_func = cloudpickle.dumps(self.func)

        results: list[str] = []
        with ProcessPoolExecutor(max_workers=self.workers) as pool:
            futures = [pool.submit(_unpickle_and_call, pickled_func, chunk) for chunk in chunks]
            for future in futures:
                results.extend(future.result())
        return results


def _unpickle_and_call(pickled_func: bytes, chunk: list[str]) -> list[str]:
    """Deserialise a cloudpickled callable and invoke it (runs in worker process).

    Args:
        pickled_func: The cloudpickle-serialised callable bytes.
        chunk: The list of string items to process.

    Returns:
        The result of calling the unpickled function on *chunk*.
    """
    func = cloudpickle.loads(pickled_func)
    return list(func(chunk))


def parallel(
    func: Callable[..., Iterable[str]],
    workers: int = 4,
    chunk_size: int = 100,
) -> _ParallelHandler:
    """Wrap a callable for data-parallel execution across a worker pool.

    Args:
        func: The transform callable to parallelise.
        workers: Number of parallel workers (default 4).
        chunk_size: Maximum items per chunk (default 100).

    Returns:
        A :class:`_ParallelHandler` that can be piped into a pipeline.

    Examples:
        >>> from slonk.handlers import parallel
        >>> handler = parallel(lambda data: [s.upper() for s in data], workers=2)
        >>> handler.workers
        2
    """
    return _ParallelHandler(func, workers=workers, chunk_size=chunk_size)
