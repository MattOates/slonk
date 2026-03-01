import contextlib
import subprocess
import sys
import threading
from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from queue import Queue
from typing import Any, Protocol, Union, runtime_checkable

import cloudpickle
from sqlalchemy import Column, String, create_engine, select
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker
from upath import UPath

__all__ = [
    "Base",
    "ExampleModel",
    "Handler",
    "PathHandler",
    "SQLAlchemyHandler",
    "ShellCommandHandler",
    "Slonk",
    "StreamingHandler",
    "TeeHandler",
    "parallel",
    "tee",
]

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

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

# Sentinel placed *before* _DONE when the original input_data was None.
# Allows streaming stages to distinguish "no input" from "empty input".
_NONE_INPUT = object()

# Default bounded-queue size for streaming pipeline backpressure.
_DEFAULT_MAX_QUEUE_SIZE = 1024


# ---------------------------------------------------------------------------
# Free-threading detection
# ---------------------------------------------------------------------------


def _is_free_threaded() -> bool:
    """Return True when running on a free-threaded (no-GIL) Python build.

    On free-threaded builds ``sys._is_gil_enabled()`` returns ``False``.
    On standard builds the function either doesn't exist or returns ``True``.
    """
    try:
        return not sys._is_gil_enabled()  # type: ignore[attr-defined]
    except AttributeError:
        return False


# ---------------------------------------------------------------------------
# SQLAlchemy base / example model
# ---------------------------------------------------------------------------


class Base(DeclarativeBase):
    pass


class ExampleModel(Base):
    __tablename__ = "example"
    id = Column(String, primary_key=True)
    data = Column(String)


# ---------------------------------------------------------------------------
# Handler protocols
# ---------------------------------------------------------------------------


@runtime_checkable
class Handler(Protocol):
    def process(self, input_data: Iterable[str] | None) -> Iterable[str]: ...


@runtime_checkable
class StreamingHandler(Handler, Protocol):
    """A handler that can process items lazily via an iterator.

    ``process_stream`` receives a lazy iterable (which may be backed by a
    queue) and yields result items one at a time.  The pipeline pushes each
    yielded item to the next stage immediately, enabling true item-level
    streaming and backpressure.

    Every ``StreamingHandler`` must also implement ``process`` for use by
    the synchronous ``run_sync()`` path.  A simple implementation is::

        def process(self, input_data):
            return list(self.process_stream(input_data))
    """

    def process_stream(self, input_stream: Iterable[str] | None) -> Iterator[str]: ...


# ---------------------------------------------------------------------------
# Queue iteration helpers
# ---------------------------------------------------------------------------


def _queue_iter(q: Queue[Any]) -> Iterator[str]:
    """Yield items from *q* until the ``_DONE`` sentinel is received.

    The ``_NONE_INPUT`` sentinel (if present) is silently skipped so that
    callers always see a clean stream of data items.
    """
    while True:
        item = q.get()
        if item is _DONE:
            return
        if item is _NONE_INPUT:
            continue
        yield item


class _QueueDrainState:
    """Mutable flag shared between ``_queue_to_stream_input`` and ``_run_stage``.

    Set to ``True`` once the ``_DONE`` sentinel has been consumed from the
    input queue, so the error handler in ``_run_stage`` knows not to call
    ``_drain_queue`` on an already-empty queue (which would deadlock).
    """

    __slots__ = ("done",)

    def __init__(self) -> None:
        self.done = False


def _queue_to_stream_input(
    q: Queue[Any],
    state: _QueueDrainState,
) -> Iterable[str] | None:
    """Read the input queue and return either ``None`` or a lazy iterator.

    Returns ``None`` when the original ``input_data`` was ``None``
    (signalled by the ``_NONE_INPUT`` sentinel), allowing streaming
    handlers to distinguish "no input" from "empty input".
    Otherwise returns an iterator that lazily drains the queue.

    *state.done* is set to ``True`` once ``_DONE`` has been consumed
    (immediately for ``None``/empty-input cases, or lazily once the
    returned iterator is exhausted).
    """
    first = q.get()

    # Original input was None.
    if first is _NONE_INPUT:
        # Consume the trailing _DONE.
        q.get()  # _DONE
        state.done = True
        return None

    # Empty input (just _DONE, no data items).
    if first is _DONE:
        state.done = True
        return iter(())  # empty iterator, not None

    # Normal: chain the first item with the rest of the queue.
    def _iter() -> Iterator[str]:
        yield first
        while True:
            item = q.get()
            if item is _DONE:
                state.done = True
                return
            if item is _NONE_INPUT:
                continue
            yield item

    return _iter()


def _drain_queue(q: Queue[Any], *, already_done: bool = False) -> None:
    """Consume all remaining items from *q* (including ``_DONE``).

    Used to unblock upstream stages when a downstream stage errors
    mid-stream.

    When *already_done* is ``True`` the queue has already had its
    ``_DONE`` sentinel consumed and is known to be empty, so this is
    a no-op.
    """
    if already_done:
        return
    while True:
        item = q.get()
        if item is _DONE:
            return


# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------


class PathHandler:
    """Unified path handler for local and remote filesystems via UPath.

    Implements ``StreamingHandler`` — file reads yield lines lazily and
    writes stream items to disk as they arrive.
    """

    def __init__(self, path: str) -> None:
        self.upath = UPath(path)

    # -- batch (Handler) ---------------------------------------------------

    def process(self, input_data: Iterable[str] | None) -> Iterable[str]:
        if input_data is not None:
            self.write(input_data)
            return input_data
        else:
            return self.read()

    # -- streaming (StreamingHandler) --------------------------------------

    def process_stream(self, input_stream: Iterable[str] | None) -> Iterator[str]:
        if input_stream is not None:
            yield from self._write_stream(input_stream)
        else:
            yield from self._read_stream()

    def _write_stream(self, data: Iterable[str]) -> Iterator[str]:
        """Write items to the file, yielding each item as pass-through."""
        with self.upath.open("w") as file:
            for line in data:
                file.write(line + "\n")
                yield line

    def _read_stream(self) -> Iterator[str]:
        """Yield lines from the file one at a time.

        Lines include trailing newlines to match the batch ``read()`` output
        (``file.readlines()``), ensuring that pipelines produce identical
        results regardless of sync vs parallel mode.
        """
        with self.upath.open("r") as file:
            yield from file

    # -- legacy convenience methods ----------------------------------------

    def write(self, data: Iterable[str]) -> None:
        with self.upath.open("w") as file:
            for line in data:
                file.write(line + "\n")

    def read(self) -> Iterable[str]:
        with self.upath.open("r") as file:
            return file.readlines()


class ShellCommandHandler:
    """Runs a shell command, piping data through stdin/stdout.

    Implements ``StreamingHandler`` — stdin is fed from the input stream
    via a writer thread, and stdout lines are yielded as they arrive.
    The shell command itself decides how much to buffer (e.g. ``grep``
    streams line-by-line, ``sort`` buffers until EOF).
    """

    def __init__(self, command: str) -> None:
        self.command = command

    # -- batch (Handler) ---------------------------------------------------

    def process(self, input_data: Iterable[str] | None) -> Iterable[str]:
        if input_data is not None:
            input_string = "\n".join(input_data)
            return [self._run_command(input_string)]
        else:
            return []

    # -- streaming (StreamingHandler) --------------------------------------

    def process_stream(self, input_stream: Iterable[str] | None) -> Iterator[str]:
        if input_stream is None:
            return

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
                for item in input_stream:
                    proc.stdin.write((item + "\n").encode())
                proc.stdin.close()
            except BaseException as exc:
                writer_error = exc
                # Still close stdin so the process doesn't hang.
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

    # -- legacy helper (used by batch process) -----------------------------

    def _run_command(self, input_string: str) -> str:
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


class SQLAlchemyHandler:
    """Queries all rows from a SQLAlchemy model and formats them as strings.

    Implements ``StreamingHandler`` — uses ``yield_per()`` to fetch rows
    in chunks from the database, yielding formatted strings one at a time
    instead of materialising the entire result set in memory.
    """

    _YIELD_PER_CHUNK = 100

    def __init__(self, model: type[Base], session_factory: sessionmaker[Session]) -> None:
        self.model = model
        self.session_factory = session_factory

    # -- batch (Handler) ---------------------------------------------------

    def process(self, input_data: Iterable[Any] | None) -> Iterable[str]:
        session = self.session_factory()
        try:
            records = session.query(self.model).all()
            return [f"{record.id}\t{record.data}" for record in records]  # type: ignore[attr-defined]
        finally:
            session.close()

    # -- streaming (StreamingHandler) --------------------------------------

    def process_stream(self, input_stream: Iterable[str] | None) -> Iterator[str]:
        """Yield formatted rows using chunked fetching via ``yield_per``."""
        session = self.session_factory()
        try:
            stmt = select(self.model)
            for record in session.execute(stmt).yield_per(self._YIELD_PER_CHUNK).scalars():
                yield f"{record.id}\t{record.data}"  # type: ignore[attr-defined]
        finally:
            session.close()


class _CallableHandler:
    """Wraps a plain callable so it conforms to the Handler protocol."""

    def __init__(self, func: Any) -> None:
        self.func = func

    def process(self, input_data: Iterable[str] | None) -> Iterable[str]:
        result: Iterable[str] = self.func(input_data)
        return result


class TeeHandler:
    """Passes input through unchanged while also running a side pipeline.

    In parallel mode the side pipeline executes concurrently in a thread.
    """

    def __init__(self, pipeline: Slonk) -> None:
        self.pipeline = pipeline

    def process(self, input_data: Iterable[str] | None) -> Iterable[str]:
        """Synchronous: run side pipeline inline."""
        results: list[str] = []
        if input_data is not None:
            results.extend(input_data)
            results.extend(self.pipeline.run_sync(input_data))
        return results

    def process_parallel(self, input_data: Iterable[str] | None) -> Iterable[str]:
        """Parallel: run the side pipeline concurrently in a thread."""
        if input_data is None:
            return []

        # Materialise so both branches can iterate independently.
        data = list(input_data)

        side_result: list[str] = []
        error: BaseException | None = None

        def _run_side() -> None:
            nonlocal side_result, error
            try:
                side_result = list(self.pipeline.run_sync(data))
            except BaseException as exc:
                error = exc

        t = threading.Thread(target=_run_side, daemon=True)
        t.start()
        t.join()

        if error is not None:
            raise error

        results: list[str] = list(data)
        results.extend(side_result)
        return results


# ---------------------------------------------------------------------------
# Parallel data-parallelism wrapper
# ---------------------------------------------------------------------------


class _ParallelHandler:
    """Splits input into chunks and processes them across a pool of workers.

    On free-threaded Python a ``ThreadPoolExecutor`` is used (no serialisation
    overhead).  On standard Python a ``ProcessPoolExecutor`` is used with
    ``cloudpickle`` for serialising the callable.
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

    def process(self, input_data: Iterable[str] | None) -> Iterable[str]:
        if input_data is None:
            return self.func(None)

        items = list(input_data)
        if not items:
            return self.func([])

        # Split into chunks.
        chunks: list[list[str]] = [
            items[i : i + self.chunk_size] for i in range(0, len(items), self.chunk_size)
        ]

        if len(chunks) == 1:
            # Not worth parallelising a single chunk.
            return self.func(items)

        if _is_free_threaded():
            return self._run_threaded(chunks)
        return self._run_multiprocess(chunks)

    def _run_threaded(self, chunks: list[list[str]]) -> list[str]:
        results: list[str] = []
        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            futures = [pool.submit(self.func, chunk) for chunk in chunks]
            for future in futures:
                results.extend(future.result())
        return results

    def _run_multiprocess(self, chunks: list[list[str]]) -> list[str]:
        # Serialise the callable with cloudpickle so that lambdas/closures work.
        pickled_func = cloudpickle.dumps(self.func)

        results: list[str] = []
        with ProcessPoolExecutor(max_workers=self.workers) as pool:
            futures = [pool.submit(_unpickle_and_call, pickled_func, chunk) for chunk in chunks]
            for future in futures:
                results.extend(future.result())
        return results


def _unpickle_and_call(pickled_func: bytes, chunk: list[str]) -> list[str]:
    """Deserialise a cloudpickled callable and invoke it (runs in worker process)."""
    func = cloudpickle.loads(pickled_func)
    return list(func(chunk))


def parallel(
    func: Callable[..., Iterable[str]],
    workers: int = 4,
    chunk_size: int = 100,
) -> _ParallelHandler:
    """Wrap a callable for data-parallel execution across a worker pool.

    Usage::

        pipeline = Slonk() | parallel(my_transform, workers=8, chunk_size=50)
    """
    return _ParallelHandler(func, workers=workers, chunk_size=chunk_size)


# ---------------------------------------------------------------------------
# Streaming pipeline internals
# ---------------------------------------------------------------------------


class _StreamingPipeline:
    """Executes pipeline stages concurrently via threads connected by bounded queues.

    Each stage runs in its own thread.  The first stage pulls from an input
    queue (pre-populated by the caller), and the last stage pushes to an output
    queue that the caller drains.  The sentinel ``_DONE`` signals end-of-stream.

    Bounded queues provide backpressure: a fast producer blocks when the
    downstream consumer hasn't consumed enough yet.
    """

    def __init__(
        self,
        stages: list[StageType],
        max_queue_size: int = _DEFAULT_MAX_QUEUE_SIZE,
    ) -> None:
        self.stages = stages
        self.max_queue_size = max_queue_size

    def execute(self, input_data: Iterable[Any] | None) -> list[str]:
        if not self.stages:
            return list(input_data) if input_data is not None else []

        # Build N+1 queues for N stages.
        queues: list[Queue[Any]] = [
            Queue(maxsize=self.max_queue_size) for _ in range(len(self.stages) + 1)
        ]

        # Collect errors from stage threads.
        errors: list[BaseException] = []
        lock = threading.Lock()

        # Start stage threads *before* seeding so that bounded queues don't
        # deadlock when max_queue_size < len(input_data).
        threads: list[threading.Thread] = []
        for i, stage in enumerate(self.stages):
            t = threading.Thread(
                target=self._run_stage,
                args=(stage, queues[i], queues[i + 1], errors, lock),
                daemon=True,
            )
            threads.append(t)
            t.start()

        # Seed the first queue with input data.
        if input_data is not None:
            for item in input_data:
                queues[0].put(item)
        else:
            queues[0].put(_NONE_INPUT)
        queues[0].put(_DONE)

        # Drain the final output queue.
        output: list[str] = []
        while True:
            item = queues[-1].get()
            if item is _DONE:
                break
            output.append(item)

        # Wait for all threads to finish.
        for t in threads:
            t.join()

        # Re-raise the first error encountered.
        if errors:
            raise errors[0]

        return output

    @staticmethod
    def _run_stage(
        stage: StageType,
        in_q: Queue[Any],
        out_q: Queue[Any],
        errors: list[BaseException],
        lock: threading.Lock,
    ) -> None:
        """Worker: read from *in_q*, run the stage, push results to *out_q*.

        Dispatch order:

        1. ``StreamingHandler`` — the stage receives a lazy iterator (or
           ``None``) over the input queue and yields results one at a
           time.  This is the true streaming path.
        2. ``TeeHandler`` — special handling for concurrent side-pipelines.
        3. ``Slonk`` (sub-pipeline) — runs synchronously on the
           materialised input.
        4. Any other ``Handler`` — batch: materialise input, call
           ``process()``, iterate the result (generators stream naturally).
        """
        # Shared flag — set to True once _DONE has been consumed from in_q,
        # either eagerly (batch path) or lazily (streaming iterator,
        # possibly in a child thread like ShellCommandHandler's writer).
        drain_state = _QueueDrainState()
        try:
            if isinstance(stage, StreamingHandler):
                # True streaming: pass None or a lazy queue iterator.
                input_stream = _queue_to_stream_input(in_q, drain_state)
                for item in stage.process_stream(input_stream):
                    out_q.put(item)
            else:
                # Batch path: materialise all input from the queue first.
                items: list[Any] = list(_queue_iter(in_q))
                drain_state.done = True  # _queue_iter consumed through _DONE
                input_data: list[Any] | None = items or None

                if isinstance(stage, Slonk):
                    result = stage.run_sync(input_data)
                elif isinstance(stage, TeeHandler):
                    result = stage.process_parallel(input_data)
                else:
                    result = stage.process(input_data)

                if result is not None:
                    for r in result:
                        out_q.put(r)
        except BaseException as exc:
            # Drain remaining input so upstream stages don't block forever
            # on a full queue — but only if we haven't already consumed
            # everything (otherwise we'd block on an empty queue).
            _drain_queue(in_q, already_done=drain_state.done)
            with lock:
                errors.append(exc)
        finally:
            out_q.put(_DONE)


# ---------------------------------------------------------------------------
# Core pipeline
# ---------------------------------------------------------------------------

StageType = Union[Handler, "Slonk"]


class Slonk:
    def __init__(self, session_factory: sessionmaker[Session] | None = None) -> None:
        self.stages: list[StageType] = []
        self.session_factory = session_factory

    def __or__(
        self,
        other: str | Slonk | type[Base] | Handler | _ParallelHandler | Any,
    ) -> Slonk:
        if isinstance(other, _ParallelHandler):
            self.stages.append(other)  # type: ignore[arg-type]
        elif isinstance(other, str):
            if self._is_path(other):
                self.stages.append(PathHandler(other))
            else:
                self.stages.append(ShellCommandHandler(other))
        elif isinstance(other, Slonk):
            self.stages.append(other)
        elif isinstance(other, type) and issubclass(other, Base):
            if self.session_factory is None:
                raise ValueError(
                    "Cannot use SQLAlchemy models without a session_factory. "
                    "Pass session_factory to Slonk()."
                )
            self.stages.append(SQLAlchemyHandler(other, self.session_factory))
        elif callable(other):
            self.stages.append(_CallableHandler(other))
        else:
            raise TypeError(f"Unsupported type: {type(other)}")
        return self

    # ------------------------------------------------------------------
    # Public execution API
    # ------------------------------------------------------------------

    def run(
        self,
        input_data: Iterable[Any] | None = None,
        *,
        parallel: bool = True,
        max_queue_size: int = _DEFAULT_MAX_QUEUE_SIZE,
        timeout: float | None = None,
    ) -> Iterable[str]:
        """Run the pipeline.

        Parameters
        ----------
        input_data:
            Seed data fed into the first stage.
        parallel:
            When ``True`` (the default) stages execute concurrently in
            threads connected by bounded queues.  Set to ``False`` for
            the legacy sequential behaviour.
        max_queue_size:
            Backpressure limit between stages (parallel mode only).
        timeout:
            Reserved for future use.
        """
        if parallel:
            return self.run_parallel(input_data, max_queue_size=max_queue_size)
        return self.run_sync(input_data)

    def run_sync(self, input_data: Iterable[Any] | None = None) -> Iterable[str]:
        """Run the pipeline sequentially — each stage blocks until complete."""
        output: Any = input_data
        for stage in self.stages:
            if isinstance(stage, Slonk):
                output = stage.run_sync(output)
            elif isinstance(stage, TeeHandler):
                output = stage.process(output)
            else:
                output = stage.process(output)
        return output if output is not None else []

    def run_parallel(
        self,
        input_data: Iterable[Any] | None = None,
        *,
        max_queue_size: int = _DEFAULT_MAX_QUEUE_SIZE,
    ) -> Iterable[str]:
        """Run the pipeline with streaming/parallel execution.

        Each stage runs in its own thread.  Bounded queues between stages
        provide backpressure.  ``TeeHandler`` side-pipelines execute
        concurrently.
        """
        sp = _StreamingPipeline(self.stages, max_queue_size=max_queue_size)
        return sp.execute(input_data)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _is_path(self, string: str) -> bool:
        """Detect whether a string represents a filesystem path (local or remote)."""
        if string.startswith(("/", "./", "../")):
            return True
        if "://" in string:
            scheme = string.split("://", 1)[0].lower()
            return scheme in _KNOWN_PATH_PROTOCOLS
        return False

    def tee(self, pipeline: Slonk) -> Slonk:
        tee_stage = TeeHandler(pipeline)
        self.stages.append(tee_stage)
        return self


# ---------------------------------------------------------------------------
# Helper function for creating a tee stage
# ---------------------------------------------------------------------------


def tee(pipeline: Slonk) -> Slonk:
    s = Slonk()
    s.tee(pipeline)
    return s


# ---------------------------------------------------------------------------
# Example usage
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine)

    # Add sample records to ExampleModel
    session = SessionLocal()
    session.add_all(
        [
            ExampleModel(id="1", data="Hello World"),
            ExampleModel(id="2", data="Goodbye World"),
            ExampleModel(id="3", data="Hello Again"),
        ]
    )
    session.commit()

    # Create a pipeline
    pipeline = (
        Slonk(session_factory=SessionLocal)
        | ExampleModel  # Automatically wraps ExampleModel with SQLAlchemyHandler
        | "grep Hello"  # Shell command to filter records
        | tee(
            Slonk() | "./file.csv"  # Tee to a local path
        )  # Forks pipeline to handle both destinations
        | "s3://my-bucket/my-file.txt"  # Tee to a cloud path
    )

    # Run pipeline (parallel by default)
    result = pipeline.run()
    print("Pipeline result:")
    print("\n".join(result))
