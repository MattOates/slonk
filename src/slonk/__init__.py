from __future__ import annotations

import contextlib
import enum
import inspect
import subprocess
import sys
import threading
from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from queue import Queue
from typing import Any, Protocol, get_type_hints, runtime_checkable

import cloudpickle
from sqlalchemy import Column, String, create_engine, select
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker
from upath import UPath

__all__ = [
    "Base",
    "ExampleModel",
    "PathHandler",
    "SQLAlchemyHandler",
    "ShellCommandHandler",
    "Sink",
    "Slonk",
    "Source",
    "TeeHandler",
    "Transform",
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
# Stage role enum
# ---------------------------------------------------------------------------


class _Role(enum.Enum):
    """The role a stage plays in the pipeline based on its position."""

    SOURCE = "source"
    TRANSFORM = "transform"
    SINK = "sink"


# ---------------------------------------------------------------------------
# Handler role protocols
# ---------------------------------------------------------------------------


@runtime_checkable
class Source(Protocol):
    """A stage that produces data from nothing (first stage, no seed data).

    Return an ``Iterable[str]``.  If the iterable is a generator the pipeline
    will stream items lazily; if it is a list they are pushed in one go.
    """

    def process_source(self) -> Iterable[str]: ...


@runtime_checkable
class Transform(Protocol):
    """A stage that receives input and produces output.

    ``input_data`` is always a real ``Iterable[str]`` — never ``None``.
    In parallel mode it may be a lazy queue-backed iterator; in sync mode
    it is typically a list.  Handlers may iterate it however they like.
    """

    def process_transform(self, input_data: Iterable[str]) -> Iterable[str]: ...


@runtime_checkable
class Sink(Protocol):
    """A stage that consumes input as the final pipeline stage.

    Returns ``None``.  The pipeline's return value when ending with a
    ``Sink`` is an empty list.
    """

    def process_sink(self, input_data: Iterable[str]) -> None: ...


# ---------------------------------------------------------------------------
# Stage type alias
# ---------------------------------------------------------------------------

# Type alias for stages.  Slonk sub-pipelines act as Transform.
type StageType = Source | Transform | Sink | "Slonk"


# ---------------------------------------------------------------------------
# Role validation helpers
# ---------------------------------------------------------------------------


def _compute_roles(
    stages: list[StageType],
    *,
    has_seed: bool,
) -> list[_Role]:
    """Determine the role each stage should play.

    Raises ``TypeError`` if a stage cannot fulfil its required role.
    """
    n = len(stages)
    if n == 0:
        return []

    roles: list[_Role] = []
    for i, stage in enumerate(stages):
        is_first = i == 0
        is_last = i == n - 1

        # Slonk sub-pipelines always act as Transform (take input, produce output).
        is_slonk = isinstance(stage, Slonk)

        if is_first and is_last:
            # Single-stage pipeline.
            if has_seed:
                # Prefer Sink, else Transform.
                if isinstance(stage, Sink):
                    roles.append(_Role.SINK)
                elif isinstance(stage, Transform) or is_slonk:
                    roles.append(_Role.TRANSFORM)
                else:
                    raise TypeError(
                        f"Stage 0 ({type(stage).__name__}) receives seed data but "
                        f"implements neither Transform nor Sink."
                    )
            else:
                if isinstance(stage, Source):
                    roles.append(_Role.SOURCE)
                else:
                    raise TypeError(
                        f"Stage 0 ({type(stage).__name__}) is the first stage with no "
                        f"seed data but does not implement Source."
                    )
        elif is_first:
            # First of multiple stages.
            if has_seed:
                if isinstance(stage, Transform) or is_slonk:
                    roles.append(_Role.TRANSFORM)
                else:
                    raise TypeError(
                        f"Stage 0 ({type(stage).__name__}) receives seed data but "
                        f"does not implement Transform."
                    )
            else:
                if isinstance(stage, Source):
                    roles.append(_Role.SOURCE)
                else:
                    raise TypeError(
                        f"Stage 0 ({type(stage).__name__}) is the first stage with no "
                        f"seed data but does not implement Source."
                    )
        elif is_last:
            # Last of multiple stages — receives input from previous.
            if isinstance(stage, Sink):
                roles.append(_Role.SINK)
            elif isinstance(stage, Transform) or is_slonk:
                roles.append(_Role.TRANSFORM)
            else:
                raise TypeError(
                    f"Stage {i} ({type(stage).__name__}) is the last stage but "
                    f"implements neither Transform nor Sink."
                )
        else:
            # Middle stage.
            if isinstance(stage, Transform) or is_slonk:
                roles.append(_Role.TRANSFORM)
            else:
                raise TypeError(
                    f"Stage {i} ({type(stage).__name__}) is a middle stage but "
                    f"does not implement Transform."
                )

    return roles


# ---------------------------------------------------------------------------
# Queue iteration helpers
# ---------------------------------------------------------------------------


def _queue_iter(q: Queue[Any]) -> Iterator[str]:
    """Yield items from *q* until the ``_DONE`` sentinel is received."""
    while True:
        item = q.get()
        if item is _DONE:
            return
        yield item


class _QueueDrainState:
    """Mutable flag shared between the queue iterator and ``_run_stage``.

    Set to ``True`` once the ``_DONE`` sentinel has been consumed from the
    input queue, so the error handler in ``_run_stage`` knows not to call
    ``_drain_queue`` on an already-empty queue (which would deadlock).
    """

    __slots__ = ("done",)

    def __init__(self) -> None:
        self.done = False


def _tracked_queue_iter(q: Queue[Any], state: _QueueDrainState) -> Iterator[str]:
    """Like ``_queue_iter`` but sets *state.done* when ``_DONE`` is consumed."""
    while True:
        item = q.get()
        if item is _DONE:
            state.done = True
            return
        yield item


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

    Implements ``Source`` (read a file), ``Transform`` (write to file and
    pass data through), and ``Sink`` (write to file, discard data).
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
        """Write input to file and pass each item through."""
        with self.upath.open("w") as file:
            for line in input_data:
                file.write(line + "\n")
                yield line

    # -- Sink --------------------------------------------------------------

    def process_sink(self, input_data: Iterable[str]) -> None:
        """Write input to file (final stage, no passthrough)."""
        with self.upath.open("w") as file:
            for line in input_data:
                file.write(line + "\n")

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

    Implements ``Transform`` — stdin is fed from the input iterable
    via a writer thread, and stdout lines are yielded as they arrive.
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

    Implements ``Source`` — uses ``yield_per()`` to fetch rows in chunks,
    yielding formatted strings one at a time for natural streaming.
    """

    _YIELD_PER_CHUNK = 100

    def __init__(self, model: type[Base], session_factory: sessionmaker[Session]) -> None:
        self.model = model
        self.session_factory = session_factory

    # -- Source ------------------------------------------------------------

    def process_source(self) -> Iterable[str]:
        """Yield formatted rows using chunked fetching via ``yield_per``."""
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
    - No parameters (or only ``self``) and returns something → Source
    - Accepts an input parameter and returns ``None`` → Sink
    - Accepts an input parameter and returns something → Transform (default)
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


class _CallableSource:
    """Wraps a no-argument callable as a ``Source``."""

    def __init__(self, func: Any) -> None:
        self.func = func

    def process_source(self) -> Iterable[str]:
        result: Iterable[str] = self.func()
        return result


class _CallableTransform:
    """Wraps a callable as a ``Transform``."""

    def __init__(self, func: Any) -> None:
        self.func = func

    def process_transform(self, input_data: Iterable[str]) -> Iterable[str]:
        result: Iterable[str] = self.func(input_data)
        return result


class _CallableSink:
    """Wraps a callable that returns ``None`` as a ``Sink``."""

    def __init__(self, func: Any) -> None:
        self.func = func

    def process_sink(self, input_data: Iterable[str]) -> None:
        self.func(input_data)


def _wrap_callable(func: Any) -> _CallableSource | _CallableTransform | _CallableSink:
    """Create the appropriate callable wrapper based on signature inference."""
    role = _infer_callable_role(func)
    if role is _Role.SOURCE:
        return _CallableSource(func)
    if role is _Role.SINK:
        return _CallableSink(func)
    return _CallableTransform(func)


class TeeHandler:
    """Passes input through unchanged while also running a side pipeline.

    Implements ``Transform`` — materialises input so both the main
    passthrough and side pipeline can iterate independently.  In the
    streaming pipeline the stage thread provides natural concurrency.
    """

    def __init__(self, pipeline: Slonk) -> None:
        self.pipeline = pipeline

    def process_transform(self, input_data: Iterable[str]) -> Iterable[str]:
        """Run side pipeline and return original data + side results."""
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

    Implements ``Transform``.

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

    def process_transform(self, input_data: Iterable[str]) -> Iterable[str]:
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
        results: list[str] = []
        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            futures = [pool.submit(self.func, chunk) for chunk in chunks]
            for future in futures:
                results.extend(future.result())
        return results

    def _run_multiprocess(self, chunks: list[list[str]]) -> list[str]:
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
        roles: list[_Role],
        max_queue_size: int = _DEFAULT_MAX_QUEUE_SIZE,
    ) -> None:
        self.stages = stages
        self.roles = roles
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
                args=(stage, self.roles[i], queues[i], queues[i + 1], errors, lock),
                daemon=True,
            )
            threads.append(t)
            t.start()

        # Seed the first queue with input data.
        if input_data is not None:
            for item in input_data:
                queues[0].put(item)
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
        role: _Role,
        in_q: Queue[Any],
        out_q: Queue[Any],
        errors: list[BaseException],
        lock: threading.Lock,
    ) -> None:
        """Worker: read from *in_q*, run the stage, push results to *out_q*.

        Dispatch is based on the pre-computed *role*:
        - SOURCE: ignore input queue (drain it), call ``process_source()``.
        - TRANSFORM: pass a lazy queue iterator to ``process_transform()``.
        - SINK: pass a lazy queue iterator to ``process_sink()``.
        """
        drain_state = _QueueDrainState()
        try:
            if role is _Role.SOURCE:
                # Drain the input queue (contains just _DONE, or seed data
                # that should not be here — validation prevents this).
                for _ in _tracked_queue_iter(in_q, drain_state):
                    pass  # discard
                assert isinstance(stage, Source)
                result = stage.process_source()
                if result is not None:
                    for item in result:
                        out_q.put(item)

            elif role is _Role.TRANSFORM:
                input_stream = _tracked_queue_iter(in_q, drain_state)
                if isinstance(stage, Slonk):
                    # Sub-pipelines run synchronously on materialised input.
                    items = list(input_stream)
                    drain_state.done = True
                    result = stage.run_sync(items or None)
                    if result is not None:
                        for r in result:
                            out_q.put(r)
                else:
                    assert isinstance(stage, Transform)
                    result = stage.process_transform(input_stream)
                    if result is not None:
                        for item in result:
                            out_q.put(item)

            elif role is _Role.SINK:
                input_stream = _tracked_queue_iter(in_q, drain_state)
                assert isinstance(stage, Sink)
                stage.process_sink(input_stream)

        except BaseException as exc:
            _drain_queue(in_q, already_done=drain_state.done)
            with lock:
                errors.append(exc)
        finally:
            out_q.put(_DONE)


# ---------------------------------------------------------------------------
# Core pipeline
# ---------------------------------------------------------------------------


class Slonk:
    def __init__(self, session_factory: sessionmaker[Session] | None = None) -> None:
        self.stages: list[StageType] = []
        self.session_factory = session_factory

    def __or__(
        self,
        other: str | Slonk | type[Base] | _ParallelHandler | Any,
    ) -> Slonk:
        if isinstance(other, _ParallelHandler):
            self.stages.append(other)  # type: ignore[arg-append]
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
            self.stages.append(_wrap_callable(other))
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
        if not self.stages:
            return list(input_data) if input_data is not None else []

        roles = _compute_roles(self.stages, has_seed=input_data is not None)
        output: Any = input_data

        for stage, role in zip(self.stages, roles, strict=True):
            if role is _Role.SOURCE:
                if isinstance(stage, Source):
                    output = stage.process_source()
                else:
                    raise TypeError(f"{type(stage).__name__} does not implement Source")
            elif role is _Role.TRANSFORM:
                if isinstance(stage, Slonk):
                    output = stage.run_sync(output)
                elif isinstance(stage, Transform):
                    output = stage.process_transform(output)
                else:
                    raise TypeError(f"{type(stage).__name__} does not implement Transform")
            elif role is _Role.SINK:
                if isinstance(stage, Sink):
                    stage.process_sink(output)
                    output = []
                else:
                    raise TypeError(f"{type(stage).__name__} does not implement Sink")

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
        roles = _compute_roles(self.stages, has_seed=input_data is not None)
        sp = _StreamingPipeline(self.stages, roles, max_queue_size=max_queue_size)
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
