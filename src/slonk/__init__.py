import subprocess
import sys
import threading
from collections.abc import Callable, Iterable
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from queue import Queue
from typing import Any, Protocol, Union, runtime_checkable

import cloudpickle
from sqlalchemy import Column, String, create_engine
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
# Handler protocol
# ---------------------------------------------------------------------------


@runtime_checkable
class Handler(Protocol):
    def process(self, input_data: Iterable[str] | None) -> Iterable[str]: ...


# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------


class PathHandler:
    """Unified path handler for local and remote filesystems via UPath."""

    def __init__(self, path: str) -> None:
        self.upath = UPath(path)

    def process(self, input_data: Iterable[str] | None) -> Iterable[str]:
        if input_data is not None:
            self.write(input_data)
            return input_data
        else:
            return self.read()

    def write(self, data: Iterable[str]) -> None:
        with self.upath.open("w") as file:
            for line in data:
                file.write(line + "\n")

    def read(self) -> Iterable[str]:
        with self.upath.open("r") as file:
            return file.readlines()


class ShellCommandHandler:
    def __init__(self, command: str) -> None:
        self.command = command

    def process(self, input_data: Iterable[str] | None) -> Iterable[str]:
        if input_data is not None:
            input_string = "\n".join(input_data)
            return [self._run_command(input_string)]
        else:
            return []

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
    def __init__(self, model: type[Base], session_factory: sessionmaker[Session]) -> None:
        self.model = model
        self.session_factory = session_factory

    def process(self, input_data: Iterable[Any] | None) -> Iterable[str]:
        session = self.session_factory()
        try:
            records = session.query(self.model).all()
            return [f"{record.id}\t{record.data}" for record in records]  # type: ignore[attr-defined]
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
        """Worker: drain *in_q*, run the stage, push results to *out_q*."""
        try:
            # Materialise all input from the queue (stages expect a full iterable).
            items: list[Any] = []
            while True:
                item = in_q.get()
                if item is _DONE:
                    break
                items.append(item)

            input_data: list[Any] | None = items if items else (items or None)
            # Distinguish "no items because upstream was empty list" from
            # "no items because there was no input at all".  We pass [] when
            # items is empty but was fed, and None only when the *first* stage
            # received no seed data.  Since the caller always seeds _DONE
            # after any (possibly zero) items, we treat empty items as [].
            if not items:
                # For the first stage it's possible that input_data was None
                # (caller had None).  But we already put _DONE, so items will
                # be [].  We want to pass [] so handlers that branch on None
                # (PathHandler read vs. write) still behave.  The only time
                # we want None is when the very first queue was seeded with
                # *only* _DONE and the original input_data was None.
                input_data = items  # i.e. []

            if isinstance(stage, Slonk):
                result = stage.run_sync(input_data if input_data else None)
            elif isinstance(stage, TeeHandler):
                result = stage.process_parallel(input_data if input_data else None)
            else:
                result = stage.process(input_data if input_data else None)

            if result is not None:
                for r in result:
                    out_q.put(r)
        except BaseException as exc:
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
