"""Core pipeline orchestrator and public API.

This module contains the :class:`Slonk` class — the main entry point for
building and running typed data pipelines.  Stages are composed using the
``|`` operator and executed with :meth:`Slonk.run`.

It also provides:

* :class:`TeeHandler` — fork data to a side-pipeline while passing it through.
* :class:`MergeHandler` — merge upstream data with sub-pipeline outputs (interleaved).
* :class:`CatHandler` — concatenate upstream data with sub-pipeline outputs (ordered).
* :func:`tee` — convenience factory that creates a ``TeeHandler``-bearing pipeline.
* :func:`merge` — convenience factory that creates a ``MergeHandler``-bearing pipeline.
* :func:`cat` — convenience factory that creates a ``CatHandler``-bearing pipeline.
* :func:`_compute_roles` — assign :class:`~slonk.roles._Role` to each stage.

Examples:
    Build and run a simple pipeline:

    >>> from slonk.pipeline import Slonk
    >>> p = (
    ...     Slonk()
    ...     | (lambda: ["hello", "world"])
    ...     | (lambda data: [s.upper() for s in data])
    ... )
    >>> sorted(p.run(parallel=False))
    ['HELLO', 'WORLD']
"""

from __future__ import annotations

import threading
import time
from collections.abc import Iterable
from typing import Any

from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from slonk.base import SlonkBase
from slonk.constants import _DEFAULT_MAX_QUEUE_SIZE, _KNOWN_PATH_PROTOCOLS
from slonk.handlers import (
    PathHandler,
    ShellCommandHandler,
    SQLAlchemyHandler,
    _ParallelHandler,
    _wrap_callable,
)
from slonk.middleware import Middleware, _Event, _EventDispatcher, _EventType
from slonk.roles import Sink, Source, StageType, Transform, _Role
from slonk.streaming import _StreamingPipeline


def _compute_roles(
    stages: list[StageType],
    *,
    has_seed: bool,
) -> list[_Role]:
    """Determine the role each stage should play.

    Inspects each stage's protocol conformance and position to assign
    :class:`~slonk.roles._Role` values.

    Args:
        stages: The ordered list of handler instances.
        has_seed: Whether the caller supplied seed data.

    Returns:
        A list of roles, one per stage.

    Raises:
        TypeError: If a stage cannot fulfil its required role.

    Examples:
        >>> from slonk.pipeline import _compute_roles
        >>> from slonk.roles import _Role, Source
        >>> class Src:
        ...     def process_source(self):
        ...         return []
        >>> _compute_roles([Src()], has_seed=False)
        [<_Role.SOURCE: 'source'>]
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


class TeeHandler(SlonkBase):
    """Passes input through unchanged while also running a side pipeline.

    Implements :class:`~slonk.roles.Transform` — materialises input so
    both the main passthrough and side pipeline can iterate independently.
    In the streaming pipeline the stage thread provides natural concurrency.

    Args:
        pipeline: The side :class:`Slonk` pipeline to execute.
    """

    def __init__(self, pipeline: Slonk) -> None:
        self.pipeline = pipeline

    def process_transform(self, input_data: Iterable[str]) -> Iterable[str]:
        """Run side pipeline and return original data plus side results.

        The input is materialised into a list so that both the passthrough
        and the side pipeline can iterate it independently.

        Args:
            input_data: Items from the upstream stage.

        Returns:
            Original items followed by any output from the side pipeline.
        """
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


class MergeHandler(SlonkBase):
    """Merge upstream data with output from sub-pipelines concurrently.

    Implements :class:`~slonk.roles.Transform` — runs each sub-pipeline
    in its own thread, yielding items from any source as soon as they are
    available (interleaved / non-deterministic order).

    Upstream data is treated as the first stream; sub-pipeline outputs
    are additional streams.  All items are merged via a shared
    :class:`queue.Queue` for natural backpressure.

    Args:
        pipelines: One or more :class:`Slonk` sub-pipelines whose output
            should be merged with upstream data.
    """

    def __init__(self, *pipelines: Slonk) -> None:
        self.pipelines: tuple[Slonk, ...] = pipelines

    def process_transform(self, input_data: Iterable[str]) -> Iterable[str]:
        """Merge upstream data with sub-pipeline outputs concurrently.

        Each producer (upstream + N sub-pipelines) pushes items into a
        shared queue.  Items are yielded as soon as any producer emits
        them.  A sentinel per producer signals completion.

        Args:
            input_data: Items from the upstream stage.

        Yields:
            Items from upstream and all sub-pipelines, interleaved.

        Raises:
            Exception: Re-raises the first error from any producer.
        """
        import queue as _queue_mod

        n_producers = 1 + len(self.pipelines)
        q: _queue_mod.Queue[object] = _queue_mod.Queue(maxsize=_DEFAULT_MAX_QUEUE_SIZE)
        errors: list[BaseException] = []
        lock = threading.Lock()
        done = object()  # local sentinel

        def _push_upstream() -> None:
            try:
                for item in input_data:
                    q.put(item)
            except BaseException as exc:
                with lock:
                    errors.append(exc)
            finally:
                q.put(done)

        def _push_pipeline(pipeline: Slonk) -> None:
            try:
                for item in pipeline.run_sync():
                    q.put(item)
            except BaseException as exc:
                with lock:
                    errors.append(exc)
            finally:
                q.put(done)

        threads: list[threading.Thread] = []
        t = threading.Thread(target=_push_upstream, daemon=True)
        threads.append(t)
        t.start()

        for p in self.pipelines:
            t = threading.Thread(target=_push_pipeline, args=(p,), daemon=True)
            threads.append(t)
            t.start()

        done_count = 0
        while done_count < n_producers:
            item = q.get()
            if item is done:
                done_count += 1
            else:
                yield item  # type: ignore[misc]

        for t in threads:
            t.join()

        if errors:
            raise errors[0]


class CatHandler(SlonkBase):
    """Concatenate upstream data with output from sub-pipelines in order.

    Implements :class:`~slonk.roles.Transform` — yields all upstream
    items first, then all items from each sub-pipeline in the order
    they were listed.  Deterministic ordering is guaranteed.

    Args:
        pipelines: One or more :class:`Slonk` sub-pipelines whose output
            should be concatenated after upstream data.
    """

    def __init__(self, *pipelines: Slonk) -> None:
        self.pipelines: tuple[Slonk, ...] = pipelines

    def process_transform(self, input_data: Iterable[str]) -> Iterable[str]:
        """Yield upstream data followed by each sub-pipeline's output in order.

        Args:
            input_data: Items from the upstream stage.

        Yields:
            All upstream items, then all items from each sub-pipeline
            sequentially.
        """
        yield from input_data
        for pipeline in self.pipelines:
            yield from pipeline.run_sync()


class Slonk:
    """The main pipeline builder and executor.

    Construct a pipeline by chaining stages with the ``|`` operator.
    Stages can be strings (paths or shell commands), callables,
    SQLAlchemy model classes, :class:`~slonk.handlers._ParallelHandler`
    wrappers, protocol-conforming handler objects, or nested
    :class:`Slonk` sub-pipelines.

    Args:
        session_factory: Optional SQLAlchemy session factory, required
            when piping declarative model classes.

    Examples:
        Build a simple pipeline with lambdas:

        >>> p = Slonk() | (lambda: ["a", "b"]) | (lambda data: [s.upper() for s in data])
        >>> sorted(p.run(parallel=False))
        ['A', 'B']

        Pipe with seed data:

        >>> p = Slonk() | (lambda data: [s + "!" for s in data])
        >>> sorted(p.run(["hi", "there"], parallel=False))
        ['hi!', 'there!']
    """

    def __init__(self, session_factory: sessionmaker[Session] | None = None) -> None:
        self.stages: list[StageType] = []
        self.session_factory = session_factory
        self._middleware: list[Middleware] = []

    def __or__(
        self,
        other: str | Slonk | type[DeclarativeBase] | _ParallelHandler | Any,
    ) -> Slonk:
        """Append a stage to the pipeline using the ``|`` operator.

        The *other* operand is automatically wrapped in the appropriate
        handler based on its type:

        - ``str`` starting with ``/``, ``./``, ``../``, or a known URI
          scheme -> :class:`~slonk.handlers.PathHandler`
        - Other ``str`` -> :class:`~slonk.handlers.ShellCommandHandler`
        - :class:`Slonk` instance -> nested sub-pipeline (Transform)
        - SQLAlchemy :class:`~sqlalchemy.orm.DeclarativeBase` subclass ->
          :class:`~slonk.handlers.SQLAlchemyHandler`
        - :class:`~slonk.handlers._ParallelHandler` -> used directly
        - :class:`~slonk.roles.Source` / :class:`~slonk.roles.Transform` /
          :class:`~slonk.roles.Sink` protocol object -> used directly
        - Other callable -> wrapped via :func:`~slonk.handlers._wrap_callable`

        Args:
            other: The stage to append.

        Returns:
            ``self`` for fluent chaining.

        Raises:
            TypeError: If *other* is not a supported type.
            ValueError: If a SQLAlchemy model is used without a session factory.
        """
        if isinstance(other, _ParallelHandler):
            self.stages.append(other)  # type: ignore[arg-append]
        elif isinstance(other, str):
            if self._is_path(other):
                self.stages.append(PathHandler(other))
            else:
                self.stages.append(ShellCommandHandler(other))
        elif isinstance(other, Slonk):
            self.stages.append(other)  # type: ignore[arg-type]  # Slonk sub-pipelines act as Transform
        elif isinstance(other, type) and issubclass(other, DeclarativeBase):
            if self.session_factory is None:
                raise ValueError(
                    "Cannot use SQLAlchemy models without a session_factory. "
                    "Pass session_factory to Slonk()."
                )
            self.stages.append(SQLAlchemyHandler(other, self.session_factory))
        elif isinstance(other, (Source, Transform, Sink)):
            self.stages.append(other)
        elif callable(other):
            self.stages.append(_wrap_callable(other))
        else:
            raise TypeError(f"Unsupported type: {type(other)}")
        return self

    # ------------------------------------------------------------------
    # Middleware registration API
    # ------------------------------------------------------------------

    def add_middleware(self, mw: Middleware) -> Slonk:
        """Register persistent middleware (runs on every pipeline execution).

        Args:
            mw: The middleware instance to register.

        Returns:
            ``self`` for fluent chaining.

        Examples:
            >>> from slonk.pipeline import Slonk
            >>> from slonk.builtin_middleware import TimingMiddleware
            >>> p = Slonk()
            >>> tm = TimingMiddleware()
            >>> p.add_middleware(tm) is p
            True
        """
        self._middleware.append(mw)
        return self

    def remove_middleware(self, mw: Middleware) -> Slonk:
        """Remove a previously registered middleware.

        Args:
            mw: The middleware instance to remove.

        Returns:
            ``self`` for fluent chaining.

        Raises:
            ValueError: If *mw* is not currently registered.
        """
        self._middleware.remove(mw)
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
        middleware: list[Middleware] | None = None,
    ) -> Iterable[str]:
        """Run the pipeline.

        Args:
            input_data: Seed data fed into the first stage.
            parallel: When ``True`` (the default) stages execute
                concurrently in threads connected by bounded queues.
                Set to ``False`` for sequential behaviour.
            max_queue_size: Backpressure limit between stages
                (parallel mode only).
            timeout: Reserved for future use.
            middleware: Additional middleware for this run only (merged
                with persistent middleware registered via
                :meth:`add_middleware`).

        Returns:
            The pipeline output as an iterable of strings.
        """
        if parallel:
            return self.run_parallel(
                input_data, max_queue_size=max_queue_size, middleware=middleware
            )
        return self.run_sync(input_data, middleware=middleware)

    def run_sync(
        self,
        input_data: Iterable[Any] | None = None,
        *,
        middleware: list[Middleware] | None = None,
    ) -> Iterable[str]:
        """Run the pipeline sequentially — each stage blocks until complete.

        Args:
            input_data: Seed data fed into the first stage.
            middleware: Additional per-run middleware.

        Returns:
            The pipeline output as an iterable of strings.
        """
        if not self.stages:
            return list(input_data) if input_data is not None else []

        roles = _compute_roles(self.stages, has_seed=input_data is not None)

        # -- Middleware setup -----------------------------------------------
        all_mw = self._merge_middleware(middleware)
        dispatcher = self._start_middleware(all_mw, self.stages, roles)

        pipeline_start = time.monotonic()
        output: Any = input_data

        try:
            for i, (stage, role) in enumerate(zip(self.stages, roles, strict=True)):
                stage_start = time.monotonic()
                self._emit_stage_start(dispatcher, stage, role, i)
                try:
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
                    self._emit_stage_end(dispatcher, stage, role, i, stage_start)
                except BaseException as exc:
                    self._emit_stage_error(dispatcher, stage, role, i, exc)
                    raise
        finally:
            self._stop_middleware(dispatcher, self.stages, roles, pipeline_start)
            self._cleanup_stages(self.stages)

        return output if output is not None else []

    def run_parallel(
        self,
        input_data: Iterable[Any] | None = None,
        *,
        max_queue_size: int = _DEFAULT_MAX_QUEUE_SIZE,
        middleware: list[Middleware] | None = None,
    ) -> Iterable[str]:
        """Run the pipeline with streaming/parallel execution.

        Each stage runs in its own thread.  Bounded queues between stages
        provide backpressure.  :class:`TeeHandler` side-pipelines execute
        concurrently.

        Args:
            input_data: Seed data fed into the first stage.
            max_queue_size: Backpressure limit between stages.
            middleware: Additional per-run middleware.

        Returns:
            The pipeline output as a list of strings.
        """
        roles = _compute_roles(self.stages, has_seed=input_data is not None)

        # -- Middleware setup -----------------------------------------------
        all_mw = self._merge_middleware(middleware)
        dispatcher = self._start_middleware(all_mw, self.stages, roles)

        pipeline_start = time.monotonic()
        try:
            sp = _StreamingPipeline(
                self.stages, roles, max_queue_size=max_queue_size, dispatcher=dispatcher
            )
            result = sp.execute(input_data)
        finally:
            self._stop_middleware(dispatcher, self.stages, roles, pipeline_start)
            self._cleanup_stages(self.stages)

        return result

    # ------------------------------------------------------------------
    # Middleware helpers (private)
    # ------------------------------------------------------------------

    def _merge_middleware(self, per_run: list[Middleware] | None) -> list[Middleware]:
        """Merge persistent and per-run middleware lists."""
        if not self._middleware and not per_run:
            return []
        result = list(self._middleware)
        if per_run:
            result.extend(per_run)
        return result

    def _start_middleware(
        self,
        all_mw: list[Middleware],
        stages: list[StageType],
        roles: list[_Role],
    ) -> _EventDispatcher | None:
        """Create and start a dispatcher if middleware exist, wire stages."""
        if not all_mw:
            return None

        dispatcher = _EventDispatcher(all_mw)
        dispatcher.start()

        # Wire event queue and metadata onto SlonkBase stages.
        for i, (stage, role) in enumerate(zip(stages, roles, strict=True)):
            if isinstance(stage, SlonkBase):
                stage._event_queue = dispatcher.queue  # type: ignore[assignment]
                stage._stage_index = i
                stage._stage_role = role

        # Emit PIPELINE_START.
        dispatcher.queue.put(
            _Event(
                type=_EventType.PIPELINE_START,
                stages=list(stages),
                roles=list(roles),
            )
        )
        return dispatcher

    def _stop_middleware(
        self,
        dispatcher: _EventDispatcher | None,
        stages: list[StageType],
        roles: list[_Role],
        pipeline_start: float,
    ) -> None:
        """Emit PIPELINE_END, drain the queue, and stop the dispatcher."""
        if dispatcher is None:
            return
        dispatcher.queue.put(
            _Event(
                type=_EventType.PIPELINE_END,
                stages=list(stages),
                roles=list(roles),
                duration=time.monotonic() - pipeline_start,
            )
        )
        dispatcher.stop()

    @staticmethod
    def _cleanup_stages(stages: list[StageType]) -> None:
        """Clear middleware wiring from stages after execution."""
        for stage in stages:
            if isinstance(stage, SlonkBase):
                stage._event_queue = None
                stage._stage_index = -1
                stage._stage_role = _Role.TRANSFORM

    @staticmethod
    def _emit_stage_start(
        dispatcher: _EventDispatcher | None,
        stage: StageType,
        role: _Role,
        index: int,
    ) -> None:
        """Push a STAGE_START event to the dispatcher."""
        if dispatcher is None:
            return
        dispatcher.queue.put(
            _Event(type=_EventType.STAGE_START, stage=stage, role=role, index=index)
        )

    @staticmethod
    def _emit_stage_end(
        dispatcher: _EventDispatcher | None,
        stage: StageType,
        role: _Role,
        index: int,
        start_time: float,
    ) -> None:
        """Push a STAGE_END event to the dispatcher."""
        if dispatcher is None:
            return
        dispatcher.queue.put(
            _Event(
                type=_EventType.STAGE_END,
                stage=stage,
                role=role,
                index=index,
                duration=time.monotonic() - start_time,
            )
        )

    @staticmethod
    def _emit_stage_error(
        dispatcher: _EventDispatcher | None,
        stage: StageType,
        role: _Role,
        index: int,
        error: BaseException,
    ) -> None:
        """Push a STAGE_ERROR event to the dispatcher."""
        if dispatcher is None:
            return
        dispatcher.queue.put(
            _Event(
                type=_EventType.STAGE_ERROR,
                stage=stage,
                role=role,
                index=index,
                error=error,
            )
        )

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _is_path(self, string: str) -> bool:
        """Detect whether a string represents a filesystem path (local or remote).

        Args:
            string: The string to test.

        Returns:
            ``True`` if the string looks like a path or URI.

        Examples:
            >>> from slonk.pipeline import Slonk
            >>> s = Slonk()
            >>> s._is_path("/tmp/file.txt")
            True
            >>> s._is_path("s3://bucket/key")
            True
            >>> s._is_path("grep hello")
            False
        """
        if string.startswith(("/", "./", "../")):
            return True
        if "://" in string:
            scheme = string.split("://", 1)[0].lower()
            return scheme in _KNOWN_PATH_PROTOCOLS
        return False

    def tee(self, pipeline: Slonk) -> Slonk:
        """Fork data to a side pipeline while passing it through.

        The side pipeline receives a copy of the data at this point in
        the main pipeline.  The main pipeline's output includes both
        the original data and any output from the side pipeline.

        Args:
            pipeline: The side :class:`Slonk` pipeline.

        Returns:
            ``self`` for fluent chaining.
        """
        tee_stage = TeeHandler(pipeline)
        self.stages.append(tee_stage)
        return self

    def merge(self, *pipelines: Slonk) -> Slonk:
        """Merge upstream data with output from sub-pipelines concurrently.

        Items from all sources are interleaved as they become available
        (non-deterministic order).  Each sub-pipeline runs in its own
        thread for maximum throughput.

        Args:
            pipelines: One or more :class:`Slonk` sub-pipelines.

        Returns:
            ``self`` for fluent chaining.
        """
        self.stages.append(MergeHandler(*pipelines))
        return self

    def cat(self, *pipelines: Slonk) -> Slonk:
        """Concatenate upstream data with output from sub-pipelines in order.

        All upstream items are yielded first, then all items from each
        sub-pipeline in the order they were listed.  Deterministic
        ordering is guaranteed.

        Args:
            pipelines: One or more :class:`Slonk` sub-pipelines.

        Returns:
            ``self`` for fluent chaining.
        """
        self.stages.append(CatHandler(*pipelines))
        return self


def tee(pipeline: Slonk) -> Slonk:
    """Convenience factory: create a new :class:`Slonk` with a tee stage.

    Equivalent to ``Slonk().tee(pipeline)``.

    Args:
        pipeline: The side pipeline to fork data into.

    Returns:
        A new :class:`Slonk` containing a single :class:`TeeHandler` stage.
    """
    s = Slonk()
    s.tee(pipeline)
    return s


def merge(*pipelines: Slonk) -> Slonk:
    """Convenience factory: create a new :class:`Slonk` with a merge stage.

    Equivalent to ``Slonk().merge(*pipelines)``.

    Args:
        pipelines: Sub-pipelines whose output should be merged with
            upstream data (interleaved, non-deterministic order).

    Returns:
        A new :class:`Slonk` containing a single :class:`MergeHandler` stage.
    """
    s = Slonk()
    s.merge(*pipelines)
    return s


def cat(*pipelines: Slonk) -> Slonk:
    """Convenience factory: create a new :class:`Slonk` with a cat stage.

    Equivalent to ``Slonk().cat(*pipelines)``.

    Args:
        pipelines: Sub-pipelines whose output should be concatenated
            after upstream data (deterministic source order).

    Returns:
        A new :class:`Slonk` containing a single :class:`CatHandler` stage.
    """
    s = Slonk()
    s.cat(*pipelines)
    return s
