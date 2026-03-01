from __future__ import annotations

import threading
import time
from collections.abc import Iterable
from queue import Queue
from typing import Any

from slonk.constants import _DEFAULT_MAX_QUEUE_SIZE, _DONE
from slonk.middleware import _Event, _EventDispatcher, _EventType
from slonk.queue import _drain_queue, _QueueDrainState, _tracked_queue_iter
from slonk.roles import Sink, Source, StageType, Transform, _Role


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
        dispatcher: _EventDispatcher | None = None,
    ) -> None:
        self.stages = stages
        self.roles = roles
        self.max_queue_size = max_queue_size
        self.dispatcher = dispatcher

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
                args=(
                    stage,
                    self.roles[i],
                    i,
                    queues[i],
                    queues[i + 1],
                    errors,
                    lock,
                    self.dispatcher,
                ),
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
        index: int,
        in_q: Queue[Any],
        out_q: Queue[Any],
        errors: list[BaseException],
        lock: threading.Lock,
        dispatcher: _EventDispatcher | None,
    ) -> None:
        """Worker: read from *in_q*, run the stage, push results to *out_q*.

        Dispatch is based on the pre-computed *role*:
        - SOURCE: ignore input queue (drain it), call ``process_source()``.
        - TRANSFORM: pass a lazy queue iterator to ``process_transform()``.
        - SINK: pass a lazy queue iterator to ``process_sink()``.
        """
        # Import here to avoid circular import — Slonk references
        # _StreamingPipeline, and _StreamingPipeline._run_stage needs Slonk
        # for isinstance checks on sub-pipelines.
        from slonk.pipeline import Slonk

        drain_state = _QueueDrainState()
        stage_start = time.monotonic()

        # Emit STAGE_START event.
        if dispatcher is not None:
            dispatcher.queue.put(
                _Event(type=_EventType.STAGE_START, stage=stage, role=role, index=index)
            )

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

            # Emit STAGE_END event on success.
            if dispatcher is not None:
                dispatcher.queue.put(
                    _Event(
                        type=_EventType.STAGE_END,
                        stage=stage,
                        role=role,
                        index=index,
                        duration=time.monotonic() - stage_start,
                    )
                )

        except BaseException as exc:
            # Emit STAGE_ERROR event.
            if dispatcher is not None:
                dispatcher.queue.put(
                    _Event(
                        type=_EventType.STAGE_ERROR,
                        stage=stage,
                        role=role,
                        index=index,
                        error=exc,
                    )
                )
            _drain_queue(in_q, already_done=drain_state.done)
            with lock:
                errors.append(exc)
        finally:
            out_q.put(_DONE)
