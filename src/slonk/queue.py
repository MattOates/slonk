from __future__ import annotations

from collections.abc import Iterator
from queue import Queue
from typing import Any

from slonk.constants import _DONE


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
