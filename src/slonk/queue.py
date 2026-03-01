"""Queue utilities for the streaming (parallel) pipeline executor.

Provides iterators that read from bounded :class:`~queue.Queue` instances
until a ``_DONE`` sentinel is received, plus a drain helper used for
error recovery.

Examples:
    >>> from queue import Queue
    >>> from slonk.constants import _DONE
    >>> from slonk.queue import _queue_iter
    >>> q: Queue[object] = Queue()
    >>> q.put("hello")
    >>> q.put(_DONE)
    >>> list(_queue_iter(q))
    ['hello']
"""

from __future__ import annotations

from collections.abc import Iterator
from queue import Queue
from typing import Any

from slonk.constants import _DONE


def _queue_iter(q: Queue[Any]) -> Iterator[Any]:
    """Yield items from *q* until the ``_DONE`` sentinel is received.

    Args:
        q: A queue containing items terminated by ``_DONE``.

    Yields:
        Each item from the queue.

    Examples:
        >>> from queue import Queue
        >>> from slonk.constants import _DONE
        >>> from slonk.queue import _queue_iter
        >>> q: Queue[object] = Queue()
        >>> q.put("a")
        >>> q.put("b")
        >>> q.put(_DONE)
        >>> list(_queue_iter(q))
        ['a', 'b']
    """
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

    Args:
        done: Whether the queue has been fully consumed.  Defaults to ``False``.

    Examples:
        >>> from slonk.queue import _QueueDrainState
        >>> state = _QueueDrainState()
        >>> state.done
        False
        >>> state.done = True
        >>> state.done
        True
    """

    __slots__ = ("done",)

    def __init__(self) -> None:
        self.done = False


def _tracked_queue_iter(q: Queue[Any], state: _QueueDrainState) -> Iterator[Any]:
    """Like :func:`_queue_iter` but sets *state.done* when ``_DONE`` is consumed.

    Args:
        q: A queue containing items terminated by ``_DONE``.
        state: Shared state flag that will be set when the queue is exhausted.

    Yields:
        Each item from the queue.
    """
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

    Args:
        q: The queue to drain.
        already_done: When ``True`` the queue has already had its
            ``_DONE`` sentinel consumed and is known to be empty, so
            this is a no-op.
    """
    if already_done:
        return
    while True:
        item = q.get()
        if item is _DONE:
            return
