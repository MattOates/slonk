"""Base mixin providing middleware event emission for pipeline handlers.

:class:`SlonkBase` is the common base class for all built-in handlers
(e.g. :class:`~slonk.handlers.PathHandler`,
:class:`~slonk.handlers.ShellCommandHandler`).  It provides a lightweight
:meth:`emit` method that pushes custom events to the middleware dispatcher
when middleware is active, and is a zero-overhead no-op otherwise.

Examples:
    >>> from slonk.base import SlonkBase
    >>> handler = SlonkBase()
    >>> handler.emit("my_event")  # no-op when no middleware is wired
"""

from __future__ import annotations

import time
from queue import Queue
from typing import Any

from slonk.middleware import _Event, _EventType
from slonk.roles import _Role


class SlonkBase:
    """Mixin that gives any handler the ability to emit middleware events.

    The runner wires ``_event_queue``, ``_stage_index``, and ``_stage_role``
    before execution.  When no middleware is active ``_event_queue`` stays
    ``None`` and :meth:`emit` is a zero-overhead no-op.

    Examples:
        >>> from slonk.base import SlonkBase
        >>> h = SlonkBase()
        >>> h._event_queue is None
        True
        >>> h.emit("test")  # silent no-op
    """

    _event_queue: Queue[_Event] | None = None
    _stage_index: int = -1
    _stage_role: _Role = _Role.TRANSFORM

    def emit(self, event: str, data: dict[str, Any] | None = None) -> None:
        """Push a custom event to middleware (non-blocking, no-op when no middleware).

        Args:
            event: A short name identifying the event (e.g. ``"row_count"``).
            data: Optional dictionary of metadata to attach to the event.
        """
        q = self._event_queue
        if q is None:
            return
        q.put(
            _Event(
                type=_EventType.CUSTOM,
                stage=self,  # type: ignore[arg-type]
                role=self._stage_role,
                index=self._stage_index,
                timestamp=time.monotonic(),
                event_name=event,
                event_data=data,
            )
        )
