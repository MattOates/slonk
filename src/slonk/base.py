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
    ``None`` and ``emit()`` is a zero-overhead no-op.
    """

    _event_queue: Queue[_Event] | None = None
    _stage_index: int = -1
    _stage_role: _Role = _Role.TRANSFORM

    def emit(self, event: str, data: dict[str, Any] | None = None) -> None:
        """Push a custom event to middleware (non-blocking, no-op when no middleware)."""
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
