"""Role-based protocol definitions for pipeline stages.

Every pipeline stage is assigned one of three roles — :class:`Source`,
:class:`Transform`, or :class:`Sink` — based on its position in the
pipeline and the type-hints / protocols it implements.  The
:class:`_Role` enum is the internal representation; the three
:func:`~typing.runtime_checkable` protocol classes define the method
contracts handlers must satisfy.

Examples:
    >>> from slonk.roles import _Role
    >>> _Role.SOURCE.value
    'source'
    >>> _Role.TRANSFORM.value
    'transform'
    >>> _Role.SINK.value
    'sink'
"""

from __future__ import annotations

import enum
from collections.abc import Iterable
from typing import Protocol, runtime_checkable


class _Role(enum.Enum):
    """The role a stage plays in the pipeline based on its position.

    Examples:
        >>> from slonk.roles import _Role
        >>> list(_Role)
        [<_Role.SOURCE: 'source'>, <_Role.TRANSFORM: 'transform'>, <_Role.SINK: 'sink'>]
    """

    SOURCE = "source"
    TRANSFORM = "transform"
    SINK = "sink"


@runtime_checkable
class Source(Protocol):
    """A stage that produces data from nothing (first stage, no seed data).

    Return an ``Iterable[str]``.  If the iterable is a generator the pipeline
    will stream items lazily; if it is a list they are pushed in one go.

    Examples:
        >>> from slonk.roles import Source
        >>> class MySource:
        ...     def process_source(self):
        ...         return ["a", "b", "c"]
        >>> isinstance(MySource(), Source)
        True
    """

    def process_source(self) -> Iterable[str]:
        """Produce items with no input."""
        ...


@runtime_checkable
class Transform(Protocol):
    """A stage that receives input and produces output.

    ``input_data`` is always a real ``Iterable[str]`` — never ``None``.
    In parallel mode it may be a lazy queue-backed iterator; in sync mode
    it is typically a list.  Handlers may iterate it however they like.

    Examples:
        >>> from slonk.roles import Transform
        >>> class Upper:
        ...     def process_transform(self, input_data):
        ...         return [s.upper() for s in input_data]
        >>> isinstance(Upper(), Transform)
        True
    """

    def process_transform(self, input_data: Iterable[str]) -> Iterable[str]:
        """Transform *input_data* into new output."""
        ...


@runtime_checkable
class Sink(Protocol):
    """A stage that consumes input as the final pipeline stage.

    Returns ``None``.  The pipeline's return value when ending with a
    ``Sink`` is an empty list.

    Examples:
        >>> from slonk.roles import Sink
        >>> class Collector:
        ...     def __init__(self):
        ...         self.items = []
        ...     def process_sink(self, input_data):
        ...         self.items.extend(input_data)
        >>> isinstance(Collector(), Sink)
        True
    """

    def process_sink(self, input_data: Iterable[str]) -> None:
        """Consume *input_data* with no output."""
        ...


# Type alias for stages.  Slonk sub-pipelines act as Transform.
type StageType = Source | Transform | Sink
