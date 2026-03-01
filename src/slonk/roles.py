from __future__ import annotations

import enum
from collections.abc import Iterable
from typing import Protocol, runtime_checkable


class _Role(enum.Enum):
    """The role a stage plays in the pipeline based on its position."""

    SOURCE = "source"
    TRANSFORM = "transform"
    SINK = "sink"


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


# Type alias for stages.  Slonk sub-pipelines act as Transform.
type StageType = Source | Transform | Sink
