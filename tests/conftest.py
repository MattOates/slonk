from __future__ import annotations

from collections.abc import Iterable
from typing import Any

import pytest

from helpers import TestBase, TestModel  # noqa: F401 — re-exported for conftest consumers
from slonk import Slonk


class PipelineRunner:
    """Callable wrapper that runs a Slonk pipeline in a specific execution mode.

    Used by the ``run_pipeline`` fixture so that every test automatically runs
    in both sync and parallel modes.  The ``mode`` attribute is available for
    introspection in test output and assertions.
    """

    def __init__(self, mode: str) -> None:
        self.mode = mode

    def __call__(self, slonk: Slonk, input_data: Iterable[Any] | None = None) -> Iterable[str]:
        if self.mode == "sync":
            return slonk.run_sync(input_data)
        return slonk.run_parallel(input_data)

    def __repr__(self) -> str:
        return f"PipelineRunner(mode={self.mode!r})"


@pytest.fixture(params=["sync", "parallel"])
def run_pipeline(request: pytest.FixtureRequest) -> PipelineRunner:
    """Parameterized fixture that runs every consuming test in both execution modes.

    Tests using this fixture will appear in pytest output as::

        test_foo[sync]   PASSED
        test_foo[parallel]   PASSED

    If a test is incompatible with one mode, mark it ``@pytest.mark.sync_only``
    or ``@pytest.mark.parallel_only`` and call ``run_sync()`` / ``run_parallel()``
    directly instead of using this fixture.
    """
    return PipelineRunner(request.param)
