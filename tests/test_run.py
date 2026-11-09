"""Tests for desyncio.run (LOOP-04).

These are plain sync tests: ``desyncio.run`` creates its own event loop, so it
must not be called from inside pytest-asyncio's running loop.
"""

import asyncio
import gc
import importlib
import importlib.util
import warnings
from collections.abc import Callable, Coroutine
from typing import Any, assert_type
from unittest import mock

import pytest

import desyncio
from desyncio.loop import SimulationDeadlock
from desyncio.randomness import RandomStreams
from desyncio.runners import SimulationCleanupError, current_random_streams, run


def test_returns_main_result() -> None:
    async def main() -> int:
        return 42

    assert run(main()) == 42


def test_virtual_time_at_end() -> None:
    async def main() -> float:
        await asyncio.sleep(5)
        await asyncio.sleep(2.5)
        return asyncio.get_running_loop().time()

    assert run(main()) == 7.5


def test_until_stops_early_and_returns_none() -> None:
    seen: list[float] = []

    async def main() -> str:
        loop = asyncio.get_running_loop()
        for _ in range(10):
            await asyncio.sleep(1)
            seen.append(loop.time())
        return "done"

    assert run(main(), until=3.5) is None
    assert seen == [1.0, 2.0, 3.0]


def test_clock_reads_exactly_until_when_stopped() -> None:
    clocks: list[float] = []

    async def main() -> None:
        loop = asyncio.get_running_loop()
        try:
            await asyncio.sleep(100)
        except asyncio.CancelledError:
            clocks.append(loop.time())  # cleanup: still at until
            raise

    assert run(main(), until=3.5) is None
    assert clocks == [3.5]


def test_main_finishing_before_until_returns_result_and_keeps_its_time() -> None:
    async def main() -> float:
        await asyncio.sleep(2)
        return asyncio.get_running_loop().time()

    assert run(main(), until=10) == 2.0


def test_timer_exactly_at_until_runs_and_later_one_does_not() -> None:
    fired: list[str] = []

    async def main() -> None:
        loop = asyncio.get_running_loop()
        loop.call_at(5.0, fired.append, "at")
        loop.call_at(5.5, fired.append, "after")
        await asyncio.sleep(100)

    assert run(main(), until=5.0) is None
    assert fired == ["at"]


def test_until_zero_runs_only_time_zero_work() -> None:
    fired: list[str] = []

    async def main() -> None:
        loop = asyncio.get_running_loop()
        loop.call_at(0.0, fired.append, "now")
        loop.call_later(1, fired.append, "later")
        await asyncio.sleep(10)

    assert run(main(), until=0) is None
    assert fired == ["now"]


def test_background_task_left_unfinished_and_cancelled_at_close() -> None:
    events: list[str] = []

    async def worker() -> None:
        try:
            await asyncio.sleep(50)
            events.append("finished")
        except asyncio.CancelledError:
            events.append("cancelled")
            raise

    async def main() -> None:
        asyncio.create_task(worker())
        await asyncio.sleep(100)

    assert run(main(), until=10) is None
    assert events == ["cancelled"]


def test_zero_time_cancellation_handler_is_fine() -> None:
    events: list[tuple[str, float]] = []

    async def main() -> None:
        loop = asyncio.get_running_loop()
        try:
            await asyncio.sleep(100)
        finally:
            await asyncio.sleep(0)
            events.append(("cleanup", loop.time()))

    assert run(main(), until=4) is None
    assert events == [("cleanup", 4.0)]


def test_sleeping_cancellation_handler_cannot_pass_until() -> None:
    reached: list[str] = []
    loops: list[asyncio.AbstractEventLoop] = []

    async def main() -> None:
        loops.append(asyncio.get_running_loop())
        try:
            await asyncio.sleep(100)
        finally:
            await asyncio.sleep(1)  # would run at t=5 > until=4
            reached.append("after-sleep")

    with pytest.raises(SimulationCleanupError, match="until"):
        run(main(), until=4)
    assert reached == []
    assert loops[0].time() == 4.0
    assert loops[0].is_closed()


def test_sleeping_handler_after_main_finished_cannot_advance_clock() -> None:
    loops: list[asyncio.AbstractEventLoop] = []

    async def worker() -> None:
        try:
            await asyncio.sleep(100)
        finally:
            await asyncio.sleep(1)

    async def main() -> str:
        loops.append(asyncio.get_running_loop())
        asyncio.create_task(worker())
        await asyncio.sleep(2)
        return "ok"

    with pytest.raises(SimulationCleanupError):
        run(main(), until=50)
    assert loops[0].time() == 2.0
    assert loops[0].is_closed()


async def _raises_key_error() -> None:
    await asyncio.sleep(1)
    raise KeyError("boom")


async def _deadlocks() -> None:
    await asyncio.get_running_loop().create_future()


async def _returns() -> None:
    pass


@pytest.mark.parametrize(
    ("body", "exc"),
    [(_raises_key_error, KeyError), (_deadlocks, SimulationDeadlock), (_returns, None)],
)
def test_loop_closed_after_exception_deadlock_or_normal_return(
    body: Callable[[], Coroutine[Any, Any, None]], exc: type[Exception] | None
) -> None:
    loops: list[asyncio.AbstractEventLoop] = []

    async def main() -> None:
        loops.append(asyncio.get_running_loop())
        await body()

    if exc is None:
        run(main())
    else:
        with pytest.raises(exc):
            run(main())
    assert loops[0].is_closed()


@pytest.mark.parametrize("until", [-1, float("nan"), float("inf")])
def test_invalid_until_value(until: float) -> None:
    async def main() -> None: ...

    with pytest.raises(ValueError, match="until"):
        run(main(), until=until)


@pytest.mark.parametrize("until", ["5", True, [1]])
def test_invalid_until_type(until: object) -> None:
    async def main() -> None: ...

    with pytest.raises(TypeError, match="until"):
        run(main(), until=until)  # type: ignore[call-overload]


@pytest.mark.parametrize(
    ("seed", "exc"),
    [(-1, ValueError), (True, TypeError), (1.5, TypeError), ("1", TypeError)],
)
def test_invalid_seed(seed: object, exc: type[Exception]) -> None:
    async def main() -> None: ...

    with pytest.raises(exc):
        run(main(), seed=seed)  # type: ignore[call-overload]


def test_non_coroutine_raises_type_error() -> None:
    async def main() -> None: ...

    with pytest.raises(TypeError, match="coroutine"):
        run(main)  # type: ignore[call-overload]  # forgot the call parentheses
    with pytest.raises(TypeError, match="coroutine"):
        run(42)  # type: ignore[call-overload]


def test_no_never_awaited_warning_on_rejected_arguments() -> None:
    async def main() -> None: ...

    with warnings.catch_warnings():
        warnings.simplefilter("error")
        with pytest.raises(ValueError):
            run(main(), until=-1.0)
        with pytest.raises(ValueError):
            run(main(), seed=-1)
        with pytest.raises(TypeError):
            run(main(), until="x")  # type: ignore[call-overload]
        gc.collect()


def test_nested_run_raises_and_does_not_leak_coroutine() -> None:
    async def inner() -> None: ...

    async def main() -> None:
        with pytest.raises(RuntimeError, match="running"):
            run(inner())

    with warnings.catch_warnings():
        warnings.simplefilter("error")
        run(main())
        gc.collect()


def test_seed_visible_in_main_and_spawned_task() -> None:
    got: dict[str, float] = {}

    async def child() -> None:
        got["child"] = current_random_streams().stream("x").random()

    async def main() -> None:
        got["main_seed"] = current_random_streams().seed
        await asyncio.create_task(child())

    run(main(), seed=7)
    assert got["main_seed"] == 7
    assert got["child"] == RandomStreams(7).stream("x").random()


def test_streams_shared_within_one_run_and_fresh_per_run() -> None:
    async def main() -> tuple[float, float]:
        s = current_random_streams()
        assert current_random_streams() is s
        return s.stream("a").random(), s.stream("a").random()

    assert run(main(), seed=3) == run(main(), seed=3)


def test_default_seed_is_zero() -> None:
    async def main() -> int:
        return current_random_streams().seed

    assert run(main()) == 0


def test_accessor_outside_run_raises() -> None:
    with pytest.raises(RuntimeError, match="run"):
        current_random_streams()


def test_accessor_does_not_leak_after_run() -> None:
    async def main() -> None: ...

    run(main(), seed=5)
    with pytest.raises(RuntimeError):
        current_random_streams()


def test_no_submodule_named_run_shadows_the_function() -> None:
    assert callable(desyncio.run)
    assert desyncio.run is run
    runners = importlib.import_module("desyncio.runners")
    assert hasattr(runners, "current_random_streams")
    assert importlib.util.find_spec("desyncio.run") is None
    with pytest.raises(ImportError):
        importlib.import_module("desyncio.run.nothing")
    # patching by dotted path hits the module attribute, not the function
    with mock.patch("desyncio.runners.current_random_streams") as fake:
        assert runners.current_random_streams is fake
    assert runners.current_random_streams is current_random_streams


def test_overload_types() -> None:
    # assert_type is a runtime no-op. It is checked only by running
    # `uv run mypy tests/test_run.py` by hand: the gate runs `mypy src` only
    # (D042 left adding `mypy tests` to the gate open for the owner).
    async def main() -> int:
        return 1

    assert_type(run(main()), int)
    assert_type(run(main(), until=None), int)
    assert_type(run(main(), until=5.0), int | None)
    assert_type(run(main(), until=5), int | None)


@pytest.mark.parametrize(
    ("until", "expected", "end"), [(None, "done", 3.5), (10, None, 10.0)]
)
def test_endless_background_task_is_cancelled_cleanly(
    until: float | None, expected: str | None, end: float
) -> None:
    seen: dict[str, float | bool] = {}

    async def ticker() -> None:
        try:
            while True:  # noqa: ASYNC110 (the point: an endless timer loop)
                await asyncio.sleep(1)
        finally:
            seen["cancelled_at"] = asyncio.get_running_loop().time()

    async def main() -> str:
        asyncio.create_task(ticker())
        await asyncio.sleep(3.5 if until is None else 100)
        return "done"

    with warnings.catch_warnings():
        warnings.simplefilter("error")
        result = run(main(), until=until)
        gc.collect()
    assert result == expected
    assert seen["cancelled_at"] == end
