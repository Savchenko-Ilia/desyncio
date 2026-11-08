"""Tests for desyncio.run (LOOP-04).

These are plain sync tests: ``desyncio.run`` creates its own event loop, so it
must not be called from inside pytest-asyncio's running loop.
"""

import asyncio
import gc
import warnings

import pytest

import desyncio
from desyncio.loop import SimulationDeadlock
from desyncio.rng import RngStreams
from desyncio.run import SimulationCleanupError, current_rng_streams, run


def test_exported_from_package() -> None:
    assert desyncio.run is run
    assert "run" in desyncio.__all__
    assert "__version__" in desyncio.__all__


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


def test_exception_propagates_and_loop_closed() -> None:
    loops: list[asyncio.AbstractEventLoop] = []

    async def main() -> None:
        loops.append(asyncio.get_running_loop())
        await asyncio.sleep(1)
        raise KeyError("boom")

    with pytest.raises(KeyError, match="boom"):
        run(main())
    assert loops[0].is_closed()


def test_deadlock_propagates_and_loop_closed() -> None:
    loops: list[asyncio.AbstractEventLoop] = []

    async def main() -> None:
        loops.append(asyncio.get_running_loop())
        await asyncio.get_running_loop().create_future()

    with pytest.raises(SimulationDeadlock):
        run(main())
    assert loops[0].is_closed()


def test_loop_closed_after_normal_return() -> None:
    loops: list[asyncio.AbstractEventLoop] = []

    async def main() -> None:
        loops.append(asyncio.get_running_loop())

    run(main())
    assert loops[0].is_closed()


@pytest.mark.parametrize(
    "until", [-1, -0.001, float("nan"), float("inf"), -float("inf")]
)
def test_invalid_until_value(until: float) -> None:
    async def main() -> None: ...

    with pytest.raises(ValueError, match="until"):
        run(main(), until=until)


@pytest.mark.parametrize("until", ["5", True, [1]])
def test_invalid_until_type(until: object) -> None:
    async def main() -> None: ...

    with pytest.raises(TypeError, match="until"):
        run(main(), until=until)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("seed", "exc"),
    [(-1, ValueError), (True, TypeError), (1.5, TypeError), ("1", TypeError)],
)
def test_invalid_seed(seed: object, exc: type[Exception]) -> None:
    async def main() -> None: ...

    with pytest.raises(exc):
        run(main(), seed=seed)  # type: ignore[arg-type]


def test_non_coroutine_raises_type_error() -> None:
    async def main() -> None: ...

    with pytest.raises(TypeError, match="coroutine"):
        run(main)  # type: ignore[arg-type]  # forgot the call parentheses
    with pytest.raises(TypeError, match="coroutine"):
        run(42)  # type: ignore[arg-type]


def test_no_never_awaited_warning_on_rejected_arguments() -> None:
    async def main() -> None: ...

    with warnings.catch_warnings():
        warnings.simplefilter("error")
        with pytest.raises(ValueError):
            run(main(), until=-1.0)
        with pytest.raises(ValueError):
            run(main(), seed=-1)
        with pytest.raises(TypeError):
            run(main(), until="x")  # type: ignore[arg-type]
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
        got["child"] = current_rng_streams().stream("x").random()

    async def main() -> None:
        got["main_seed"] = current_rng_streams().seed
        await asyncio.create_task(child())

    run(main(), seed=7)
    assert got["main_seed"] == 7
    assert got["child"] == RngStreams(7).stream("x").random()


def test_same_seed_same_stream_different_seed_differs() -> None:
    def draw(seed: int) -> list[float]:
        out: list[float] = []

        async def main() -> None:
            out.extend(current_rng_streams().stream("a").random() for _ in range(3))

        run(main(), seed=seed)
        return out

    assert draw(1) == draw(1)
    assert draw(1) != draw(2)


def test_streams_shared_within_one_run_and_fresh_per_run() -> None:
    async def main() -> tuple[float, float]:
        s = current_rng_streams()
        assert current_rng_streams() is s
        return s.stream("a").random(), s.stream("a").random()

    assert run(main(), seed=3) == run(main(), seed=3)


def test_default_seed_is_zero() -> None:
    async def main() -> int:
        return current_rng_streams().seed

    assert run(main()) == 0


def test_accessor_outside_run_raises() -> None:
    with pytest.raises(RuntimeError, match="run"):
        current_rng_streams()


def test_accessor_does_not_leak_after_run() -> None:
    async def main() -> None: ...

    run(main(), seed=5)
    with pytest.raises(RuntimeError):
        current_rng_streams()
