"""Tests for the virtual-time event loop."""

import asyncio
import time
from collections.abc import Callable, Coroutine
from typing import Any

import pytest

from desyncio.clock import VirtualClock
from desyncio.loop import SimulationDeadlock, VirtualTimeLoop


def run[T](main: Callable[[], Coroutine[Any, Any, T]], *, start: float = 0.0) -> T:
    """Run ``main()`` to completion on a fresh VirtualTimeLoop."""
    with asyncio.Runner(loop_factory=lambda: VirtualTimeLoop(start=start)) as runner:
        return runner.run(main())


def test_time_starts_at_zero() -> None:
    async def main() -> float:
        return asyncio.get_running_loop().time()

    assert run(main) == 0.0


def test_time_starts_at_given_start() -> None:
    async def main() -> float:
        return asyncio.get_running_loop().time()

    assert run(main, start=100.0) == 100.0


def test_accepts_clock() -> None:
    clock = VirtualClock(5.0)
    loop = VirtualTimeLoop(clock=clock)
    try:
        assert loop.time() == 5.0
        loop.run_until_complete(asyncio.sleep(2))
        assert clock.now == 7.0
    finally:
        loop.close()


def test_sleep_one_hour_is_instant() -> None:
    """Sleeping 3600 virtual seconds takes < 50 ms of wall time.

    Only ``runner.run`` is timed (not loop creation or teardown) and the best
    of three runs is used, so a one-off scheduling hiccup on a busy CI machine
    cannot fail the test.
    """

    async def main() -> float:
        await asyncio.sleep(3600)
        return asyncio.get_running_loop().time()

    best = float("inf")
    for _ in range(3):
        with asyncio.Runner(loop_factory=VirtualTimeLoop) as runner:
            t0 = time.perf_counter()
            result = runner.run(main())
            best = min(best, time.perf_counter() - t0)
        assert result == 3600.0
    assert best < 0.05


def test_sleeps_wake_in_time_order() -> None:
    async def main() -> list[tuple[str, float]]:
        loop = asyncio.get_running_loop()
        woke: list[tuple[str, float]] = []

        async def sleeper(name: str, d: float) -> None:
            await asyncio.sleep(d)
            woke.append((name, loop.time()))

        await asyncio.gather(sleeper("c", 30), sleeper("a", 10), sleeper("b", 20))
        return woke

    assert run(main) == [("a", 10.0), ("b", 20.0), ("c", 30.0)]


def test_call_later_and_call_at() -> None:
    async def main() -> list[tuple[str, float]]:
        loop = asyncio.get_running_loop()
        fired: list[tuple[str, float]] = []
        loop.call_later(5, lambda: fired.append(("later", loop.time())))
        loop.call_at(2.5, lambda: fired.append(("at", loop.time())))
        await asyncio.sleep(10)
        return fired

    assert run(main) == [("at", 2.5), ("later", 5.0)]


def test_cancelled_timer_does_not_advance_clock() -> None:
    async def main() -> float:
        loop = asyncio.get_running_loop()
        loop.call_later(1000, lambda: None).cancel()
        await asyncio.sleep(1)
        return loop.time()

    assert run(main) == 1.0


def test_cancelled_timer_alone_does_not_jump() -> None:
    async def main() -> float:
        loop = asyncio.get_running_loop()
        handle = loop.call_later(1000, lambda: None)
        await asyncio.sleep(0)
        handle.cancel()
        await asyncio.sleep(0)
        return loop.time()

    assert run(main) == 0.0


def test_sleep_zero_does_not_advance_time() -> None:
    async def main() -> float:
        for _ in range(5):
            await asyncio.sleep(0)
        return asyncio.get_running_loop().time()

    assert run(main) == 0.0


def test_sequential_and_nested_sleeps_accumulate() -> None:
    async def inner() -> None:
        await asyncio.sleep(2)
        await asyncio.sleep(3)

    async def main() -> float:
        await asyncio.sleep(1)
        await inner()
        await asyncio.sleep(4)
        return asyncio.get_running_loop().time()

    assert run(main) == 10.0


def test_deadlock_raises_instead_of_blocking() -> None:
    async def main() -> None:
        await asyncio.get_running_loop().create_future()

    with pytest.raises(SimulationDeadlock):
        run(main)


def test_threadsafe_wakeup_is_still_polled() -> None:
    async def main() -> int:
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[int] = loop.create_future()
        loop.call_soon_threadsafe(fut.set_result, 42)
        return await fut

    assert run(main) == 42


def test_cpython_adapted_code_carries_psf_notice() -> None:
    """AGENTS.md §3: code adapted from CPython keeps the PSF notice."""
    import inspect
    from pathlib import Path

    import desyncio.loop as loop_module

    notice = Path(__file__).resolve().parent.parent / "LICENSES" / "PSF.txt"
    assert notice.is_file()
    assert "PYTHON SOFTWARE FOUNDATION LICENSE VERSION 2" in notice.read_text(
        encoding="utf-8"
    )

    assert "LICENSES/PSF.txt" in inspect.getsource(loop_module)

    doc = loop_module.__doc__ or ""
    assert "No CPython code is copied" not in doc
    assert "not a substantive copy" not in doc
