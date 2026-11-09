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


@pytest.mark.parametrize("start", [0.0, 100.0])
def test_time_starts_at_given_start(start: float) -> None:
    async def main() -> float:
        return asyncio.get_running_loop().time()

    assert run(main, start=start) == start


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


def test_deadlock_raises_instead_of_blocking() -> None:
    async def main() -> None:
        await asyncio.get_running_loop().create_future()

    with pytest.raises(SimulationDeadlock):
        run(main)


def test_threadsafe_wakeup_from_loop_thread_is_polled() -> None:
    """call_soon_threadsafe from the loop's own thread works.

    This does NOT test cross-thread wake-ups; those are unsupported (D006), see
    ``test_worker_thread_wakeup_is_a_deadlock``.
    """

    async def main() -> int:
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[int] = loop.create_future()
        loop.call_soon_threadsafe(fut.set_result, 42)
        return await fut

    assert run(main) == 42


def test_worker_thread_wakeup_is_a_deadlock() -> None:
    """D006: a real thread's ``call_soon_threadsafe`` is not waited for.

    The worker is blocked on a ``threading.Event`` that the test only sets
    *after* ``run`` has raised. So the worker provably has not woken the loop
    when the loop gives up: main awaits a future with no timers pending, the
    loop sees nothing to do and raises ``SimulationDeadlock`` at once instead
    of waiting for the thread. No sleeps, no wall-clock bounds.
    """
    import threading

    release = threading.Event()
    worker_done = threading.Event()
    threads: list[threading.Thread] = []

    async def main() -> int:
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[int] = loop.create_future()

        def worker() -> None:
            release.wait()
            try:
                loop.call_soon_threadsafe(fut.set_result, 1)
            except RuntimeError:
                pass  # loop already closed: expected, the deadlock came first
            worker_done.set()

        t = threading.Thread(target=worker)
        threads.append(t)
        t.start()
        return await fut

    try:
        with pytest.raises(SimulationDeadlock):
            run(main)
    finally:
        release.set()  # only now may the worker proceed
        threads[0].join()
    assert worker_done.is_set()


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


# On the integration branch before LOOP-09, ``len(vars(VirtualTimeLoop()))``
# was 28 (measured with CPython 3.13), i.e. 2 more than the stock
# ``SelectorEventLoop`` (26 on 3.13). The stock loop's own count differs between
# interpreters (27 on 3.14), so the limit is relative to the running
# interpreter's baseline. Do not add instance attributes to the loop: put new
# per-loop state on ``loop._state`` instead.
_EXTRA_LOOP_ATTRS = 2


def test_loop_instance_dict_is_not_larger_than_before_loop_09() -> None:
    """Guard the loop's per-step speed through its instance-dict size.

    CPython 3.13 keeps an object's attributes "inline" (no separate dict,
    keys shared by every instance of the class) only up to a fixed number of
    attributes. Past that limit the instance gets a real dict, and attribute
    loads such as ``self._ready`` or ``self._scheduled`` in the stock
    ``BaseEventLoop._run_once`` lose their specialised fast path. LOOP-09
    first added three attributes (28 -> 31) and every ``asyncio.sleep``
    became ~10% slower with tracing off. This count is deterministic, unlike
    a timing test, so it is the regression guard; the benchmark numbers are
    in docs/how-the-loop-works.md section 10.
    """
    stock = asyncio.SelectorEventLoop()
    try:
        limit = len(vars(stock)) + _EXTRA_LOOP_ATTRS
    finally:
        stock.close()
    loop = VirtualTimeLoop()
    try:
        assert len(vars(loop)) <= limit, sorted(vars(loop))
    finally:
        loop.close()
