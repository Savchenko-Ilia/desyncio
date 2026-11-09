"""Deterministic tie-breaking: timers fire in (time, priority, sequence) order."""

import asyncio
import itertools
import math
from collections.abc import Callable, Coroutine
from typing import Any

import pytest
from hypothesis import given
from hypothesis import strategies as st

from desyncio import scheduling
from desyncio.loop import VirtualTimeLoop
from desyncio.scheduling import PriorityTimerHandle


def run[T](main: Callable[[], Coroutine[Any, Any, T]]) -> T:
    """Run ``main()`` to completion on a fresh VirtualTimeLoop."""
    with asyncio.Runner(loop_factory=VirtualTimeLoop) as runner:
        return runner.run(main())


def test_thousand_sleepers_wake_in_creation_order() -> None:
    n = 1000

    async def main() -> list[int]:
        woke: list[int] = []

        async def sleeper(i: int) -> None:
            await asyncio.sleep(5)
            woke.append(i)

        tasks = [asyncio.create_task(sleeper(i)) for i in range(n)]
        await asyncio.gather(*tasks)
        return woke

    assert run(main) == list(range(n))


def test_thousand_call_at_same_time_fire_in_schedule_order() -> None:
    n = 1000

    async def main() -> list[int]:
        loop = asyncio.get_running_loop()
        fired: list[int] = []
        for i in range(n):
            loop.call_at(7.0, fired.append, i)
        await asyncio.sleep(10)
        return fired

    assert run(main) == list(range(n))


def test_lower_priority_fires_first_at_same_time() -> None:
    async def main() -> list[str]:
        loop = asyncio.get_running_loop()
        assert isinstance(loop, VirtualTimeLoop)
        fired: list[str] = []
        loop.call_at(1.0, fired.append, "default-first")
        loop.call_at_priority(1.0, 5, fired.append, "p5")
        loop.call_at_priority(1.0, -1, fired.append, "p-1")
        loop.call_at(1.0, fired.append, "default-second")
        await asyncio.sleep(2)
        return fired

    assert run(main) == ["p-1", "default-first", "default-second", "p5"]


def test_stock_apis_use_default_priority_zero_and_rising_sequence() -> None:
    async def main() -> list[PriorityTimerHandle]:
        loop = asyncio.get_running_loop()
        handles = [
            loop.call_later(1, lambda: None),
            loop.call_at(1.0, lambda: None),
        ]
        async with asyncio.timeout(1) as cm:
            handles.append(getattr(cm, "_timeout_handler"))  # noqa: B009
            await asyncio.sleep(0)
        sleeper = asyncio.ensure_future(asyncio.sleep(1))
        await asyncio.sleep(0)
        out = [h for h in handles if isinstance(h, PriorityTimerHandle)]
        sleeper.cancel()
        return out

    handles = run(main)
    assert len(handles) == 3
    assert [h.priority for h in handles] == [0, 0, 0]
    seqs = [h.sequence for h in handles]
    assert seqs == sorted(seqs) and len(set(seqs)) == 3


def test_sequence_starts_at_zero_per_loop_and_is_a_plain_itertools_count() -> None:
    # Reads PriorityTimerHandle.sequence (public property) and the loop's
    # private _state, because the stdlib-counter claim has no other surface.
    assert not hasattr(scheduling, "SequenceCounter")  # principle 6
    loop_a, loop_b = VirtualTimeLoop(), VirtualTimeLoop()
    try:
        assert type(loop_a._state.sequence) is type(itertools.count())
        a = [loop_a.call_at(1.0, lambda: None) for _ in range(3)]
        b = [loop_b.call_at(1.0, lambda: None) for _ in range(2)]
        a.append(loop_a.call_at(1.0, lambda: None))
        assert [h.sequence for h in a] == [0, 1, 2, 3]  # type: ignore[attr-defined]
        assert [h.sequence for h in b] == [0, 1]  # type: ignore[attr-defined]
    finally:
        loop_a.close()
        loop_b.close()


def test_priority_must_be_int() -> None:
    loop = VirtualTimeLoop()
    try:
        for bad in (1.5, "1", True, None):
            try:
                loop.call_at_priority(1.0, bad, lambda: None)  # type: ignore[arg-type]
            except TypeError:
                continue
            raise AssertionError(f"accepted {bad!r}")
    finally:
        loop.close()


def test_debug_source_traceback_points_at_caller() -> None:
    async def main() -> list[str]:
        loop = asyncio.get_running_loop()
        loop.set_debug(True)
        h = loop.call_later(1, lambda: None)
        h2 = loop.call_at(1.0, lambda: None)
        tb = getattr(h, "_source_traceback")  # noqa: B009
        tb2 = getattr(h2, "_source_traceback")  # noqa: B009
        return [tb[-1].name, tb2[-1].name]

    assert run(main) == ["main", "main"]


# --- clock-resolution window ------------------------------------------------


@pytest.mark.parametrize("late", [1.0 + 5e-10, math.nextafter(1.0, math.inf)])
def test_near_simultaneous_timers_each_see_their_own_time(late: float) -> None:
    """call_at(1.0) and a later float (5e-10 away, or the adjacent float) must
    not share one instant."""

    async def main() -> list[tuple[str, float]]:
        loop = asyncio.get_running_loop()
        seen: list[tuple[str, float]] = []
        loop.call_at(late, lambda: seen.append(("late", loop.time())))
        loop.call_at(1.0, lambda: seen.append(("early", loop.time())))
        await asyncio.sleep(2)
        return seen

    assert run(main) == [("early", 1.0), ("late", late)]


def test_exact_same_time_timers_share_one_instant() -> None:
    async def main() -> list[float]:
        loop = asyncio.get_running_loop()
        seen: list[float] = []
        for _ in range(3):
            loop.call_at(4.0, lambda: seen.append(loop.time()))
        await asyncio.sleep(5)
        return seen

    assert run(main) == [4.0, 4.0, 4.0]


# --- overdue and cancelled timers (carried over from LOOP-02 review) -------


def test_overdue_timers_fire_without_moving_the_clock() -> None:
    async def main() -> tuple[list[tuple[str, float]], float]:
        loop = asyncio.get_running_loop()
        await asyncio.sleep(10)
        fired: list[tuple[str, float]] = []
        loop.call_at(3, lambda: fired.append(("three", loop.time())))
        loop.call_at(-1, lambda: fired.append(("neg", loop.time())))
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        return fired, loop.time()

    fired, now = run(main)
    assert fired == [("neg", 10.0), ("three", 10.0)]
    assert now == 10.0


def test_cancelled_timers_in_middle_of_heap_are_skipped() -> None:
    async def main() -> list[int]:
        loop = asyncio.get_running_loop()
        fired: list[int] = []
        handles = [loop.call_at(float(i), fired.append, i) for i in range(1, 21)]
        for h in handles[3:15]:
            h.cancel()
        await asyncio.sleep(30)
        return fired

    assert run(main) == [1, 2, 3, 16, 17, 18, 19, 20]


def test_many_cancelled_timers_trigger_heap_rebuild_and_keep_order() -> None:
    async def main() -> list[int]:
        loop = asyncio.get_running_loop()
        fired: list[int] = []
        handles = [loop.call_at(1.0, fired.append, i) for i in range(300)]
        for i, h in enumerate(handles):
            if i % 3:
                h.cancel()  # >100 cancelled and >50%: _run_once rebuilds the heap
        await asyncio.sleep(2)
        return fired

    assert run(main) == list(range(0, 300, 3))


# --- determinism ------------------------------------------------------------


def _trace(schedule: list[tuple[float, int]]) -> list[tuple[float, int]]:
    """Run one schedule on a fresh loop; return (time seen, label) per firing."""
    loop = VirtualTimeLoop()
    trace: list[tuple[float, int]] = []
    try:
        for label, (when, prio) in enumerate(schedule):

            def fire(lab: int = label) -> None:
                trace.append((loop.time(), lab))

            loop.call_at_priority(when, prio, fire)
        last = max((w for w, _ in schedule), default=0)
        loop.run_until_complete(asyncio.sleep(last + 1))
    finally:
        loop.close()
    return trace


@given(
    st.lists(
        st.tuples(
            st.sampled_from([0.0, 1.0, 2.0, 2.5, 3.0, 10.0]),
            st.integers(min_value=-3, max_value=3),
        ),
        max_size=60,
    )
)
def test_fire_order_is_time_priority_sequence(
    schedule: list[tuple[float, int]],
) -> None:
    first = _trace(schedule)
    second = _trace(schedule)
    assert first == second
    expected = sorted(range(len(schedule)), key=lambda i: (*schedule[i], i))
    assert [lab for _, lab in first] == expected
    assert [t for t, _ in first] == [schedule[i][0] for i in expected]


@given(
    st.lists(
        st.tuples(
            st.floats(min_value=0, max_value=100, allow_nan=False),
            st.integers(min_value=-2, max_value=2),
        ),
        max_size=40,
    )
)
def test_fire_order_with_arbitrary_float_times(
    schedule: list[tuple[float, int]],
) -> None:
    first = _trace(schedule)
    assert first == _trace(schedule)
    expected = sorted(range(len(schedule)), key=lambda i: (*schedule[i], i))
    assert [lab for _, lab in first] == expected
    # every timer observed exactly its own time
    assert [t for t, _ in first] == [schedule[i][0] for i in expected]
