"""PERF-01 (D075): cancelling many waiters on one resource costs O(n), not O(n²).

The measure is deterministic: the number of profiler events (Python calls and
returns, C calls such as ``Future.done()``, generator resumptions) counted with
``sys.setprofile`` while the waiters are cancelled. It is compared between n
and 4n waiters. Linear work gives a ratio of about 4, quadratic work about 16;
the threshold, 8, is midway on a log scale, so neither noise nor a constant
overhead can move a result across it. No wall clock is read.

Blind spot: work done inside one C call (a ``deque.remove`` or a list ``del``
shifting memory) is a single event. The quadratic paths this guards against
were Python-level scans (``Semaphore._wake_up_next``,
``PriorityResource._grant_free_units`` and ``_blocked_by_waiter``), which
this measure sees.

Scenarios: the holder releases while every waiter has just been cancelled
(the worst order for the old code, made deterministic by cancelling in a fixed
order); a run cut at ``until`` with n waiters (Hodos's report, teardown order
is the runner's); and, on a ``PreemptiveResource``, a stream of arrivals each
better than every queued waiter (each one used to scan the whole queue).
"""

import asyncio
import contextlib
import random
import sys
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from types import FrameType
from typing import Any

import pytest

import desyncio
from desyncio import Interrupt, PreemptiveResource, PriorityResource, Resource, sim

N = 500  # compared with 4 * N; the new code needs well under 0.5 s in total
THRESHOLD = 8.0  # linear: ~4, quadratic: ~16

AnyResource = Resource | PriorityResource
ALL = pytest.mark.parametrize(
    "make",
    [Resource, PriorityResource, PreemptiveResource],
    ids=["Resource", "PriorityResource", "PreemptiveResource"],
)


class EventCounter:
    """Counts profiler events between ``start()`` and ``stop()``."""

    def __init__(self) -> None:
        self.events = 0

    def _hook(self, frame: FrameType, event: str, arg: Any) -> None:
        self.events += 1

    def start(self) -> None:
        sys.setprofile(self._hook)

    def stop(self) -> None:
        sys.setprofile(None)


def request(res: AnyResource, priority: float = 1) -> AbstractAsyncContextManager[Any]:
    """A plain ``async with res`` for ``Resource``, a priority request otherwise."""
    if isinstance(res, PriorityResource):
        return res.request(priority=priority)
    return res


def ratio(measure: Callable[[int], int]) -> float:
    small, large = measure(N), measure(4 * N)
    assert small > 0
    return large / small


def cancel_after_release(make: Callable[[], AnyResource], n: int) -> int:
    """One holder and n waiters; the holder is cancelled first, then every
    waiter, so the holder's release runs before any waiter's cleanup."""
    counter = EventCounter()

    async def use(res: AnyResource, priority: float) -> None:
        async with request(res, priority):
            await sim.timeout(1e9)

    async def main() -> None:
        res = make()
        holder = sim.spawn(use(res, 0))
        waiters = [sim.spawn(use(res, 1)) for _ in range(n)]
        await sim.timeout(1)
        assert res.count == 1 and res.queue_length == n
        counter.start()
        try:
            holder.cancel()
            for task in waiters:
                task.cancel()
            await asyncio.gather(holder, *waiters, return_exceptions=True)
        finally:
            counter.stop()
        assert res.count == 0 and res.queue_length == 0

    desyncio.run(main())
    return counter.events


def until_with_waiters(make: Callable[[], AnyResource], n: int) -> int:
    """Hodos's report: the run ends at ``until`` with one holder and n waiters;
    teardown cancels them all. Counted from just before ``until`` to the end
    of ``run`` (teardown included).

    The runner cancels leftover tasks in an address-dependent order, so how
    badly the old code did here depended on where the holder fell in that
    order (in trials its ratio ranged from about 4.5 to 25); the new code is
    linear in any order. ``cancel_after_release`` is the deterministic worst
    case."""
    counter = EventCounter()

    async def use(res: AnyResource) -> None:
        async with request(res):
            await sim.timeout(1e9)

    async def main() -> None:
        res = make()
        for _ in range(n + 1):
            sim.spawn(use(res))
        await sim.timeout(99)
        counter.start()
        await sim.timeout(10)

    try:
        desyncio.run(main(), until=100)
    finally:
        counter.stop()
    return counter.events


@ALL
def test_cancelling_waiters_after_the_release_is_linear(
    make: Callable[[], AnyResource],
) -> None:
    assert ratio(lambda n: cancel_after_release(make, n)) < THRESHOLD


@ALL
def test_run_cut_at_until_with_waiters_is_linear(
    make: Callable[[], AnyResource],
) -> None:
    assert ratio(lambda n: until_with_waiters(make, n)) < THRESHOLD


def test_preemptive_arrivals_better_than_every_waiter_are_linear() -> None:
    """Each preempt=True arrival on a busy ``PreemptiveResource`` asks whether
    a live waiter is at least as good; with every arrival better than all
    queued ones, the old ``any(...)`` scanned the whole queue each time."""

    def arrivals(n: int) -> int:
        counter = EventCounter()

        async def use(res: PreemptiveResource, priority: float) -> None:
            async with res.request(priority=priority):
                await sim.timeout(1e9)

        async def main() -> None:
            res = PreemptiveResource(1)
            sim.spawn(use(res, -1))  # nobody below can preempt it
            await sim.timeout(1)
            counter.start()
            try:
                for i in range(n):
                    sim.spawn(use(res, n - i))  # better than every waiter so far
                await sim.timeout(1)
            finally:
                counter.stop()
            assert res.count == 1 and res.queue_length == n

        desyncio.run(main(), until=10)
        return counter.events

    assert ratio(arrivals) < THRESHOLD


# --- behaviour of the lazy removal -------------------------------------------
#
# Cancelled waiters stay in the queue until a release or a newcomer passes
# them, and the queue is rebuilt once they are the majority. These pin that
# neither changes the order, the counters or what a newcomer sees.


@ALL
def test_order_and_counts_survive_many_cancellations(
    make: Callable[[], AnyResource],
) -> None:
    async def main() -> tuple[list[int], list[int]]:
        res = make()
        served: list[int] = []

        async def use(i: int) -> None:
            async with request(res):
                served.append(i)
                await sim.timeout(1)

        async def holder() -> None:
            async with request(res, 0):
                await sim.timeout(10)

        sim.spawn(holder())
        tasks = [sim.spawn(use(i)) for i in range(200)]
        await sim.timeout(1)
        # Cancel three of every four, the first ones included (the head).
        for i, task in enumerate(tasks):
            if i % 4 != 3:
                task.cancel()
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert res.queue_length == 50
        # The queue was rebuilt: it holds at most the live waiters plus slack.
        assert len(res._waiters) <= 2 * 50 + 32
        # A newcomer still queues behind the 50 live waiters (no barging).
        late = sim.spawn(use(1000))
        await asyncio.sleep(0)
        assert res.queue_length == 51
        await asyncio.gather(*tasks, late, return_exceptions=True)
        return served, [i for i in range(200) if i % 4 == 3]

    served, live = desyncio.run(main())
    assert served == [*live, 1000]


def test_resource_newcomer_skips_cancelled_head_and_takes_free_unit() -> None:
    """A unit freed while only cancelled waiters are queued goes to the next
    newcomer at once: the dead head does not count as someone queued."""

    async def main() -> list[tuple[float, int, int]]:
        res = Resource(1)
        seen: list[tuple[float, int, int]] = []

        async def wait() -> None:
            async with res:
                pass

        async with res:
            waiters = [sim.spawn(wait()) for _ in range(3)]
            await asyncio.sleep(0)
            for task in waiters:
                task.cancel()
            await asyncio.gather(*waiters, return_exceptions=True)
            seen.append((sim.now, res.count, res.queue_length))
        async with res:  # fast path: nothing live is queued
            seen.append((sim.now, res.count, res.queue_length))
        return seen

    assert desyncio.run(main()) == [(0, 1, 0), (0, 1, 0)]


def test_resource_newcomer_queues_behind_a_granted_waiter_that_has_not_resumed() -> (
    None
):
    """``Semaphore.locked()`` semantics, kept exactly: a waiter granted a unit
    but not resumed yet still counts as queued, so a newcomer in that window
    queues even though another unit is free; the granted waiter hands that
    unit on when it resumes."""

    async def main() -> tuple[list[tuple[str, float]], list[tuple[int, int]]]:
        states: list[tuple[int, int]] = []
        res = Resource(2, monitor=lambda r: states.append((r.count, r.queue_length)))
        served: list[tuple[str, float]] = []

        async def use(name: str, hold: float) -> None:
            async with res:
                served.append((name, sim.now))
                await sim.timeout(hold)

        async def late(name: str) -> None:
            # Two steps, so that this timer for t=1 is scheduled after the
            # holders' timers and fires after them (LOOP-03 order).
            await sim.timeout(0.5)
            await sim.timeout(0.5)
            await use(name, 1)

        # a and b hold until t=1; w queues at t=0. At t=1 the timers of a, b
        # and n fire in one loop iteration: a's release grants w (which only
        # resumes in the next iteration), b's release frees a unit, then n
        # arrives while w is still granted-but-not-resumed.
        tasks = [
            sim.spawn(use("a", 1)),
            sim.spawn(use("b", 1)),
            sim.spawn(use("w", 1)),
            sim.spawn(late("n")),
        ]
        await asyncio.gather(*tasks)
        return served, states

    served, states = desyncio.run(main())
    assert served == [("a", 0), ("b", 0), ("w", 1), ("n", 1)]
    assert states == [
        (1, 0),  # a
        (2, 0),  # b
        (2, 1),  # w queues
        (1, 1),  # a releases: w granted, not resumed
        (0, 1),  # b releases: one unit free, w still queued
        (0, 2),  # n queues behind w
        (1, 1),  # w resumes and hands the free unit to n
        (2, 0),  # n resumes
        (1, 0),
        (0, 0),
    ]


@pytest.mark.parametrize("seed", range(60))
@ALL
def test_seeded_stress_keeps_the_counters_consistent(
    make: Callable[..., AnyResource], seed: int
) -> None:
    """Random arrivals, holds, timeouts and cancels (some at the instant of a
    release): at every monitor call 0 <= count <= capacity and
    queue_length >= 0, and at the end everything is back to zero. (A grant
    that briefly saw a phantom free unit fails this.)"""
    rng = random.Random(seed)
    capacity = rng.randint(1, 3)

    # Recorded, not asserted: a monitor's exception goes to the loop's
    # exception handler (D023 v2) and would not fail the test.
    bad: list[tuple[float, int, int]] = []

    def check(r: AnyResource) -> None:
        if not (0 <= r.count <= r.capacity and r.queue_length >= 0):
            bad.append((sim.now, r.count, r.queue_length))

    async def main() -> tuple[int, int]:
        res = make(capacity, monitor=check)

        async def use(priority: float, hold: float, patience: float) -> None:
            with contextlib.suppress(TimeoutError, Interrupt):
                async with asyncio.timeout(patience):
                    async with request(res, priority):
                        await sim.timeout(hold)

        tasks: list[asyncio.Task[None]] = []
        for _ in range(rng.randint(5, 40)):
            if rng.random() < 0.6:
                await sim.timeout(rng.choice([0.5, 1, 2]))
            tasks.append(
                sim.spawn(
                    use(rng.randint(0, 3), rng.choice([0, 1, 2]), rng.choice([0, 1, 9]))
                )
            )
            if rng.random() < 0.3:
                victim = rng.choice(tasks)
                asyncio.get_running_loop().call_later(
                    rng.choice([0, 1, 2]), victim.cancel
                )
        await asyncio.gather(*tasks, return_exceptions=True)
        return res.count, res.queue_length

    assert desyncio.run(main()) == (0, 0)
    assert bad == []
