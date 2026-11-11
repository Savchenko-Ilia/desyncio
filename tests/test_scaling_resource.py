"""PERF-01 (D075): cancelling many waiters on one resource costs O(n), not O(n²).

The measure is deterministic: the number of profiler events (Python calls and
returns, C calls such as ``Future.done()``, generator resumptions) counted with
``sys.setprofile`` while the waiters are cancelled. It is compared between n
and 4n waiters. Linear work gives a ratio of about 4, quadratic work about 16;
the threshold, 8, is midway on a log scale, so neither noise nor a constant
overhead can move a result across it. No wall clock is read.

Blind spot: work done inside one C call (a ``deque.remove`` or a
``heapify``) is a single event, and so is a Python-level scan that only
compares with ``is`` (no call, no comparison method). The old quadratic paths
were Python-level calls (``Semaphore._wake_up_next``,
``PriorityResource._grant_free_units`` and ``_blocked_by_waiter``), which
this measure sees. For the rest a second measure counts work on the waiter
containers: comparisons (futures and priorities whose
``__eq__``/``__lt__``/``__le__`` count) plus the elements yielded by
iterating ``_waiters`` and the length of every rebuilt ``_waiters`` (a
resource subclass swaps in counting containers), compared between n and 4n
the same way, with the waiters cancelled newest first both all at once and
one at a time (reneging, where a cleanup that rebuilds or scans the queue on
every cancellation is quadratic).

Still invisible, documented and not tested: per-cancel work done entirely in
C that neither compares, nor iterates ``_waiters`` through its ``__iter__``,
nor assigns a new ``_waiters``; for example ``self._waiters.rotate(k)``, a
list ``del`` shifting memory, or ``len(list(deque.__iter__(self._waiters)))``
on every cancellation. These are contrived; nothing in the code does them.

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
from collections import deque
from collections.abc import Callable, Iterator
from contextlib import AbstractAsyncContextManager
from types import FrameType
from typing import Any

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

import desyncio
from desyncio import Interrupt, PreemptiveResource, PriorityResource, Resource, sim
from desyncio.events import Event, ResourceChanged

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


# --- work no profiler event sees ------------------------------------------
#
# The profiler counts a C call as one event however long it runs, so a
# ``deque.remove(fut)`` or ``list.remove(entry); heapify(...)`` per cancelled
# waiter (both linear scans in C) looks linear above. What those scans do
# for each element is compare it: ``==`` on futures for a deque, and ``==``
# and ``<`` on the priorities inside the heap's tuples. So this test gives
# every element a Python ``__eq__``/``__lt__``/``__le__`` that counts, which
# makes those comparisons visible, deterministically.
#
# A scan that compares with ``is`` (``for i, f in enumerate(waiters): if f is
# fut``), or a rebuild of the queue on every cancellation, compares nothing.
# So the resource also counts, through ``_CountingWaiters``, the elements
# yielded by iterating its ``_waiters`` and the length of every new
# ``_waiters`` it builds.


class CountingFuture(asyncio.Future[None]):
    """A future that counts how often it is compared with ``==``."""

    compared = 0

    def __eq__(self, other: object) -> bool:
        CountingFuture.compared += 1
        return self is other

    __hash__ = object.__hash__


class CountingPriority(float):
    """A priority that counts the comparisons the heap makes on it."""

    compared = 0

    def __eq__(self, other: object) -> bool:
        CountingPriority.compared += 1
        return float(self) == other

    def __lt__(self, other: float) -> bool:
        CountingPriority.compared += 1
        return float(self) < other

    def __le__(self, other: float) -> bool:
        CountingPriority.compared += 1
        return float(self) <= other

    __hash__ = float.__hash__


class Touched:
    """Elements of a waiter container stepped over (iterated or rebuilt)."""

    count = 0


class _CountingDeque(deque[Any]):
    """``Resource``'s waiter deque, counting the elements its iterator yields
    (a ``for`` loop, a generator expression, ``deque(...)`` built from it)."""

    def __iter__(self) -> Iterator[Any]:
        for element in super().__iter__():
            Touched.count += 1
            yield element


class _CountingList(list[Any]):
    """The priority classes' waiter heap, counting the elements its iterator
    yields. ``heapq`` works on it unchanged (and does not iterate)."""

    def __iter__(self) -> Iterator[Any]:
        for element in super().__iter__():
            Touched.count += 1
            yield element


class _CountingWaiters:
    """Makes ``_waiters`` a property: whatever deque or list the resource
    stores there (at creation, or in a rebuild) is kept as a counting copy,
    and every rebuild adds its length to ``Touched.count``. O(1) operations
    (``append``, ``popleft``, ``heappush``, ``heappop``, indexing, ``len``)
    add nothing."""

    @property
    def _waiters(self) -> Any:
        return self.__dict__["_waiters"]

    @_waiters.setter
    def _waiters(self, value: Any) -> None:
        if "_waiters" in self.__dict__:
            Touched.count += len(value)  # a rebuild
        if type(value) is deque:
            value = _CountingDeque(value)
        elif type(value) is list:
            value = _CountingList(value)
        self.__dict__["_waiters"] = value


class _CountingResource(_CountingWaiters, Resource):
    pass


class _CountingPriorityResource(_CountingWaiters, PriorityResource):
    pass


class _CountingPreemptiveResource(_CountingWaiters, PreemptiveResource):
    pass


COUNTING: dict[Callable[[], AnyResource], Callable[[], AnyResource]] = {
    Resource: _CountingResource,
    PriorityResource: _CountingPriorityResource,
    PreemptiveResource: _CountingPreemptiveResource,
}


def work_cancelling_newest_first(
    make: Callable[[], AnyResource], n: int, *, one_at_a_time: bool
) -> int:
    """One holder and n waiters; the waiters are cancelled newest first while
    the holder still holds (so no release passes over them), and their
    cleanups run. Returns the comparisons and container elements touched
    meanwhile (see above), plus n.

    Newest first is the worst order for a removal that searches from the
    head: each search walks past every waiter still queued. All at once, the
    cleanups run after every waiter is cancelled; one at a time (reneging),
    each cleanup runs (``asyncio.sleep(0)``) before the next waiter gives up,
    so a cleanup that rebuilds the queue every time copies the whole queue
    still waiting. The ``+ n`` is the one unit of work each cancellation
    costs at the least; it keeps the ratio defined if nothing else counts."""

    async def use(res: AnyResource, priority: float) -> None:
        async with request(res, priority):
            await sim.timeout(1e9)

    async def main() -> int:
        loop = asyncio.get_running_loop()
        # Instance attribute: shadows the method for this run's loop only.
        loop.create_future = lambda: CountingFuture(loop=loop)  # type: ignore[method-assign]
        res = COUNTING[make]()
        holder = sim.spawn(use(res, CountingPriority(0)))
        # Each waiter has its own priority object, so no comparison between
        # two entries is short-cut by identity.
        waiters = [sim.spawn(use(res, CountingPriority(1))) for _ in range(n)]
        await sim.timeout(1)
        assert res.count == 1 and res.queue_length == n
        CountingFuture.compared = CountingPriority.compared = Touched.count = 0
        for task in reversed(waiters):
            task.cancel()
            if one_at_a_time:
                await asyncio.sleep(0)
                assert task.done()  # its cleanup has run
        await asyncio.gather(*waiters, return_exceptions=True)
        work = CountingFuture.compared + CountingPriority.compared + Touched.count
        assert res.count == 1 and res.queue_length == 0
        # The swap is still in place: the count saw every container.
        assert isinstance(res._waiters, _CountingDeque | _CountingList)
        holder.cancel()
        return work + n

    return desyncio.run(main())


@ALL
@pytest.mark.parametrize("one_at_a_time", [False, True], ids=["at_once", "reneging"])
def test_cancelling_waiters_makes_no_linear_search_per_waiter(
    make: Callable[[], AnyResource], one_at_a_time: bool
) -> None:
    assert (
        ratio(
            lambda n: work_cancelling_newest_first(make, n, one_at_a_time=one_at_a_time)
        )
        < THRESHOLD
    )


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


@ALL
@given(st.integers(0, 2**32))
# 60 derandomized hypothesis draws per class, not the fixed seeds 0..59 (D042).
@settings(max_examples=60, derandomize=True, deadline=None)
def test_stress_keeps_counters_events_and_fifo_consistent(
    make: Callable[..., AnyResource], seed: int
) -> None:
    """Random arrivals, holds, timeouts and cancels (some at the instant of a
    release). Checked:

    * at every monitor call 0 <= count <= capacity and queue_length >= 0 (a
      grant that briefly saw a phantom free unit fails this), and everything
      is back to zero at the end;
    * the ``ResourceChanged`` events report exactly the monitor's
      ``(count, queue_length)`` sequence, with the capacity;
    * FIFO among equal priorities (all of them for ``Resource``): the
      requests served at one priority are served in the order they were made.
      A request that gave up is never served, so it cannot break this; one
      skipped while still live would. "Served" is when the body starts. For
      ``Resource`` that is exactly grant order (a granted waiter that has not
      resumed still counts as queued). The priority classes let a newcomer
      take a free unit past such a waiter, so there two requests served at
      the same instant may start in either order.
    """
    rng = random.Random(seed)
    capacity = rng.randint(1, 3)
    priority_class = make is not Resource

    # Recorded, not asserted: a monitor's exception goes to the loop's
    # exception handler (D023 v2) and would not fail the test.
    states: list[tuple[int, int]] = []
    bad: list[tuple[float, int, int]] = []

    def check(r: AnyResource) -> None:
        states.append((r.count, r.queue_length))
        if not (0 <= r.count <= r.capacity and r.queue_length >= 0):
            bad.append((sim.now, r.count, r.queue_length))

    requested = 0
    # priority -> (time, request number) of each request served, in service order
    served: dict[float, list[tuple[float, int]]] = {}

    async def main() -> tuple[int, int]:
        res = make(capacity, monitor=check)

        async def use(priority: float, hold: float, patience: float) -> None:
            nonlocal requested
            with contextlib.suppress(TimeoutError, Interrupt):
                async with asyncio.timeout(patience):
                    number = requested  # no await between this and the request
                    requested += 1
                    async with request(res, priority):
                        key = priority if priority_class else 0
                        served.setdefault(key, []).append((sim.now, number))
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

    events: list[Event] = []
    assert desyncio.run(main(), subscribers=[events.append]) == (0, 0)
    assert bad == []
    changes = [e for e in events if isinstance(e, ResourceChanged)]
    assert [(e.in_use, e.queue_len) for e in changes] == states
    assert all(e.capacity == capacity for e in changes)
    for entries in served.values():
        if priority_class:
            entries = sorted(entries)  # free order within one instant
        numbers = [number for _, number in entries]
        assert numbers == sorted(numbers)
