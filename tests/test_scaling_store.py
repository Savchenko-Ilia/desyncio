"""PERF-02 (D075): cancelling many blocked store waiters is linear, not O(n²).

Hodos measured that cancelling n tasks parked on one store (at the teardown
after ``until``, or mid-run in a ``TaskGroup`` abort or a mass interrupt) is
quadratic. The cause was asyncio's own waiter cleanup: ``Queue.get``/``put``
and ``Condition.wait`` remove a cancelled waiter's future with
``deque.remove`` (a scan), and ``Condition`` rescans its deque to pass a
notification on.

The scaling tests count work deterministically instead of timing it: every
future created by the loop is a ``_CountingFuture`` that counts the calls a
scan makes on it (``==`` from ``deque.remove``, ``done()``/``cancelled()``
from the wake-up and rescan loops). Linear cleanup touches each future O(1)
times, so going from n to 4n waiters multiplies the count by about 4; the old
quadratic cleanup multiplied it by about 16. The threshold, 8, is 2x away from
both.

Because the stores now park their own futures, the differential tests check
that nothing else changed: random scenarios with puts, gets, cancellations at
the same instants as wake-ups, and shutdowns give the same event log as the
asyncio originals (``asyncio.Queue``, ``asyncio.PriorityQueue`` and the
pre-PERF-02 ``FilterStore`` built on ``asyncio.Condition``).
"""

import asyncio
import itertools
import random
from collections import deque
from collections.abc import Callable
from typing import Any

import pytest

import desyncio
import desyncio.store
from desyncio import sim
from desyncio.loop import VirtualTimeLoop
from desyncio.store import FilterStore, PriorityStore, Store

# --- scaling: a deterministic count of the work done on waiter futures --------


class _CountingFuture(asyncio.Future[Any]):
    """A future that counts how often a scan looks at it."""

    touches = 0

    def done(self) -> bool:
        _CountingFuture.touches += 1
        return super().done()

    def cancelled(self) -> bool:
        _CountingFuture.touches += 1
        return super().cancelled()

    def __eq__(self, other: object) -> bool:  # deque.remove compares with ==
        _CountingFuture.touches += 1
        return self is other

    __hash__ = asyncio.Future.__hash__


WAITERS = {
    "Store.get": (Store, "get"),
    "Store.put": (Store, "put"),
    "PriorityStore.get": (PriorityStore, "get"),
    "PriorityStore.put": (PriorityStore, "put"),
    "FilterStore.get": (FilterStore, "get"),
    "FilterStore.put": (FilterStore, "put"),
}


def _touches(case: str, n: int, mid_run: bool) -> int:
    """Park ``n`` waiters on one store, cancel them all, return the count.

    ``mid_run``: cancel every waiter at once (odd ones first, so the order is
    not the park order), inside the run. Otherwise the run ends at ``until``
    with the waiters still parked and the teardown cancels them.
    """
    kind, side = WAITERS[case]

    async def waiter(store: Any) -> None:
        if side == "put":
            await store.put("x")
        elif kind is FilterStore:
            await store.get(filter=lambda item: False)
        else:
            await store.get()

    async def main() -> None:
        store: Any = kind(capacity=1) if side == "put" else kind()
        if side == "put":
            await store.put("full")
        tasks = [sim.spawn(waiter(store)) for _ in range(n)]
        await sim.timeout(1)  # every waiter is parked now
        if mid_run:
            for task in tasks[1::2] + tasks[::2]:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            assert all(task.cancelled() for task in tasks)
        else:
            await sim.timeout(10)  # still waiting when the run ends at until

    _CountingFuture.touches = 0
    desyncio.run(main(), until=5)
    return _CountingFuture.touches


@pytest.mark.parametrize("mid_run", [False, True], ids=["teardown", "mid-run"])
@pytest.mark.parametrize("case", list(WAITERS))
def test_cancelling_n_waiters_is_linear(
    case: str, mid_run: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    def create_future(self: VirtualTimeLoop) -> asyncio.Future[Any]:
        return _CountingFuture(loop=self)

    monkeypatch.setattr(VirtualTimeLoop, "create_future", create_future)
    n = 250
    small, large = _touches(case, n, mid_run), _touches(case, 4 * n, mid_run)
    ratio = large / small
    # linear: about 4; quadratic: about 16
    assert ratio < 8, f"{case}: {small} -> {large} touches (x{ratio:.1f}) for n -> 4n"


# --- semantics: same event log as the asyncio originals ---------------------


class _ConditionFilterStore:
    """The pre-PERF-02 ``FilterStore`` waiting logic, on ``asyncio.Condition``."""

    def __init__(self, capacity: float) -> None:
        self._capacity = capacity
        self._items: deque[int] = deque()
        self._cond = asyncio.Condition()

    def __len__(self) -> int:
        return len(self._items)

    async def put(self, item: int) -> None:
        async with self._cond:
            await self._cond.wait_for(lambda: len(self._items) < self._capacity)
            self._items.append(item)
            self._cond.notify_all()

    async def get(self, filter: Callable[[int], bool] | None = None) -> int:  # noqa: A002
        taken: list[int] = []

        def take_first_match() -> bool:
            for index, candidate in enumerate(self._items):
                if filter is None or filter(candidate):
                    del self._items[index]
                    taken.append(candidate)
                    return True
            return False

        async with self._cond:
            await self._cond.wait_for(take_first_match)
            self._cond.notify_all()
        return taken[0]


class _Adapter:
    """One put/get/shutdown interface over a store or its asyncio original."""

    def __init__(self, flavour: str, ours: bool, capacity: float) -> None:
        self.flavour = flavour
        self.seq = itertools.count()
        size = 0 if capacity == float("inf") else int(capacity)
        self.store: Any
        if flavour == "fifo":
            self.store = Store(capacity) if ours else asyncio.Queue(size)
        elif flavour == "priority" and ours:
            self.store = PriorityStore(capacity)
        elif flavour == "priority":
            self.store = asyncio.PriorityQueue(size)
        elif ours:
            self.store = FilterStore(capacity)
        else:
            self.store = _ConditionFilterStore(capacity)
        self.ours = ours

    async def put(self, item: int) -> None:
        if self.flavour != "priority":
            await self.store.put(item)
        elif self.ours:
            await self.store.put(item, priority=item % 3)
        else:  # PriorityStore's entry, sequence taken when put is called
            await self.store.put((item % 3, next(self.seq), item))

    async def get(self, parity: int | None) -> int:
        if self.flavour == "filter":
            match = None if parity is None else (lambda item: item % 2 == parity)
            return int(await self.store.get(filter=match))
        got = await self.store.get()
        return int(got[2]) if isinstance(got, tuple) else int(got)

    def size(self) -> int:
        if isinstance(self.store, asyncio.Queue):
            return self.store.qsize()
        return len(self.store)


def _event_log(flavour: str, ours: bool, seed: int) -> list[tuple[Any, ...]]:
    """Run one random scenario and return what every task saw, and when."""
    rng = random.Random(seed)
    capacity = rng.choice([1, 2, 5, float("inf")])
    shutdown_at = None if flavour == "filter" else rng.choice([None, 15, 25])
    immediate = rng.random() < 0.5
    log: list[tuple[Any, ...]] = []

    async def actor(store: _Adapter, i: int, is_put: bool, start: int) -> None:
        await sim.timeout(start)
        try:
            if is_put:
                await store.put(i)
                log.append((sim.now, i, "put", store.size()))
            else:
                parity = rng.choice([None, 0, 1])
                log.append((sim.now, i, "got", await store.get(parity), store.size()))
        except asyncio.CancelledError:
            if sim.now < 40:  # teardown cancels leftovers in address order
                log.append((sim.now, i, "cancelled", store.size()))
            raise
        except asyncio.QueueShutDown:
            log.append((sim.now, i, "shut down", store.size()))

    async def cancel_at(task: asyncio.Task[None], at: int) -> None:
        await sim.timeout(at)
        task.cancel()

    async def main() -> None:
        store = _Adapter(flavour, ours, capacity)
        for i in range(200):
            start = rng.randrange(20)
            task = sim.spawn(actor(store, i, rng.random() < 0.5, start))
            if rng.random() < 0.5:
                # often while it waits, sometimes at the instant it is woken
                sim.spawn(cancel_at(task, start + rng.randrange(6)))
        if shutdown_at is not None:
            await sim.timeout(shutdown_at)
            store.store.shutdown(immediate)
        await sim.timeout(40 - sim.now)
        log.append(("left", store.size()))

    desyncio.run(main(), until=50)
    return log


@pytest.mark.parametrize("compact_min", [32, 1], ids=["default", "compact-often"])
@pytest.mark.parametrize("flavour", ["fifo", "priority", "filter"])
def test_same_event_log_as_the_asyncio_original(
    flavour: str, compact_min: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    # compact-often rebuilds the waiter deque at almost every cancellation, so
    # the rebuild path is exercised, not only the lazy skipping.
    monkeypatch.setattr(desyncio.store, "_COMPACT_MIN", compact_min)
    outcomes: set[str] = set()
    for seed in range(15):
        expected = _event_log(flavour, ours=False, seed=seed)
        assert _event_log(flavour, ours=True, seed=seed) == expected, f"seed {seed}"
        outcomes |= {entry[2] for entry in expected if len(entry) > 2}
    # the scenarios really mix outcomes, so the comparison means something
    want = {"got", "put", "cancelled"}
    if flavour != "filter":
        want.add("shut down")
    assert outcomes == want


def test_store_rejects_a_second_event_loop() -> None:
    # asyncio.Queue was bound to the first loop it parked a task in; the
    # stores keep that rule now that they park their own futures.
    for store in (Store[int](), FilterStore[int]()):

        async def park(s: Any = store) -> None:
            filtered = isinstance(s, FilterStore)
            task = sim.spawn(s.get(filter=bool) if filtered else s.get())
            await sim.timeout(1)
            if task.done():
                task.result()  # re-raise what the parking task hit
            task.cancel()

        desyncio.run(park())  # binds the store to this run's loop
        with pytest.raises(RuntimeError, match="different event loop"):
            desyncio.run(park())
