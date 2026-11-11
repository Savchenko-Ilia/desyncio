"""Tests for ``desyncio.store``: Store (STO-01), PriorityStore and FilterStore (STO-02).

Store and PriorityStore keep their items in ``asyncio.Queue`` and
``asyncio.PriorityQueue``; blocked callers wait in desyncio's own waiter FIFO
(D075), adapted from ``Queue.put``/``get`` with the same semantics.
FilterStore is the ``asyncio.Condition`` pattern on an ``asyncio.Lock`` with
that waiter FIFO. ``tests/test_scaling_store.py`` checks that the waiting
logic gives the same event log as the asyncio originals and scales linearly.
These tests pin the store contracts: the ``items`` mirror, the monitor hook,
the PriorityStore ordering contract, FilterStore's filtering, validation, and
that cancellation still loses and duplicates nothing in *virtual* time.

Behaviours shared by all three stores are written once and run for each kind
in ``KIND``. FilterStore has no ``shutdown()``, so tests that use it check
``hasattr`` first.
"""

import asyncio
import math
from typing import Any

import pytest

import desyncio
from desyncio import sim
from desyncio.interrupt import Interrupt
from desyncio.store import FilterStore, PriorityStore, Store

KIND = [Store, PriorityStore, FilterStore]


def get(store: Any, match: Any = None) -> Any:
    """Return a ``get`` coroutine that works for every kind of store.

    FilterStore takes a filter; the others do not. A FilterStore always gets
    one (``match``, or match-everything), so that its filtered path, the one
    that parks on the ``Condition``, is the code under test. ``match`` is
    ignored by Store and PriorityStore.
    """
    if isinstance(store, FilterStore):
        return store.get(filter=match or (lambda item: True))
    return store.get()


# --- items, len, capacity -----------------------------------------------


@pytest.mark.parametrize("kind", [Store, FilterStore])
def test_capacity_len_items_snapshot(kind: Any) -> None:
    async def main() -> None:
        s = kind(capacity=3)
        assert (s.capacity, len(s), s.items) == (3, 0, ())
        await s.put("a")
        await s.put("b")
        snap = s.items
        await s.put("c")
        assert snap == ("a", "b")  # a snapshot, not a live view
        assert await s.get() == "a"
        assert (len(s), s.items) == (2, ("b", "c"))
        assert kind().capacity == math.inf  # the default is unbounded
        with pytest.raises(AttributeError):
            s.items = ()
        with pytest.raises(AttributeError):
            s.capacity = 5

    desyncio.run(main())


# --- cancellation -------------------------------------------------------


@pytest.mark.parametrize("kind", KIND)
def test_getter_cancelled_at_the_instant_an_item_arrives_loses_nothing(
    kind: Any,
) -> None:
    """put() hands the item to the waiting getter, then the getter is cancelled
    before it runs. The item is only removed when get() actually returns it,
    so nothing is lost or duplicated."""

    async def main() -> None:
        s = kind()
        victim = sim.spawn(get(s))
        await sim.timeout(1)
        await s.put("only")  # wakes victim, which has not run yet
        victim.cancel()  # ...and cancel it in the same instant
        with pytest.raises(asyncio.CancelledError):
            await victim
        assert (len(s), s.items) == (1, ("only",))
        assert await get(s) == "only"
        assert len(s) == 0

    desyncio.run(main())


@pytest.mark.parametrize("kind", KIND)
def test_cancelled_getter_hands_the_item_to_the_next_getter(kind: Any) -> None:
    async def main() -> tuple[str, int]:
        s = kind()
        first = sim.spawn(get(s))
        second = sim.spawn(get(s))
        await sim.timeout(1)
        await s.put("only")
        first.cancel()  # same instant as the arrival
        item = await second  # not lost: the next waiter receives it
        return item, len(s)

    assert desyncio.run(main()) == ("only", 0)


@pytest.mark.parametrize("kind", KIND)
def test_cancelled_putter_is_removed(kind: Any) -> None:
    async def main() -> list[str]:
        s = kind(capacity=1)
        await s.put("a")
        p1 = sim.spawn(s.put("cancelled"))
        sim.spawn(s.put("kept"))
        await sim.timeout(1)
        assert s.items == ("a",)
        p1.cancel()
        await sim.timeout(1)
        # The mirror only ever holds stored items: a cancelled putter's item
        # must not appear in it (nor later come out of a get).
        assert s.items == ("a",)
        out = [await get(s)]
        await sim.timeout(1)
        out.append(await get(s))
        return out

    assert desyncio.run(main()) == ["a", "kept"]


@pytest.mark.parametrize("kind", KIND)
def test_item_never_duplicated_under_many_arrival_time_cancels(kind: Any) -> None:
    async def main() -> tuple[list[int], int]:
        s = kind()
        received: list[int] = []

        async def getter() -> None:
            received.append(await get(s))

        tasks = [sim.spawn(getter()) for _ in range(6)]
        await sim.timeout(1)
        for i in range(6):
            await s.put(i)
        for t in tasks[::2]:  # cancel every other getter at the same instant
            t.cancel()
        await sim.timeout(1)
        rest = [await get(s) for _ in range(len(s))]
        return sorted(received + rest), len(s)

    assert desyncio.run(main()) == ([0, 1, 2, 3, 4, 5], 0)


def test_filter_getter_interrupted_at_arrival_loses_nothing() -> None:
    # Interrupt (D024) is cancellation underneath; FilterStore is the one store
    # not built on asyncio.Queue, so it gets its own check.
    async def main() -> tuple[object, int, str]:
        s: FilterStore[str] = FilterStore()
        cause: list[object] = []

        async def victim() -> None:
            try:
                async with sim.interruptible():
                    await s.get(filter=lambda x: x == "only")
            except Interrupt as exc:
                cause.append(exc.cause)

        t = sim.spawn(victim())
        await sim.timeout(1)
        await s.put("only")
        sim.interrupt(t, "boom")
        await t
        return cause[0], len(s), await s.get()

    assert desyncio.run(main()) == ("boom", 1, "only")


# --- monitor hook (D021) ------------------------------------------------


@pytest.mark.parametrize("kind", KIND)
def test_monitor_called_after_each_put_and_get(kind: Any) -> None:
    async def main() -> list[tuple[float, int, tuple[str, ...]]]:
        seen: list[tuple[float, int, tuple[str, ...]]] = []
        s = kind(monitor=lambda st: seen.append((sim.now, len(st), st.items)))
        await s.put("a")
        await s.put("b")
        await sim.timeout(2)
        await get(s)  # takes "a"
        await get(s)  # takes "b"

        async def producer() -> None:
            await sim.timeout(3)
            await s.put("x")

        sim.spawn(producer())
        await get(s)  # blocks until "x" arrives at t=5
        return seen

    assert desyncio.run(main()) == [
        (0.0, 1, ("a",)),
        (0.0, 2, ("a", "b")),
        (2.0, 1, ("b",)),
        (2.0, 0, ()),
        (5.0, 1, ("x",)),  # the put that releases the blocked get...
        (5.0, 0, ()),  # ...then the get itself
    ]


@pytest.mark.parametrize("kind", KIND)
def test_monitor_not_called_for_cancelled_or_failed_operations(kind: Any) -> None:
    async def main() -> int:
        calls = 0

        def monitor(store: Any) -> None:
            nonlocal calls
            calls += 1

        s = kind(monitor=monitor)
        g = sim.spawn(get(s))
        await sim.timeout(1)
        g.cancel()
        await sim.timeout(1)
        if hasattr(s, "shutdown"):  # FilterStore has none
            s.shutdown()
            with pytest.raises(asyncio.QueueShutDown):
                await get(s)  # refused by shutdown
        return calls

    assert desyncio.run(main()) == 0


@pytest.mark.parametrize("kind", [Store, PriorityStore])
def test_monitor_called_after_immediate_shutdown_clears_items(kind: Any) -> None:
    async def main() -> list[tuple[int, tuple[int, ...]]]:
        seen: list[tuple[int, tuple[int, ...]]] = []
        s = kind(monitor=lambda st: seen.append((len(st), st.items)))
        await s.put(1)
        await s.put(2)
        s.shutdown(immediate=True)
        with pytest.raises(asyncio.QueueShutDown):
            await s.get()
        return seen

    assert desyncio.run(main()) == [(1, (1,)), (2, (1, 2)), (0, ())]


# --- raising monitor (D023 v2) -----------------------------------------


class MonitorBoom(Exception):
    pass


def _capture_handler(loop: asyncio.AbstractEventLoop) -> list[dict[str, Any]]:
    seen: list[dict[str, Any]] = []
    loop.set_exception_handler(lambda _loop, ctx: seen.append(ctx))
    return seen


def test_raising_monitor_on_get_still_returns_the_item() -> None:
    # The STO-01 regression: the monitor raises only after the get has emptied
    # the store, and the get must still hand over the item.
    async def main() -> tuple[str, int, int, list[dict[str, Any]]]:
        seen = _capture_handler(asyncio.get_running_loop())

        def monitor(store: Store[str]) -> None:
            if len(store) == 0:  # only after the get empties the store
                raise MonitorBoom("monitor failed")

        s: Store[str] = Store(monitor=monitor)
        await s.put("x")
        item = await s.get()
        return item, len(s), len(s.items), seen

    item, n, n_items, seen = desyncio.run(main())
    assert (item, n, n_items) == ("x", 0, 0)
    assert len(seen) == 1


@pytest.mark.parametrize("kind", KIND)
def test_raising_monitor_loses_nothing_and_reaches_handler(kind: Any) -> None:
    async def main() -> tuple[Any, list[dict[str, Any]]]:
        seen = _capture_handler(asyncio.get_running_loop())

        def monitor(store: Any) -> None:
            raise MonitorBoom("monitor failed")

        s = kind(monitor=monitor)
        await s.put("x")  # must not raise
        assert (len(s), s.items) == (1, ("x",))  # the item is stored
        assert await get(s) == "x"  # and still comes out
        assert (len(s), s.items) == (0, ())
        return s, seen

    s, seen = desyncio.run(main())
    assert len(seen) == 2  # one for the put, one for the get
    for ctx in seen:
        assert isinstance(ctx["exception"], MonitorBoom)
        assert ctx["store"] is s
        assert ctx["message"] == "Store monitor raised"


@pytest.mark.parametrize("kind", [Store, PriorityStore])
def test_raising_monitor_on_immediate_shutdown_still_completes(kind: Any) -> None:
    async def main() -> tuple[tuple[Any, ...], list[dict[str, Any]]]:
        seen = _capture_handler(asyncio.get_running_loop())

        def monitor(store: Any) -> None:
            if len(store) == 0:  # only the call after shutdown has emptied it
                raise MonitorBoom("monitor failed")

        s = kind(monitor=monitor)
        await s.put("x")
        s.shutdown(immediate=True)  # must not raise
        with pytest.raises(asyncio.QueueShutDown):
            await s.put("y")
        return s.items, seen

    items, seen = desyncio.run(main())
    assert items == ()
    assert len(seen) == 1
    assert isinstance(seen[0]["exception"], MonitorBoom)


@pytest.mark.parametrize("kind", [Store, PriorityStore])
def test_raising_monitor_on_immediate_shutdown_outside_a_loop_is_logged(
    kind: Any, caplog: pytest.LogCaptureFixture
) -> None:
    def monitor(store: Any) -> None:
        if len(store) == 0:  # only the call after shutdown has emptied it
            raise MonitorBoom("monitor failed")

    s = kind(monitor=monitor)
    desyncio.run(s.put("x"))  # the loop is closed again when this returns

    with caplog.at_level("ERROR", logger="desyncio"):
        s.shutdown(immediate=True)  # no running loop; must not raise

    assert s.items == ()
    (record,) = [r for r in caplog.records if r.name == "desyncio"]
    assert record.levelname == "ERROR"
    assert record.getMessage() == "Store monitor raised"
    assert record.exc_info is not None
    assert isinstance(record.exc_info[1], MonitorBoom)


@pytest.mark.parametrize("kind", KIND)
def test_monitor_base_exceptions_propagate(kind: Any) -> None:
    def put_with_raising_monitor(exc_type: type[BaseException]) -> None:
        def monitor(store: Any) -> None:
            raise exc_type()

        async def main() -> None:
            await kind(monitor=monitor).put("x")

        desyncio.run(main())

    for exc_type in (KeyboardInterrupt, SystemExit):
        with pytest.raises(exc_type):
            put_with_raising_monitor(exc_type)


# --- validation ---------------------------------------------------------


# 3.0 is a logged gotcha: an integral float is still not an int capacity.
@pytest.mark.parametrize("bad", [0, math.nan, -math.inf, 3.0])
def test_capacity_bad_value(bad: float) -> None:
    with pytest.raises(ValueError, match="capacity"):
        Store(capacity=bad)


@pytest.mark.parametrize("bad", [True, "3", None])
def test_capacity_bad_type(bad: object) -> None:
    with pytest.raises(TypeError, match="capacity"):
        Store(capacity=bad)  # type: ignore[arg-type]


def test_monitor_must_be_callable() -> None:
    with pytest.raises(TypeError, match="monitor"):
        Store(monitor=42)  # type: ignore[arg-type]


# --- determinism --------------------------------------------------------


def _model(kind: Any, parity_filter: bool = False) -> Any:
    """Two producers and two consumers on one store.

    With ``parity_filter`` the consumers are FilterStore getters that each
    take only items of their own parity; otherwise they take anything.
    """

    async def main() -> list[tuple[float, str, int]]:
        s = kind(capacity=4)
        log: list[tuple[float, str, int]] = []

        async def producer(pid: int) -> None:
            rng = sim.random(f"prod{pid}")
            for _ in range(30):
                await sim.timeout(rng.expovariate(1.0))
                item = rng.randrange(100)
                priority = rng.randrange(3)
                if isinstance(s, PriorityStore):
                    await s.put(item, priority=priority)
                else:
                    await s.put(item)
                log.append((sim.now, f"put{pid}", item))

        async def consumer(cid: int) -> None:
            rng = sim.random(f"cons{cid}")
            match = (lambda x: x % 2 == cid) if parity_filter else None
            while True:
                item = await get(s, match)
                log.append((sim.now, f"get{cid}", item))
                await sim.timeout(rng.expovariate(1.2))

        tasks = [sim.spawn(producer(p)) for p in range(2)]
        tasks += [sim.spawn(consumer(c)) for c in range(2)]
        await sim.timeout(40)
        for t in tasks:
            t.cancel()
        return log

    return main()


@pytest.mark.parametrize("kind", KIND)
def test_same_seed_gives_identical_log_and_a_different_seed_differs(
    kind: Any,
) -> None:
    filtered = kind is FilterStore
    a = desyncio.run(_model(kind, filtered), seed=7)
    assert len(a) > 20
    assert a == desyncio.run(_model(kind, filtered), seed=7)
    assert a != desyncio.run(_model(kind, filtered), seed=8)


# =======================================================================
# PriorityStore (wraps asyncio.PriorityQueue)
# =======================================================================


class NoOrder:
    """An item that refuses every comparison: proves items are never compared."""

    def __init__(self, name: str) -> None:
        self.name = name

    def __lt__(self, other: object) -> bool:
        raise AssertionError("items must never be compared")

    __gt__ = __le__ = __ge__ = __lt__


def test_priority_items_need_not_be_comparable() -> None:
    async def main() -> list[str]:
        s: PriorityStore[NoOrder] = PriorityStore()
        for name in "abc":
            await s.put(NoOrder(name), priority=5)  # all ties
        await s.put(NoOrder("first"), priority=1)
        return [(await s.get()).name for _ in range(4)]

    assert desyncio.run(main()) == ["first", "a", "b", "c"]


def test_priority_default_priority_is_zero_and_ties_are_fifo() -> None:
    async def main() -> list[str]:
        s: PriorityStore[str] = PriorityStore()
        await s.put("late", priority=1)
        await s.put("first")  # no priority: 0
        await s.put("second", priority=0)
        await s.put("third")
        return [await s.get() for _ in range(4)]

    assert desyncio.run(main()) == ["first", "second", "third", "late"]


def test_priority_items_snapshot_is_in_priority_order() -> None:
    async def main() -> None:
        s: PriorityStore[str] = PriorityStore(capacity=5)
        assert (s.capacity, len(s), s.items) == (5, 0, ())
        await s.put("low", priority=9)
        await s.put("high", priority=1)
        await s.put("mid", priority=5)
        snap = s.items
        assert snap == ("high", "mid", "low")
        assert len(s) == 3
        await s.put("high2", priority=1)
        assert snap == ("high", "mid", "low")  # a copy
        assert list(s.items) == ["high", "high2", "mid", "low"]
        assert await s.get() == "high"
        assert s.items == ("high2", "mid", "low")
        assert len(s) == 3

    desyncio.run(main())


def test_priority_capacity_blocks_put_until_get() -> None:
    # The capacity -> maxsize mapping is ours, so this is not an asyncio re-proof.
    async def main() -> tuple[float, list[float]]:
        s: PriorityStore[int] = PriorityStore(capacity=2)
        times: list[float] = []

        async def consumer() -> None:
            await sim.timeout(4)
            await s.get()

        sim.spawn(consumer())
        for i in range(3):
            await s.put(i, priority=i)
            times.append(sim.now)
        return sim.now, times

    assert desyncio.run(main()) == (4.0, [0.0, 0.0, 4.0])


def test_priority_bad_arguments() -> None:
    with pytest.raises(ValueError):
        PriorityStore(capacity=0)
    with pytest.raises(TypeError):
        PriorityStore(capacity=True)
    with pytest.raises(TypeError):
        PriorityStore(monitor=42)  # type: ignore[arg-type]


# =======================================================================
# FilterStore (built on asyncio.Condition)
# =======================================================================


def test_filter_get_returns_first_fifo_match() -> None:
    async def main() -> tuple[int, int, int, tuple[int, ...]]:
        s: FilterStore[int] = FilterStore()
        for i in (1, 2, 3, 4, 5, 6):
            await s.put(i)
        a = await s.get(filter=lambda x: x % 2 == 0)
        b = await s.get(filter=lambda x: x > 2)
        c = await s.get()  # no filter: the oldest item
        return a, b, c, s.items

    assert desyncio.run(main()) == (2, 3, 1, (4, 5, 6))


def test_filter_no_head_of_line_blocking() -> None:
    async def main() -> list[tuple[float, str, int]]:
        s: FilterStore[int] = FilterStore()
        got: list[tuple[float, str, int]] = []

        async def getter(name: str, fn: Any) -> None:
            item = await s.get(filter=fn)
            got.append((sim.now, name, item))

        sim.spawn(getter("big", lambda x: x >= 100))  # waits first, no match yet
        sim.spawn(getter("small", lambda x: x < 100))
        await sim.timeout(1)
        await s.put(5)  # only "small" can take it, though "big" waits longer
        await sim.timeout(1)
        await s.put(500)
        await sim.timeout(1)
        return got

    assert desyncio.run(main()) == [(1.0, "small", 5), (2.0, "big", 500)]


def test_filter_blocked_getters_without_filter_are_fifo() -> None:
    async def main() -> list[tuple[str, str]]:
        s: FilterStore[str] = FilterStore()
        got: list[tuple[str, str]] = []

        async def getter(name: str) -> None:
            got.append((name, await s.get()))

        for name in "abc":
            sim.spawn(getter(name))
        await sim.timeout(1)
        for item in "xyz":
            await s.put(item)
        await sim.timeout(1)
        return got

    assert desyncio.run(main()) == [("a", "x"), ("b", "y"), ("c", "z")]


def test_filter_capacity_blocks_put_and_putters_are_fifo() -> None:
    async def main() -> tuple[list[str], list[float]]:
        s: FilterStore[str] = FilterStore(capacity=1)
        await s.put("first")
        times: list[float] = []

        async def putter(item: str) -> None:
            await s.put(item)
            times.append(sim.now)

        for item in ("p1", "p2", "p3"):
            sim.spawn(putter(item))
        await sim.timeout(1)
        out = []
        for _ in range(4):
            out.append(await s.get())
            await sim.timeout(1)
        return out, times

    assert desyncio.run(main()) == (["first", "p1", "p2", "p3"], [1.0, 2.0, 3.0])


def test_filter_bad_arguments() -> None:
    with pytest.raises(ValueError):
        FilterStore(capacity=0)
    with pytest.raises(TypeError):
        FilterStore(capacity=True)
    with pytest.raises(TypeError):
        FilterStore(monitor="x")  # type: ignore[arg-type]

    async def main() -> None:
        s: FilterStore[int] = FilterStore()
        with pytest.raises(TypeError):
            await s.get(filter=3)  # type: ignore[arg-type]

    desyncio.run(main())


def test_filter_raising_filter_goes_to_that_getter_and_store_stays_usable() -> None:
    async def main() -> tuple[str, tuple[int, ...], int]:
        s: FilterStore[int] = FilterStore()
        await s.put(1)

        def bad(x: int) -> bool:
            raise ValueError("bad filter")

        with pytest.raises(ValueError, match="bad filter"):
            await s.get(filter=bad)
        # nothing was removed and the Condition's lock was released
        items = s.items
        await s.put(2)
        return "ok", items, await s.get(filter=lambda x: x == 2)

    assert desyncio.run(main()) == ("ok", (1,), 2)


def test_filter_raising_filter_on_recheck_does_not_strand_others() -> None:
    async def main() -> tuple[str, int, tuple[int, ...]]:
        s: FilterStore[int] = FilterStore()

        def picky(x: int) -> bool:
            if x == 99:
                raise ValueError("boom at recheck")
            return False

        async def bad() -> str:
            try:
                await s.get(filter=picky)
            except ValueError as exc:
                return str(exc)
            return "no error"

        bad_task = sim.spawn(bad())
        good = sim.spawn(s.get(filter=lambda x: x == 99))
        await sim.timeout(1)
        await s.put(99)  # wakes both; `bad` raises, `good` still gets it
        return await bad_task, await good, s.items

    assert desyncio.run(main()) == ("boom at recheck", 99, ())


@pytest.mark.parametrize("non_matching", [0, 1, 2, 3])
def test_filter_getter_keeps_seniority_after_re_parking(non_matching: int) -> None:
    """Non-matching puts wake both getters and they re-park; the one that
    waited longer still gets the first matching item. With 0 non-matching puts
    this is plain "the longest-waiting matching getter wins"."""

    async def main() -> list[tuple[str, str]]:
        s: FilterStore[str] = FilterStore()
        got: list[tuple[str, str]] = []

        async def getter(name: str) -> None:
            got.append((name, await s.get(lambda p: p.startswith("wheel"))))

        sim.spawn(getter("older"))
        await sim.timeout(1)
        sim.spawn(getter("younger"))
        await sim.timeout(1)
        for i in range(non_matching):  # each wakes both getters; neither matches
            await s.put(f"gear-{i}")
            await sim.timeout(1)
        assert got == []
        await s.put("wheel-1")
        await s.put("wheel-2")
        await sim.timeout(1)
        return got

    assert desyncio.run(main()) == [("older", "wheel-1"), ("younger", "wheel-2")]
