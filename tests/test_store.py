"""Tests for ``desyncio.store.Store`` (STO-01).

Store is a thin wrapper over ``asyncio.Queue``; FIFO order, blocking,
cancellation cleanup and ``shutdown`` are asyncio's behaviour. These tests pin
down that the wrapper keeps those guarantees in *virtual* time.
"""

import asyncio
import math
from typing import Any

import pytest

import desyncio
from desyncio import sim
from desyncio.interrupt import Interrupt
from desyncio.store import FilterStore, PriorityStore, Store

# --- FIFO ---------------------------------------------------------------


def test_items_come_out_fifo() -> None:
    async def main() -> list[int]:
        s: Store[int] = Store()
        for i in range(5):
            await s.put(i)
        return [await s.get() for _ in range(5)]

    assert desyncio.run(main()) == [0, 1, 2, 3, 4]


def test_waiting_getters_are_served_fifo() -> None:
    async def main() -> list[tuple[str, str]]:
        s: Store[str] = Store()
        got: list[tuple[str, str]] = []

        async def getter(name: str) -> None:
            got.append((name, await s.get()))

        for name in "abc":
            sim.spawn(getter(name))
        await sim.timeout(1)  # all three are now waiting
        for item in ("x", "y", "z"):
            await s.put(item)
        await sim.timeout(1)
        return got

    assert desyncio.run(main()) == [("a", "x"), ("b", "y"), ("c", "z")]


def test_waiting_putters_are_served_fifo() -> None:
    async def main() -> list[str]:
        s: Store[str] = Store(capacity=1)
        await s.put("first")
        for item in ("p1", "p2", "p3"):
            sim.spawn(s.put(item))
        await sim.timeout(1)  # three putters queued behind a full store
        out = []
        for _ in range(4):
            out.append(await s.get())
            await sim.timeout(1)
        return out

    assert desyncio.run(main()) == ["first", "p1", "p2", "p3"]


# --- blocking in virtual time ------------------------------------------


def test_get_blocks_while_empty_until_put() -> None:
    async def main() -> tuple[float, str]:
        s: Store[str] = Store()

        async def producer() -> None:
            await sim.timeout(7)
            await s.put("late")

        sim.spawn(producer())
        item = await s.get()
        return sim.now, item

    assert desyncio.run(main()) == (7.0, "late")


def test_put_blocks_while_full_until_get() -> None:
    async def main() -> tuple[float, list[float]]:
        s: Store[int] = Store(capacity=2)
        times: list[float] = []

        async def consumer() -> None:
            await sim.timeout(4)
            await s.get()

        sim.spawn(consumer())
        for i in range(3):
            await s.put(i)
            times.append(sim.now)
        return sim.now, times

    # the first two puts are immediate; the third waits for the get at t=4
    assert desyncio.run(main()) == (4.0, [0.0, 0.0, 4.0])


def test_capacity_and_len_and_items_snapshot() -> None:
    async def main() -> None:
        s: Store[str] = Store(capacity=3)
        assert s.capacity == 3
        assert len(s) == 0
        assert s.items == ()
        await s.put("a")
        await s.put("b")
        assert len(s) == 2
        assert s.items == ("a", "b")
        snap = s.items
        await s.put("c")
        assert snap == ("a", "b")  # a snapshot, not a live view
        assert await s.get() == "a"
        assert s.items == ("b", "c")
        assert len(s) == 2

    desyncio.run(main())


def test_default_capacity_is_infinite() -> None:
    async def main() -> None:
        s: Store[int] = Store()
        assert s.capacity == math.inf
        for i in range(1000):
            await s.put(i)
        assert len(s) == 1000

    desyncio.run(main())


def test_items_and_capacity_are_read_only() -> None:
    s: Store[int] = Store()
    with pytest.raises(AttributeError):
        s.items = (1,)  # type: ignore[misc]
    with pytest.raises(AttributeError):
        s.capacity = 5  # type: ignore[misc]


# --- cancellation -------------------------------------------------------


def test_cancelled_getter_is_removed_from_the_queue() -> None:
    async def main() -> str:
        s: Store[str] = Store()
        g1 = sim.spawn(s.get())
        g2 = sim.spawn(s.get())
        await sim.timeout(1)
        g1.cancel()
        await sim.timeout(1)
        await s.put("x")  # must go to g2, not to the cancelled g1
        return await g2

    assert desyncio.run(main()) == "x"


def test_cancelled_putter_is_removed_from_the_queue() -> None:
    async def main() -> list[str]:
        s: Store[str] = Store(capacity=1)
        await s.put("a")
        p1 = sim.spawn(s.put("cancelled"))
        sim.spawn(s.put("kept"))
        await sim.timeout(1)
        p1.cancel()
        await sim.timeout(1)
        out = [await s.get()]
        await sim.timeout(1)
        out.append(await s.get())
        return out

    assert desyncio.run(main()) == ["a", "kept"]


def test_getter_cancelled_at_the_instant_an_item_arrives_loses_nothing() -> None:
    """put() hands the item to the waiting getter, then the getter is cancelled
    before it runs. asyncio leaves the item in the queue (it is only removed
    when get() actually returns it), so nothing is lost or duplicated."""

    async def main() -> tuple[int, tuple[str, ...], str, int]:
        s: Store[str] = Store()
        victim = sim.spawn(s.get())
        await sim.timeout(1)
        await s.put("only")  # wakes victim, which has not run yet
        victim.cancel()  # ...and cancel it in the same instant
        with pytest.raises(asyncio.CancelledError):
            await victim
        before = (len(s), s.items)
        item = await s.get()
        return before[0], before[1], item, len(s)

    assert desyncio.run(main()) == (1, ("only",), "only", 0)


def test_cancelled_getter_hands_the_item_to_the_next_getter() -> None:
    async def main() -> tuple[str, int]:
        s: Store[str] = Store()
        first = sim.spawn(s.get())
        second = sim.spawn(s.get())
        await sim.timeout(1)
        await s.put("only")
        first.cancel()  # same instant as the arrival
        item = await second  # not lost: the next waiter receives it
        return item, len(s)

    assert desyncio.run(main()) == ("only", 0)


def test_item_never_duplicated_under_many_arrival_time_cancels() -> None:
    async def main() -> tuple[list[int], int]:
        s: Store[int] = Store()
        received: list[int] = []

        async def getter() -> None:
            received.append(await s.get())

        tasks = [sim.spawn(getter()) for _ in range(6)]
        await sim.timeout(1)
        for i in range(6):
            await s.put(i)
        for t in tasks[::2]:  # cancel every other getter at the same instant
            t.cancel()
        await sim.timeout(1)
        rest = [await s.get() for _ in range(len(s))]
        return sorted(received + rest), len(s)

    assert desyncio.run(main()) == ([0, 1, 2, 3, 4, 5], 0)


# --- shutdown -----------------------------------------------------------


def test_shutdown_lets_consumers_drain_then_raises() -> None:
    async def main() -> list[Any]:
        s: Store[int] = Store()
        await s.put(1)
        await s.put(2)
        s.shutdown()
        out: list[Any] = [await s.get(), await s.get()]
        with pytest.raises(asyncio.QueueShutDown):
            await s.get()
        with pytest.raises(asyncio.QueueShutDown):
            await s.put(3)
        return out

    assert desyncio.run(main()) == [1, 2]


def test_shutdown_wakes_blocked_getters() -> None:
    async def main() -> tuple[float, bool]:
        s: Store[int] = Store()

        async def getter() -> bool:
            try:
                await s.get()
            except asyncio.QueueShutDown:
                return True
            return False

        g = sim.spawn(getter())
        await sim.timeout(3)
        s.shutdown()
        return sim.now, await g

    assert desyncio.run(main()) == (3.0, True)


def test_shutdown_wakes_blocked_putters() -> None:
    async def main() -> bool:
        s: Store[int] = Store(capacity=1)
        await s.put(1)

        async def putter() -> bool:
            try:
                await s.put(2)
            except asyncio.QueueShutDown:
                return True
            return False

        p = sim.spawn(putter())
        await sim.timeout(1)
        s.shutdown()
        return await p

    assert desyncio.run(main()) is True


def test_shutdown_immediate_discards_items() -> None:
    async def main() -> tuple[int, tuple[int, ...]]:
        s: Store[int] = Store()
        await s.put(1)
        await s.put(2)
        s.shutdown(immediate=True)
        with pytest.raises(asyncio.QueueShutDown):
            await s.get()
        return len(s), s.items

    assert desyncio.run(main()) == (0, ())


# --- producer / consumer ------------------------------------------------


def test_producer_consumer_with_bounded_buffer() -> None:
    async def main() -> list[tuple[float, str, int]]:
        s: Store[int] = Store(capacity=2)
        log: list[tuple[float, str, int]] = []

        async def producer() -> None:
            for i in range(5):
                await s.put(i)  # blocks when the buffer holds 2
                log.append((sim.now, "put", i))
                await sim.timeout(1)
            s.shutdown()

        async def consumer() -> None:
            while True:
                try:
                    item = await s.get()
                except asyncio.QueueShutDown:
                    return
                log.append((sim.now, "got", item))
                await sim.timeout(3)  # slower than the producer

        c = sim.spawn(consumer())
        p = sim.spawn(producer())
        await p
        await c
        return log

    log = desyncio.run(main())
    assert [i for _, kind, i in log if kind == "got"] == [0, 1, 2, 3, 4]
    assert [i for _, kind, i in log if kind == "put"] == [0, 1, 2, 3, 4]
    # the consumer takes 3 per item, so the last item is fetched at t=12
    assert (12.0, "got", 4) in log
    # the producer is throttled by the full buffer: it wants to put item 4 at
    # t=4 but the buffer holds [2, 3] until the consumer takes 2 at t=6
    assert (6.0, "put", 4) in log


# --- monitor hook (D021) ------------------------------------------------


def test_monitor_called_after_each_put_and_get() -> None:
    async def main() -> list[tuple[float, int, tuple[str, ...]]]:
        seen: list[tuple[float, int, tuple[str, ...]]] = []

        def monitor(store: Store[str]) -> None:
            seen.append((sim.now, len(store), store.items))

        s: Store[str] = Store(monitor=monitor)
        await s.put("a")
        await s.put("b")
        await sim.timeout(2)
        await s.get()
        return seen

    assert desyncio.run(main()) == [
        (0.0, 1, ("a",)),
        (0.0, 2, ("a", "b")),
        (2.0, 1, ("b",)),
    ]


def test_monitor_fires_for_a_blocked_get_when_it_completes() -> None:
    async def main() -> list[tuple[float, int]]:
        seen: list[tuple[float, int]] = []
        s: Store[str] = Store(monitor=lambda st: seen.append((sim.now, len(st))))

        async def producer() -> None:
            await sim.timeout(5)
            await s.put("x")

        sim.spawn(producer())
        await s.get()
        return seen

    # one call for the put, one for the get that it released, both at t=5
    assert desyncio.run(main()) == [(5.0, 1), (5.0, 0)]


def test_monitor_not_called_for_cancelled_or_failed_operations() -> None:
    async def main() -> int:
        calls = 0

        def monitor(store: Store[int]) -> None:
            nonlocal calls
            calls += 1

        s: Store[int] = Store(monitor=monitor)
        g = sim.spawn(s.get())
        await sim.timeout(1)
        g.cancel()
        await sim.timeout(1)
        s.shutdown()
        with pytest.raises(asyncio.QueueShutDown):
            await s.get()
        return calls

    assert desyncio.run(main()) == 0


def test_monitor_called_after_immediate_shutdown_clears_items() -> None:
    async def main() -> list[tuple[int, tuple[int, ...]]]:
        seen: list[tuple[int, tuple[int, ...]]] = []
        s: Store[int] = Store(monitor=lambda st: seen.append((len(st), st.items)))
        await s.put(1)
        await s.put(2)
        s.shutdown(immediate=True)
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


def test_raising_monitor_on_put_completes_and_keeps_mirror_in_step() -> None:
    async def main() -> tuple[int, int, str]:
        _capture_handler(asyncio.get_running_loop())
        calls = 0

        def monitor(store: Store[str]) -> None:
            nonlocal calls
            calls += 1
            if calls == 1:  # raise once, on the first put
                raise MonitorBoom("monitor failed")

        s: Store[str] = Store(monitor=monitor)
        await s.put("x")  # must not raise
        n, n_items = len(s), len(s.items)
        later = await s.get()  # the stored item is still there
        return n, n_items, later

    assert desyncio.run(main()) == (1, 1, "x")


def test_raising_monitor_error_goes_to_the_loop_exception_handler() -> None:
    async def main() -> tuple[Store[str], list[dict[str, Any]]]:
        seen = _capture_handler(asyncio.get_running_loop())

        def monitor(store: Store[str]) -> None:
            raise MonitorBoom("monitor failed")

        s: Store[str] = Store(monitor=monitor)
        await s.put("x")
        await s.get()
        return s, seen

    s, seen = desyncio.run(main())
    assert len(seen) == 2  # one for the put, one for the get
    for ctx in seen:
        assert isinstance(ctx["exception"], MonitorBoom)
        assert ctx["store"] is s
        assert ctx["message"] == "Store monitor raised"


@pytest.mark.parametrize("exc_type", [KeyboardInterrupt, SystemExit])
def test_monitor_base_exceptions_propagate(exc_type: type[BaseException]) -> None:
    async def main() -> None:
        def monitor(store: Store[str]) -> None:
            raise exc_type()

        s: Store[str] = Store(monitor=monitor)
        await s.put("x")

    with pytest.raises(exc_type):
        desyncio.run(main())


# --- validation ---------------------------------------------------------


@pytest.mark.parametrize("bad", [0, -1, -math.inf, math.nan, 2.5, 3.0])
def test_capacity_bad_value(bad: float) -> None:
    with pytest.raises(ValueError, match="capacity"):
        Store(capacity=bad)  # type: ignore[arg-type]


@pytest.mark.parametrize("bad", [True, False, "3", None, [1]])
def test_capacity_bad_type(bad: object) -> None:
    with pytest.raises(TypeError, match="capacity"):
        Store(capacity=bad)  # type: ignore[arg-type]


def test_monitor_must_be_callable() -> None:
    with pytest.raises(TypeError, match="monitor"):
        Store(monitor=42)  # type: ignore[arg-type]


def test_capacity_accepts_int_and_inf() -> None:
    assert Store(capacity=1).capacity == 1
    assert Store(capacity=math.inf).capacity == math.inf


# --- determinism --------------------------------------------------------


def _model() -> Any:
    async def main() -> list[tuple[float, str, int]]:
        s: Store[int] = Store(capacity=3)
        log: list[tuple[float, str, int]] = []

        async def producer(pid: int) -> None:
            rng = sim.random(f"prod{pid}")
            for k in range(20):
                await sim.timeout(rng.expovariate(1.0))
                await s.put(pid * 100 + k)
                log.append((sim.now, "put", pid * 100 + k))

        async def consumer(cid: int) -> None:
            rng = sim.random(f"cons{cid}")
            for _ in range(20):
                item = await s.get()
                log.append((sim.now, f"get{cid}", item))
                await sim.timeout(rng.expovariate(1.2))

        tasks = [sim.spawn(producer(p)) for p in range(2)]
        tasks += [sim.spawn(consumer(c)) for c in range(2)]
        for t in tasks:
            await t
        return log

    return main()


def test_same_seed_gives_identical_log() -> None:
    a = desyncio.run(_model(), seed=7)
    b = desyncio.run(_model(), seed=7)
    assert len(a) == 80
    assert a == b


def test_different_seed_gives_different_log() -> None:
    assert desyncio.run(_model(), seed=7) != desyncio.run(_model(), seed=8)


# =======================================================================
# STO-02: PriorityStore (wraps asyncio.PriorityQueue)
# =======================================================================


class NoOrder:
    """An item that refuses every comparison: proves items are never compared."""

    def __init__(self, name: str) -> None:
        self.name = name

    def __lt__(self, other: object) -> bool:
        raise AssertionError("items must never be compared")

    __gt__ = __le__ = __ge__ = __lt__


def test_priority_lowest_first() -> None:
    async def main() -> list[str]:
        s: PriorityStore[str] = PriorityStore()
        for name, prio in [("c", 3), ("a", 1), ("d", 4), ("b", 2)]:
            await s.put(name, priority=prio)
        return [await s.get() for _ in range(4)]

    assert desyncio.run(main()) == ["a", "b", "c", "d"]


def test_priority_ties_come_out_in_put_order() -> None:
    async def main() -> list[str]:
        s: PriorityStore[str] = PriorityStore()
        for name in "abcde":
            await s.put(name, priority=1)
        await s.put("urgent", priority=0)
        return [await s.get() for _ in range(6)]

    assert desyncio.run(main()) == ["urgent", "a", "b", "c", "d", "e"]


def test_priority_default_priority_is_fifo() -> None:
    async def main() -> list[int]:
        s: PriorityStore[int] = PriorityStore()
        for i in range(4):
            await s.put(i)
        return [await s.get() for _ in range(4)]

    assert desyncio.run(main()) == [0, 1, 2, 3]


def test_priority_items_need_not_be_comparable() -> None:
    async def main() -> list[str]:
        s: PriorityStore[NoOrder] = PriorityStore()
        for name in "abc":
            await s.put(NoOrder(name), priority=5)  # all ties
        await s.put(NoOrder("first"), priority=1)
        return [(await s.get()).name for _ in range(4)]

    assert desyncio.run(main()) == ["first", "a", "b", "c"]


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
        assert s.items == ("high", "high2", "mid", "low")
        assert await s.get() == "high"
        assert s.items == ("high2", "mid", "low")
        assert len(s) == 3

    desyncio.run(main())


def test_priority_get_blocks_in_virtual_time() -> None:
    async def main() -> tuple[float, str]:
        s: PriorityStore[str] = PriorityStore()

        async def producer() -> None:
            await sim.timeout(4)
            await s.put("x", priority=2)

        sim.spawn(producer())
        item = await s.get()
        return sim.now, item

    assert desyncio.run(main()) == (4.0, "x")


def test_priority_capacity_blocks_put_until_get() -> None:
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


def test_priority_blocked_putters_are_served_fifo() -> None:
    async def main() -> list[str]:
        s: PriorityStore[str] = PriorityStore(capacity=1)
        await s.put("first")
        for item in ("p1", "p2", "p3"):
            sim.spawn(s.put(item))
        await sim.timeout(1)
        out = []
        for _ in range(4):
            out.append(await s.get())
            await sim.timeout(1)
        return out

    assert desyncio.run(main()) == ["first", "p1", "p2", "p3"]


def test_priority_waiting_getters_served_fifo() -> None:
    async def main() -> list[tuple[str, str]]:
        s: PriorityStore[str] = PriorityStore()
        got: list[tuple[str, str]] = []

        async def getter(name: str) -> None:
            got.append((name, await s.get()))

        for name in "ab":
            sim.spawn(getter(name))
        await sim.timeout(1)
        await s.put("x", priority=1)
        await s.put("y", priority=0)
        await sim.timeout(1)
        return got

    # the longest-waiting getter runs first and takes the best item present
    assert desyncio.run(main()) == [("a", "y"), ("b", "x")]


def test_priority_bad_arguments() -> None:
    with pytest.raises(ValueError):
        PriorityStore(capacity=0)
    with pytest.raises(TypeError):
        PriorityStore(capacity=True)
    with pytest.raises(TypeError):
        PriorityStore(monitor=42)  # type: ignore[arg-type]


def test_priority_cancelled_getter_is_removed() -> None:
    async def main() -> str:
        s: PriorityStore[str] = PriorityStore()
        g1 = sim.spawn(s.get())
        g2 = sim.spawn(s.get())
        await sim.timeout(1)
        g1.cancel()
        await sim.timeout(1)
        await s.put("x")
        return await g2

    assert desyncio.run(main()) == "x"


def test_priority_cancelled_putter_is_removed() -> None:
    async def main() -> list[str]:
        s: PriorityStore[str] = PriorityStore(capacity=1)
        await s.put("a")
        p1 = sim.spawn(s.put("cancelled"))
        sim.spawn(s.put("kept"))
        await sim.timeout(1)
        p1.cancel()
        await sim.timeout(1)
        out = [await s.get()]
        await sim.timeout(1)
        out.append(await s.get())
        return out

    assert desyncio.run(main()) == ["a", "kept"]


def test_priority_getter_cancelled_at_arrival_loses_nothing() -> None:
    async def main() -> tuple[int, tuple[str, ...], str, int]:
        s: PriorityStore[str] = PriorityStore()
        victim = sim.spawn(s.get())
        await sim.timeout(1)
        await s.put("only", priority=3)
        victim.cancel()
        with pytest.raises(asyncio.CancelledError):
            await victim
        before = (len(s), s.items)
        item = await s.get()
        return before[0], before[1], item, len(s)

    assert desyncio.run(main()) == (1, ("only",), "only", 0)


def test_priority_getter_interrupted_at_arrival_loses_nothing() -> None:
    async def main() -> tuple[object, int, str]:
        s: PriorityStore[str] = PriorityStore()
        cause: list[object] = []

        async def victim() -> None:
            try:
                async with sim.interruptible():
                    await s.get()
            except Interrupt as exc:
                cause.append(exc.cause)

        t = sim.spawn(victim())
        await sim.timeout(1)
        await s.put("only")
        sim.interrupt(t, "boom")
        await t
        return cause[0], len(s), await s.get()

    assert desyncio.run(main()) == ("boom", 1, "only")


def test_priority_no_duplicates_under_many_arrival_time_cancels() -> None:
    async def main() -> tuple[list[int], int]:
        s: PriorityStore[int] = PriorityStore()
        received: list[int] = []

        async def getter() -> None:
            received.append(await s.get())

        tasks = [sim.spawn(getter()) for _ in range(6)]
        await sim.timeout(1)
        for i in range(6):
            await s.put(i, priority=-i)
        for t in tasks[::2]:
            t.cancel()
        await sim.timeout(1)
        rest = [await s.get() for _ in range(len(s))]
        return sorted(received + rest), len(s)

    assert desyncio.run(main()) == ([0, 1, 2, 3, 4, 5], 0)


def test_priority_shutdown_passthrough() -> None:
    async def main() -> tuple[list[str], int]:
        s: PriorityStore[str] = PriorityStore()
        await s.put("b", priority=2)
        await s.put("a", priority=1)
        s.shutdown()
        with pytest.raises(asyncio.QueueShutDown):
            await s.put("c")
        drained = [await s.get(), await s.get()]
        with pytest.raises(asyncio.QueueShutDown):
            await s.get()
        s2: PriorityStore[str] = PriorityStore()
        await s2.put("z")
        s2.shutdown(immediate=True)
        return drained, len(s2) + len(s2.items)

    assert desyncio.run(main()) == (["a", "b"], 0)


def test_priority_monitor_called_after_each_put_and_get() -> None:
    async def main() -> list[tuple[int, tuple[str, ...]]]:
        seen: list[tuple[int, tuple[str, ...]]] = []
        s: PriorityStore[str] = PriorityStore(
            monitor=lambda st: seen.append((len(st), st.items))
        )
        await s.put("b", priority=2)
        await s.put("a", priority=1)
        await s.get()
        s.shutdown(immediate=True)
        return seen

    assert desyncio.run(main()) == [
        (1, ("b",)),
        (2, ("a", "b")),
        (1, ("b",)),
        (0, ()),
    ]


def test_priority_monitor_not_called_for_cancelled_operations() -> None:
    async def main() -> int:
        calls = 0

        def monitor(st: PriorityStore[str]) -> None:
            nonlocal calls
            calls += 1

        s: PriorityStore[str] = PriorityStore(monitor=monitor)
        g = sim.spawn(s.get())
        await sim.timeout(1)
        g.cancel()
        await sim.timeout(1)
        return calls

    assert desyncio.run(main()) == 0


def test_priority_raising_monitor_loses_nothing_and_reaches_handler() -> None:
    async def main() -> tuple[str, int, list[dict[str, Any]], PriorityStore[str]]:
        seen = _capture_handler(asyncio.get_running_loop())

        def monitor(st: PriorityStore[str]) -> None:
            raise MonitorBoom("monitor failed")

        s: PriorityStore[str] = PriorityStore(monitor=monitor)
        await s.put("x")
        item = await s.get()
        return item, len(s.items), seen, s

    item, n, seen, s = desyncio.run(main())
    assert (item, n, len(seen)) == ("x", 0, 2)
    assert all(isinstance(c["exception"], MonitorBoom) for c in seen)
    assert all(c["store"] is s for c in seen)


@pytest.mark.parametrize("exc_type", [KeyboardInterrupt, SystemExit])
def test_priority_monitor_base_exceptions_propagate(
    exc_type: type[BaseException],
) -> None:
    async def main() -> None:
        def monitor(st: PriorityStore[str]) -> None:
            raise exc_type()

        s: PriorityStore[str] = PriorityStore(monitor=monitor)
        await s.put("x")

    with pytest.raises(exc_type):
        desyncio.run(main())


def _priority_model() -> Any:
    async def main() -> list[tuple[float, str, int]]:
        s: PriorityStore[int] = PriorityStore(capacity=4)
        log: list[tuple[float, str, int]] = []

        async def producer(pid: int) -> None:
            rng = sim.random(f"prod{pid}")
            for k in range(20):
                await sim.timeout(rng.expovariate(1.0))
                await s.put(pid * 100 + k, priority=rng.randrange(3))
                log.append((sim.now, "put", pid * 100 + k))

        async def consumer(cid: int) -> None:
            rng = sim.random(f"cons{cid}")
            for _ in range(20):
                item = await s.get()
                log.append((sim.now, f"get{cid}", item))
                await sim.timeout(rng.expovariate(1.2))

        tasks = [sim.spawn(producer(p)) for p in range(2)]
        tasks += [sim.spawn(consumer(c)) for c in range(2)]
        for t in tasks:
            await t
        return log

    return main()


def test_priority_same_seed_gives_identical_log() -> None:
    a = desyncio.run(_priority_model(), seed=7)
    assert len(a) == 80
    assert a == desyncio.run(_priority_model(), seed=7)
    assert a != desyncio.run(_priority_model(), seed=8)


# =======================================================================
# STO-02: FilterStore (built on asyncio.Condition)
# =======================================================================


def test_filter_get_returns_first_fifo_match() -> None:
    async def main() -> tuple[int, int, tuple[int, ...]]:
        s: FilterStore[int] = FilterStore()
        for i in (1, 2, 3, 4, 5, 6):
            await s.put(i)
        a = await s.get(filter=lambda x: x % 2 == 0)
        b = await s.get(filter=lambda x: x > 2)
        return a, b, s.items

    assert desyncio.run(main()) == (2, 3, (1, 4, 5, 6))


def test_filter_get_without_filter_is_plain_fifo() -> None:
    async def main() -> list[int]:
        s: FilterStore[int] = FilterStore()
        for i in range(4):
            await s.put(i)
        return [await s.get() for _ in range(4)]

    assert desyncio.run(main()) == [0, 1, 2, 3]


def test_filter_get_blocks_until_a_match_exists() -> None:
    async def main() -> tuple[float, int, tuple[int, ...]]:
        s: FilterStore[int] = FilterStore()
        await s.put(1)

        async def producer() -> None:
            await sim.timeout(3)
            await s.put(3)  # no match
            await sim.timeout(3)
            await s.put(10)  # match

        sim.spawn(producer())
        item = await s.get(filter=lambda x: x >= 10)
        return sim.now, item, s.items

    assert desyncio.run(main()) == (6.0, 10, (1, 3))


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


def test_filter_longest_waiting_matching_getter_wins() -> None:
    async def main() -> list[tuple[str, int]]:
        s: FilterStore[int] = FilterStore()
        got: list[tuple[str, int]] = []

        async def getter(name: str, fn: Any) -> None:
            got.append((name, await s.get(filter=fn)))

        sim.spawn(getter("first", lambda x: x % 2 == 0))
        await sim.timeout(1)
        sim.spawn(getter("second", lambda x: True))
        sim.spawn(getter("third", lambda x: True))
        await sim.timeout(1)
        await s.put(4)  # matches all three; the longest-waiting takes it
        await sim.timeout(1)
        await s.put(7)  # "first" is done; "second" is next in line
        await sim.timeout(1)
        return got

    assert desyncio.run(main()) == [("first", 4), ("second", 7)]


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


def test_filter_get_frees_space_for_blocked_put() -> None:
    async def main() -> tuple[tuple[str, ...], float]:
        s: FilterStore[str] = FilterStore(capacity=2)
        await s.put("a")
        await s.put("b")

        async def taker() -> None:
            await sim.timeout(5)
            await s.get(filter=lambda x: x == "b")

        sim.spawn(taker())
        await s.put("c")  # blocks until "b" is taken at t=5
        return s.items, sim.now

    assert desyncio.run(main()) == (("a", "c"), 5.0)


def test_filter_capacity_len_items_read_only() -> None:
    async def main() -> None:
        s: FilterStore[str] = FilterStore(capacity=3)
        assert (s.capacity, len(s), s.items) == (3, 0, ())
        await s.put("a")
        snap = s.items
        await s.put("b")
        assert snap == ("a",)
        assert len(s) == 2
        assert FilterStore().capacity == math.inf
        with pytest.raises(AttributeError):
            s.items = ()  # type: ignore[misc]

    desyncio.run(main())


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


def test_filter_cancelled_getter_is_removed() -> None:
    async def main() -> str:
        s: FilterStore[str] = FilterStore()
        g1 = sim.spawn(s.get())
        g2 = sim.spawn(s.get())
        await sim.timeout(1)
        g1.cancel()
        await sim.timeout(1)
        await s.put("x")
        return await g2

    assert desyncio.run(main()) == "x"


def test_filter_cancelled_putter_is_removed() -> None:
    async def main() -> list[str]:
        s: FilterStore[str] = FilterStore(capacity=1)
        await s.put("a")
        p1 = sim.spawn(s.put("cancelled"))
        sim.spawn(s.put("kept"))
        await sim.timeout(1)
        p1.cancel()
        await sim.timeout(1)
        out = [await s.get()]
        await sim.timeout(1)
        out.append(await s.get())
        return out

    assert desyncio.run(main()) == ["a", "kept"]


def test_filter_getter_cancelled_at_arrival_loses_nothing() -> None:
    async def main() -> tuple[int, tuple[str, ...], str, int]:
        s: FilterStore[str] = FilterStore()
        victim = sim.spawn(s.get(filter=lambda x: x == "only"))
        await sim.timeout(1)
        await s.put("only")  # wakes victim, which has not run yet
        victim.cancel()
        with pytest.raises(asyncio.CancelledError):
            await victim
        before = (len(s), s.items)
        item = await s.get()
        return before[0], before[1], item, len(s)

    assert desyncio.run(main()) == (1, ("only",), "only", 0)


def test_filter_cancelled_getter_item_goes_to_next_matching_getter() -> None:
    async def main() -> tuple[str, int]:
        s: FilterStore[str] = FilterStore()
        first = sim.spawn(s.get())
        second = sim.spawn(s.get())
        await sim.timeout(1)
        await s.put("only")
        first.cancel()
        return await second, len(s)

    assert desyncio.run(main()) == ("only", 0)


def test_filter_getter_interrupted_at_arrival_loses_nothing() -> None:
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


def test_filter_interrupted_blocked_putter_loses_nothing() -> None:
    async def main() -> tuple[tuple[str, ...], bool]:
        s: FilterStore[str] = FilterStore(capacity=1)
        await s.put("a")
        hit: list[bool] = []

        async def victim() -> None:
            try:
                async with sim.interruptible():
                    await s.put("never")
            except Interrupt:
                hit.append(True)

        t = sim.spawn(victim())
        await sim.timeout(1)
        sim.interrupt(t)
        await t
        await s.get()
        await sim.timeout(1)
        return s.items, bool(hit)

    assert desyncio.run(main()) == ((), True)


def test_filter_no_duplicates_under_many_arrival_time_cancels() -> None:
    async def main() -> tuple[list[int], int]:
        s: FilterStore[int] = FilterStore()
        received: list[int] = []

        async def getter() -> None:
            received.append(await s.get())

        tasks = [sim.spawn(getter()) for _ in range(6)]
        await sim.timeout(1)
        for i in range(6):
            await s.put(i)
        for t in tasks[::2]:
            t.cancel()
        await sim.timeout(1)
        rest = [await s.get() for _ in range(len(s))]
        return sorted(received + rest), len(s)

    assert desyncio.run(main()) == ([0, 1, 2, 3, 4, 5], 0)


def test_filter_monitor_called_after_each_put_and_get() -> None:
    async def main() -> list[tuple[int, tuple[int, ...]]]:
        seen: list[tuple[int, tuple[int, ...]]] = []
        s: FilterStore[int] = FilterStore(
            monitor=lambda st: seen.append((len(st), st.items))
        )
        await s.put(1)
        await s.put(2)
        await s.get(filter=lambda x: x == 2)
        return seen

    assert desyncio.run(main()) == [(1, (1,)), (2, (1, 2)), (1, (1,))]


def test_filter_monitor_fires_for_blocked_get_and_not_for_cancelled() -> None:
    async def main() -> list[int]:
        seen: list[int] = []
        s: FilterStore[int] = FilterStore(monitor=lambda st: seen.append(len(st)))
        g = sim.spawn(s.get(filter=lambda x: x == 1))
        cancelled = sim.spawn(s.get(filter=lambda x: x == 2))
        await sim.timeout(1)
        cancelled.cancel()
        await s.put(1)
        await g
        return seen

    assert desyncio.run(main()) == [1, 0]


def test_filter_raising_monitor_loses_nothing_and_reaches_handler() -> None:
    async def main() -> tuple[int, int, list[dict[str, Any]], FilterStore[int]]:
        seen = _capture_handler(asyncio.get_running_loop())

        def monitor(st: FilterStore[int]) -> None:
            raise MonitorBoom("monitor failed")

        s: FilterStore[int] = FilterStore(monitor=monitor)
        await s.put(5)
        item = await s.get(filter=lambda x: x == 5)
        return item, len(s), seen, s

    item, n, seen, s = desyncio.run(main())
    assert (item, n, len(seen)) == (5, 0, 2)
    assert all(isinstance(c["exception"], MonitorBoom) for c in seen)
    assert all(c["store"] is s for c in seen)


@pytest.mark.parametrize("exc_type", [KeyboardInterrupt, SystemExit])
def test_filter_monitor_base_exceptions_propagate(
    exc_type: type[BaseException],
) -> None:
    async def main() -> None:
        def monitor(st: FilterStore[int]) -> None:
            raise exc_type()

        s: FilterStore[int] = FilterStore(monitor=monitor)
        await s.put(1)

    with pytest.raises(exc_type):
        desyncio.run(main())


def _filter_model() -> Any:
    async def main() -> list[tuple[float, str, int]]:
        s: FilterStore[int] = FilterStore(capacity=4)
        log: list[tuple[float, str, int]] = []

        async def producer(pid: int) -> None:
            rng = sim.random(f"prod{pid}")
            for _ in range(30):
                await sim.timeout(rng.expovariate(1.0))
                item = rng.randrange(100)
                await s.put(item)
                log.append((sim.now, f"put{pid}", item))

        async def consumer(parity: int) -> None:
            rng = sim.random(f"cons{parity}")
            while True:  # takes only items of its own parity
                item = await s.get(filter=lambda x: x % 2 == parity)
                log.append((sim.now, f"get{parity}", item))
                await sim.timeout(rng.expovariate(1.2))

        tasks = [sim.spawn(producer(p)) for p in range(2)]
        tasks += [sim.spawn(consumer(c)) for c in range(2)]
        await sim.timeout(40)
        for t in tasks:
            t.cancel()
        return log

    return main()


def test_filter_same_seed_gives_identical_log() -> None:
    a = desyncio.run(_filter_model(), seed=7)
    assert len(a) > 20
    assert a == desyncio.run(_filter_model(), seed=7)
    assert a != desyncio.run(_filter_model(), seed=8)
