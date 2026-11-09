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
from desyncio.store import Store

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
            rng = sim.rng(f"prod{pid}")
            for k in range(20):
                await sim.timeout(rng.expovariate(1.0))
                await s.put(pid * 100 + k)
                log.append((sim.now, "put", pid * 100 + k))

        async def consumer(cid: int) -> None:
            rng = sim.rng(f"cons{cid}")
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
