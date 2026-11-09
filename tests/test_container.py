"""Tests for ``desyncio.container.Container`` (CON-01)."""

import asyncio
import math

import pytest

import desyncio
from desyncio import sim
from desyncio.container import Container


def test_defaults_and_read_only() -> None:
    c = Container()
    assert c.capacity == math.inf
    assert c.level == 0
    c2 = Container(10, init=4)
    assert (c2.capacity, c2.level) == (10, 4)
    with pytest.raises(AttributeError):
        c2.level = 1  # type: ignore[misc]
    with pytest.raises(AttributeError):
        c2.capacity = 1  # type: ignore[misc]


def test_get_blocks_until_put() -> None:
    async def main() -> list[tuple[str, float]]:
        c = Container()
        log: list[tuple[str, float]] = []

        async def getter() -> None:
            await c.get(5)
            log.append(("got", sim.now))

        t = sim.spawn(getter())
        await sim.timeout(2)
        await c.put(3)
        await sim.timeout(2)
        assert not t.done()
        await c.put(2)
        await t
        assert c.level == 0
        return log

    assert desyncio.run(main()) == [("got", 4.0)]


def test_put_blocks_until_get_makes_room() -> None:
    async def main() -> float:
        c = Container(10, init=8)

        async def putter() -> float:
            await c.put(5)
            return sim.now

        t = sim.spawn(putter())
        await sim.timeout(3)
        assert not t.done()
        await c.get(3)
        when = await t
        assert c.level == 10
        return when

    assert desyncio.run(main()) == 3.0


def test_capacity_limit_exact_fit_does_not_block() -> None:
    async def main() -> float:
        c = Container(10, init=5)
        await c.put(5)
        await c.get(10)
        return sim.now

    assert desyncio.run(main()) == 0.0


def test_head_of_line_get() -> None:
    async def main() -> list[tuple[str, float]]:
        c = Container()
        order: list[tuple[str, float]] = []

        async def getter(name: str, n: float) -> None:
            await c.get(n)
            order.append((name, sim.now))

        sim.spawn(getter("big", 10))
        await sim.timeout(1)
        sim.spawn(getter("small", 1))
        await sim.timeout(1)
        await c.put(5)  # small would fit, but big is ahead of it
        await sim.timeout(1)
        assert order == []
        await c.put(6)  # big served; the 1 left over serves small
        await sim.timeout(1)
        return order

    assert desyncio.run(main()) == [("big", 3.0), ("small", 3.0)]


def test_head_of_line_put() -> None:
    async def main() -> list[tuple[str, float]]:
        c = Container(10, init=10)
        order: list[tuple[str, float]] = []

        async def putter(name: str, n: float) -> None:
            await c.put(n)
            order.append((name, sim.now))

        sim.spawn(putter("big", 8))
        await sim.timeout(1)
        sim.spawn(putter("small", 1))
        await sim.timeout(1)
        await c.get(5)  # small would fit (5 + 1) but big is first
        await sim.timeout(1)
        assert order == []
        await c.get(5)
        await sim.timeout(1)
        return order

    assert desyncio.run(main()) == [("big", 3.0), ("small", 3.0)]


def test_directions_are_independent() -> None:
    async def main() -> float:
        c = Container(10, init=0)
        sim.spawn(c.get(8))  # a blocked get must not block puts
        await sim.timeout(1)
        await c.put(10)
        return sim.now

    assert desyncio.run(main()) == 1.0


def test_cancelled_head_of_line_unblocks_next() -> None:
    async def main() -> tuple[float, float]:
        c = Container()
        big = sim.spawn(c.get(10))
        await sim.timeout(1)
        small = sim.spawn(c.get(1))
        await sim.timeout(1)
        await c.put(2)
        await sim.timeout(1)
        assert not small.done()
        big.cancel()
        await sim.timeout(1)
        assert big.cancelled()
        await small
        return c.level, sim.now

    assert desyncio.run(main()) == (1.0, 4.0)


def test_cancelled_waiter_takes_nothing() -> None:
    async def main() -> float:
        c = Container()
        t = sim.spawn(c.get(5))
        await sim.timeout(1)
        t.cancel()
        await sim.timeout(1)
        await c.put(5)
        await sim.timeout(1)
        return c.level

    assert desyncio.run(main()) == 5


def test_timeout_cancels_waiter() -> None:
    async def main() -> float:
        c = Container()
        with pytest.raises(TimeoutError):
            async with asyncio.timeout(2):
                await c.get(1)
        await c.put(1)
        await c.get(1)
        return sim.now

    assert desyncio.run(main()) == 2.0


@pytest.mark.parametrize("amount", [0, -1, math.nan, math.inf, 11])
def test_bad_amount_value(amount: float) -> None:
    async def main() -> None:
        c = Container(10, init=5)
        with pytest.raises(ValueError):
            await c.put(amount)
        with pytest.raises(ValueError):
            await c.get(amount)

    desyncio.run(main())


@pytest.mark.parametrize("amount", [True, "1", None, 1j])
def test_bad_amount_type(amount: object) -> None:
    async def main() -> None:
        c = Container(10, init=5)
        with pytest.raises(TypeError):
            await c.put(amount)  # type: ignore[arg-type]
        with pytest.raises(TypeError):
            await c.get(amount)  # type: ignore[arg-type]

    desyncio.run(main())


def test_bad_construction() -> None:
    for cap in (0, -1, math.nan):
        with pytest.raises(ValueError):
            Container(cap)
    with pytest.raises(TypeError):
        Container(True)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        Container("5")  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        Container(10, init=-1)
    with pytest.raises(ValueError):
        Container(10, init=11)
    with pytest.raises(TypeError):
        Container(10, init=True)  # type: ignore[arg-type]


def test_monitor_hook() -> None:
    # D023: the monitor receives the container itself, after the state is
    # settled, and reads public properties from it.
    async def main() -> tuple[Container, list[Container], list[float]]:
        seen: list[Container] = []
        levels: list[float] = []

        def monitor(c: Container) -> None:
            seen.append(c)
            levels.append(c.level)

        c = Container(10, monitor=monitor)
        await c.put(4)
        await c.get(1)
        t = sim.spawn(c.get(9))
        await sim.timeout(1)
        await c.put(6)
        await t
        return c, seen, levels

    c, seen, levels = desyncio.run(main())
    assert seen == [c, c, c, c]
    assert levels == [4, 3, 9, 0]


def _raising_monitor(c: Container) -> None:
    raise RuntimeError("monitor boom")


def _capture_handler(errors: list[dict[str, object]]) -> None:
    def handler(loop: asyncio.AbstractEventLoop, context: dict[str, object]) -> None:
        errors.append(context)

    asyncio.get_running_loop().set_exception_handler(handler)


def test_raising_monitor_goes_to_exception_handler() -> None:
    # D023 v2: a monitor error never alters the model. The operation completes
    # normally, and the error goes to loop.call_exception_handler.
    async def main() -> tuple[Container, list[dict[str, object]], float]:
        errors: list[dict[str, object]] = []
        _capture_handler(errors)
        c = Container(10, monitor=_raising_monitor)
        await c.put(4)
        assert c.level == 4
        await c.get(1)
        assert c.level == 3
        later = sim.spawn(c.get(5))
        await sim.timeout(1)
        await c.put(2)
        await later  # the later waiter is still served
        assert c.level == 0
        return c, errors, sim.now

    c, errors, now = desyncio.run(main())
    assert now == 1.0
    assert len(errors) == 4
    for ctx in errors:
        assert ctx["message"] == "Container monitor raised"
        assert isinstance(ctx["exception"], RuntimeError)
        assert ctx["container"] is c


def test_raising_monitor_does_not_mask_cancellation() -> None:
    async def main() -> tuple[float, int]:
        errors: list[dict[str, object]] = []
        _capture_handler(errors)
        c = Container(monitor=_raising_monitor)
        waiter = sim.spawn(c.get(5))
        await sim.timeout(1)
        # put wakes the waiter (and its monitor call raises); cancel the
        # waiter before it gets to run.
        await c.put(5)
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter
        assert waiter.cancelled()
        return c.level, len(errors)

    assert desyncio.run(main()) == (5, 1)


def test_monitor_base_exceptions_propagate() -> None:
    def monitor(c: Container) -> None:
        raise KeyboardInterrupt

    async def main() -> None:
        c = Container(monitor=monitor)
        await c.put(1)

    with pytest.raises(KeyboardInterrupt):
        desyncio.run(main())


def test_fuel_tank() -> None:
    async def main() -> list[tuple[str, float, float]]:
        tank = Container(100, init=30)
        log: list[tuple[str, float, float]] = []

        async def car(name: str, arrive: float, need: float) -> None:
            await sim.timeout(arrive)
            await tank.get(need)
            log.append((name, sim.now, tank.level))

        async def tanker() -> None:
            await sim.timeout(10)
            await tank.put(60)

        async with asyncio.TaskGroup() as tg:
            tg.create_task(car("a", 1, 20))
            tg.create_task(car("b", 2, 40))
            tg.create_task(car("c", 3, 5))
            tg.create_task(tanker())
        return log

    # b (40) blocks c (5) until the tanker arrives, even though c would fit.
    assert desyncio.run(main()) == [
        ("a", 1.0, 10.0),
        ("b", 10.0, 30.0),
        ("c", 10.0, 25.0),
    ]


async def _feeder(c: Container) -> None:
    for _ in range(20):
        await sim.timeout(1)
        await c.put(3)


def _model() -> object:
    async def model() -> list[tuple[int, float, float]]:
        c = Container(1000, init=5)
        log: list[tuple[int, float, float]] = []

        async def worker(i: int) -> None:
            for k in range(3):
                await sim.timeout((i * 7 + k * 3) % 5 + 1)
                if i % 2:
                    await c.put(3 + i)
                else:
                    await c.get(2 + i)
                log.append((i, sim.now, c.level))

        async with asyncio.TaskGroup() as tg:
            for i in range(6):
                tg.create_task(worker(i))
            tg.create_task(_feeder(c))
        return log

    return desyncio.run(model())


def test_determinism() -> None:
    first = _model()
    assert first  # the model really did something
    assert first == _model()
