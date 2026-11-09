"""Tests for ``desyncio.resource.Resource`` (RES-01)."""

import asyncio

import pytest

import desyncio
from desyncio import sim
from desyncio.resource import Resource
from models.mm1 import CustomerRecord, mm1


def test_fifo_service_in_virtual_time() -> None:
    async def main() -> list[tuple[str, float]]:
        res = Resource(1)
        log: list[tuple[str, float]] = []

        async def user(name: str, arrive: float) -> None:
            await sim.timeout(arrive)
            async with res:
                log.append((name, sim.now))
                await sim.timeout(5)

        await asyncio.gather(
            *(sim.spawn(user(n, t)) for n, t in zip("abcd", range(4), strict=True))
        )
        return log

    assert desyncio.run(main()) == [("a", 0.0), ("b", 5.0), ("c", 10.0), ("d", 15.0)]


def test_no_barging_same_instant_newcomer() -> None:
    async def main() -> list[str]:
        res = Resource(1)
        order: list[str] = []

        async def user(name: str, hold: float) -> None:
            async with res:
                order.append(name)
                await sim.timeout(hold)

        first = sim.spawn(user("first", 2))
        await sim.timeout(1)
        waiter = sim.spawn(user("waiter", 1))
        await sim.timeout(1)  # t=2: first releases; waiter is queued
        newcomer = sim.spawn(user("newcomer", 1))  # same instant as the release
        await asyncio.gather(first, waiter, newcomer)
        return order

    assert desyncio.run(main()) == ["first", "waiter", "newcomer"]


def test_capacity_limit_and_counters() -> None:
    async def main() -> None:
        res = Resource(2)
        assert (res.capacity, res.count, res.queue_length) == (2, 0, 0)
        peak = 0

        async def user() -> None:
            nonlocal peak
            async with res:
                peak = max(peak, res.count)
                assert res.count <= 2
                await sim.timeout(1)

        tasks = [sim.spawn(user()) for _ in range(5)]
        await sim.timeout(0.5)
        assert (res.count, res.queue_length) == (2, 3)
        await asyncio.gather(*tasks)
        assert peak == 2
        assert (res.count, res.queue_length) == (0, 0)
        assert sim.now == 3.0

    desyncio.run(main())


def test_properties_are_read_only() -> None:
    res = Resource(1)
    for name in ("capacity", "count", "queue_length"):
        with pytest.raises(AttributeError):
            setattr(res, name, 5)


def test_cancelled_waiter_is_removed() -> None:
    async def main() -> list[str]:
        res = Resource(1)
        got: list[str] = []

        async def user(name: str, hold: float) -> None:
            async with res:
                got.append(name)
                await sim.timeout(hold)

        holder = sim.spawn(user("holder", 3))
        await sim.timeout(1)
        doomed = sim.spawn(user("doomed", 1))
        later = sim.spawn(user("later", 1))
        await sim.timeout(1)
        assert (res.count, res.queue_length) == (1, 2)
        doomed.cancel()
        with pytest.raises(asyncio.CancelledError):
            await doomed
        assert (res.count, res.queue_length) == (1, 1)
        await asyncio.gather(holder, later)
        assert (res.count, res.queue_length) == (0, 0)
        return got

    assert desyncio.run(main()) == ["holder", "later"]


def test_cancelled_right_after_grant_passes_unit_on() -> None:
    async def main() -> list[str]:
        res = Resource(1)
        got: list[str] = []

        async def user(name: str, hold: float) -> None:
            async with res:
                got.append(name)
                await sim.timeout(hold)

        holder = sim.spawn(user("holder", 1))
        await sim.timeout(0.5)
        doomed = sim.spawn(user("doomed", 1))
        later = sim.spawn(user("later", 1))
        await sim.timeout(0.5)  # t=1: holder releases, doomed is granted...
        # ...but has not run yet; cancel it before it resumes.
        doomed.cancel()
        with pytest.raises(asyncio.CancelledError):
            await doomed
        await asyncio.gather(holder, later)
        assert (res.count, res.queue_length) == (0, 0)
        return got

    assert desyncio.run(main()) == ["holder", "later"]


def test_release_on_exception() -> None:
    async def main() -> None:
        res = Resource(1)
        with pytest.raises(ZeroDivisionError):
            async with res:
                assert res.count == 1
                1 / 0  # noqa: B018
        assert res.count == 0
        async with res:  # unit is available again
            assert res.count == 1

    desyncio.run(main())


def test_release_on_cancellation_of_holder() -> None:
    async def main() -> None:
        res = Resource(1)

        async def hold() -> None:
            async with res:
                await sim.timeout(100)

        t = sim.spawn(hold())
        await sim.timeout(1)
        t.cancel()
        with pytest.raises(asyncio.CancelledError):
            await t
        assert res.count == 0

    desyncio.run(main())


def test_monitor_hook_calls() -> None:
    async def main() -> list[tuple[float, int, int]]:
        seen: list[tuple[float, int, int]] = []
        res = Resource(
            1, monitor=lambda r: seen.append((sim.now, r.count, r.queue_length))
        )

        async def user() -> None:
            async with res:
                await sim.timeout(1)

        a, b = sim.spawn(user()), sim.spawn(user())
        await asyncio.gather(a, b)
        return seen

    assert desyncio.run(main()) == [
        (0.0, 1, 0),  # a acquires
        (0.0, 1, 1),  # b queues
        (1.0, 0, 1),  # a releases (b granted, not yet running)
        (1.0, 1, 0),  # b acquires
        (2.0, 0, 0),  # b releases
    ]


def test_monitor_sees_cancelled_waiter_leave() -> None:
    async def main() -> list[tuple[int, int]]:
        seen: list[tuple[int, int]] = []
        res = Resource(1, monitor=lambda r: seen.append((r.count, r.queue_length)))

        async def hold() -> None:
            async with res:
                await sim.timeout(10)

        sim.spawn(hold())
        await sim.timeout(1)
        w = sim.spawn(hold())
        await sim.timeout(1)
        w.cancel()
        await asyncio.gather(w, return_exceptions=True)
        return list(seen)  # snapshot before shutdown cancels the holder

    assert desyncio.run(main()) == [(1, 0), (1, 1), (1, 0)]


@pytest.mark.parametrize("bad", [0, -1])
def test_capacity_value_validation(bad: int) -> None:
    with pytest.raises(ValueError, match="capacity"):
        Resource(bad)


@pytest.mark.parametrize("bad", [True, False, 1.0, "1", None])
def test_capacity_type_validation(bad: object) -> None:
    with pytest.raises(TypeError, match="capacity"):
        Resource(bad)  # type: ignore[arg-type]


def test_monitor_must_be_callable() -> None:
    with pytest.raises(TypeError, match="monitor"):
        Resource(1, monitor=3)  # type: ignore[arg-type]


async def _mm1_with_resource(
    n_customers: int = 300, arrival_rate: float = 0.9, service_rate: float = 1.0
) -> list[CustomerRecord]:
    """Same model as tests/models/mm1.py, but the server is a Resource."""
    arrivals = sim.rng("arrivals")
    service = sim.rng("service")
    server = Resource(1)
    records: list[CustomerRecord | None] = [None] * n_customers

    async def customer(index: int) -> None:
        arrival = sim.now
        async with server:
            start = sim.now
            await sim.timeout(service.expovariate(service_rate))
        records[index] = CustomerRecord(arrival, start, sim.now)

    tasks = []
    for i in range(n_customers):
        await sim.timeout(arrivals.expovariate(arrival_rate))
        tasks.append(sim.spawn(customer(i)))
    await asyncio.gather(*tasks)
    return [r for r in records if r is not None]


def test_mm1_matches_lock_reference() -> None:
    expected = desyncio.run(mm1(300), seed=7)
    actual = desyncio.run(_mm1_with_resource(300), seed=7)
    assert actual == expected
    assert len(actual) == 300


def test_determinism() -> None:
    async def main() -> list[tuple[float, str, int, int]]:
        res = Resource(2)
        rng = sim.rng("x")
        trace: list[tuple[float, str, int, int]] = []

        async def user(name: str) -> None:
            await sim.timeout(rng.random())
            async with res:
                trace.append((sim.now, name, res.count, res.queue_length))
                await sim.timeout(rng.random())

        await asyncio.gather(*(sim.spawn(user(f"u{i}")) for i in range(20)))
        return trace

    assert desyncio.run(main(), seed=3) == desyncio.run(main(), seed=3)
