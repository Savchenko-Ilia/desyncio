"""Tests for ``desyncio.resource.Resource`` (RES-01)."""

import asyncio
from collections.abc import Callable

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


# --- A raising monitor never changes the model (D023 v2) --------------------
#
# Monitor errors go to the loop's exception handler, as asyncio does for a
# failing call_soon callback; the operation completes normally for the task.


class MonitorBoom(Exception):
    """Raised by the test monitor."""


class RaisingMonitor:
    """Raises ``MonitorBoom`` whenever ``armed`` is set and ``when(r)`` holds."""

    def __init__(self, when: Callable[[Resource], bool] = lambda r: True) -> None:
        self.when = when
        self.armed = True

    def __call__(self, r: Resource) -> None:
        if self.armed and self.when(r):
            raise MonitorBoom


def _capture_loop_errors() -> list[dict[str, object]]:
    """Install an exception handler on the running loop that records contexts."""
    errors: list[dict[str, object]] = []
    asyncio.get_running_loop().set_exception_handler(
        lambda loop, context: errors.append(context)
    )
    return errors


def _assert_reported(errors: list[dict[str, object]], res: Resource, n: int) -> None:
    assert len(errors) == n
    for ctx in errors:
        assert isinstance(ctx["exception"], MonitorBoom)
        assert ctx["resource"] is res
        assert ctx["message"] == "Resource monitor raised"


async def _assert_clean_and_acquirable(res: Resource) -> None:
    assert (res.count, res.queue_length) == (0, 0)

    async def user() -> None:
        async with res:
            assert res.count == 1

    # A fresh task gets the unit at once: nothing is held or queued.
    t = sim.spawn(user())
    await sim.timeout(0)
    assert t.done()
    await t
    assert (res.count, res.queue_length) == (0, 0)


def test_raising_monitor_on_queue_enter_is_reported_and_waiter_served() -> None:
    async def main() -> None:
        errors = _capture_loop_errors()
        mon = RaisingMonitor(lambda r: (r.count, r.queue_length) == (1, 1))
        res = Resource(1, monitor=mon)
        got: list[tuple[str, float]] = []

        async def user(name: str) -> None:
            async with res:
                got.append((name, sim.now))
                await sim.timeout(2)

        holder = sim.spawn(user("holder"))
        await sim.timeout(1)
        waiter = sim.spawn(user("waiter"))
        await sim.timeout(0)
        assert (res.count, res.queue_length) == (1, 1)  # the request did queue
        await asyncio.gather(holder, waiter)
        assert got == [("holder", 0.0), ("waiter", 2.0)]
        _assert_reported(errors, res, 1)
        mon.armed = False
        await _assert_clean_and_acquirable(res)

    desyncio.run(main())


def test_raising_monitor_on_grant_is_reported_and_grant_stands() -> None:
    async def main() -> None:
        errors = _capture_loop_errors()
        mon = RaisingMonitor(lambda r: (r.count, r.queue_length) == (1, 0))
        res = Resource(1, monitor=mon)
        got: list[tuple[str, float, int]] = []

        async def user(name: str) -> None:
            async with res:
                got.append((name, sim.now, res.count))
                await sim.timeout(1)

        # Immediate grant (the probe from the review) and grant after waiting.
        await asyncio.gather(sim.spawn(user("a")), sim.spawn(user("b")))
        assert got == [("a", 0.0, 1), ("b", 1.0, 1)]
        _assert_reported(errors, res, 2)
        mon.armed = False
        await _assert_clean_and_acquirable(res)

    desyncio.run(main())


def test_raising_monitor_on_queue_leave_keeps_cancel() -> None:
    async def main() -> None:
        errors = _capture_loop_errors()
        mon = RaisingMonitor(lambda r: (r.count, r.queue_length) == (1, 0))
        mon.armed = False
        res = Resource(1, monitor=mon)
        seen: list[BaseException] = []

        async def hold() -> None:
            async with res:
                await sim.timeout(2)

        async def waiter() -> None:
            try:
                async with res:
                    pytest.fail("must not be granted")
            except BaseException as exc:
                seen.append(exc)
                raise

        holder = sim.spawn(hold())
        await sim.timeout(1)
        w = sim.spawn(waiter())
        await sim.timeout(0)
        assert res.queue_length == 1
        mon.armed = True  # fires when the cancelled waiter leaves the queue
        w.cancel()
        with pytest.raises(asyncio.CancelledError):
            await w
        assert w.cancelled()
        assert len(seen) == 1 and isinstance(seen[0], asyncio.CancelledError)
        assert (res.count, res.queue_length) == (1, 0)
        _assert_reported(errors, res, 1)
        mon.armed = False
        await holder
        await _assert_clean_and_acquirable(res)

    desyncio.run(main())


def test_raising_monitor_on_release_is_reported_and_unit_released() -> None:
    async def main() -> None:
        errors = _capture_loop_errors()
        mon = RaisingMonitor(lambda r: r.count == 0)
        res = Resource(1, monitor=mon)

        # Normal exit: no exception for the task.
        async with res:
            pass
        assert res.count == 0
        _assert_reported(errors, res, 1)

        # Exit by exception: the body's exception propagates unchanged.
        with pytest.raises(ValueError, match="body") as info:
            async with res:
                raise ValueError("body")
        assert info.value.__context__ is None
        assert res.count == 0
        _assert_reported(errors, res, 2)

        # Exit by cancellation: the cancel is not masked.
        async def hold() -> None:
            async with res:
                await sim.timeout(100)

        t = sim.spawn(hold())
        await sim.timeout(1)
        t.cancel()
        with pytest.raises(asyncio.CancelledError):
            await t
        assert t.cancelled()
        _assert_reported(errors, res, 3)
        mon.armed = False
        await _assert_clean_and_acquirable(res)

    desyncio.run(main())
