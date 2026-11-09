"""Tests for ``desyncio.resource.Resource`` (RES-01)."""

import asyncio
from collections.abc import Callable

import pytest

import desyncio
from desyncio import sim
from desyncio.interrupt import Interrupt
from desyncio.resource import PriorityResource, Resource
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
    arrivals = sim.random("arrivals")
    service = sim.random("service")
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
        rng = sim.random("x")
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


# --- PriorityResource (RES-02) ----------------------------------------------


async def _use(
    res: PriorityResource,
    log: list[str],
    name: str,
    priority: float | None,
    hold: float,
    delay: float = 0,
) -> None:
    await sim.timeout(delay)
    if priority is None:
        async with res:
            log.append(name)
            await sim.timeout(hold)
    else:
        async with res.request(priority=priority):
            log.append(name)
            await sim.timeout(hold)


def test_priority_order_and_fifo_within_equal_priority() -> None:
    async def main() -> list[str]:
        res = PriorityResource(1)
        log: list[str] = []
        tasks = [sim.spawn(_use(res, log, "holder", 0, 5))]
        await sim.timeout(1)
        for name, prio in [("p2a", 2), ("p1a", 1), ("p2b", 2), ("p1b", 1), ("p0", 0)]:
            tasks.append(sim.spawn(_use(res, log, name, prio, 1)))
            await sim.timeout(0.1)
        assert res.queue_length == 5
        await asyncio.gather(*tasks)
        return log

    assert desyncio.run(main()) == ["holder", "p0", "p1a", "p1b", "p2a", "p2b"]


def test_plain_async_with_is_priority_zero() -> None:
    async def main() -> list[str]:
        res = PriorityResource(1)
        log: list[str] = []
        tasks = [sim.spawn(_use(res, log, "holder", 0, 5))]
        await sim.timeout(1)
        tasks.append(sim.spawn(_use(res, log, "worse", 1, 1)))
        tasks.append(sim.spawn(_use(res, log, "plain", None, 1)))
        tasks.append(sim.spawn(_use(res, log, "better", -1, 1)))
        await asyncio.gather(*tasks)
        return log

    assert desyncio.run(main()) == ["holder", "better", "plain", "worse"]


def test_better_priority_newcomer_at_release_instant_goes_first() -> None:
    async def main() -> list[str]:
        res = PriorityResource(1)
        log: list[str] = []
        a = sim.spawn(_use(res, log, "holder", 0, 2))
        b = sim.spawn(_use(res, log, "waiter", 5, 1, delay=1))
        c = sim.spawn(_use(res, log, "vip", 1, 1, delay=2))  # same instant as release
        await asyncio.gather(a, b, c)
        return log

    assert desyncio.run(main()) == ["holder", "vip", "waiter"]


def test_no_barging_equal_or_worse_newcomer_at_release_instant() -> None:
    async def main() -> list[str]:
        res = PriorityResource(1)
        log: list[str] = []
        a = sim.spawn(_use(res, log, "holder", 0, 2))
        b = sim.spawn(_use(res, log, "waiter", 3, 1, delay=1))
        c = sim.spawn(_use(res, log, "equal", 3, 1, delay=2))
        d = sim.spawn(_use(res, log, "worse", 4, 1, delay=2))
        await asyncio.gather(a, b, c, d)
        return log

    assert desyncio.run(main()) == ["holder", "waiter", "equal", "worse"]


def test_capacity_two_counters_and_order() -> None:
    async def main() -> list[tuple[str, float]]:
        res = PriorityResource(2)
        assert (res.capacity, res.count, res.queue_length) == (2, 0, 0)
        log: list[tuple[str, float]] = []

        async def user(name: str, prio: int, hold: float) -> None:
            async with res.request(priority=prio):
                assert res.count <= 2
                log.append((name, sim.now))
                await sim.timeout(hold)

        tasks = [sim.spawn(user("a", 0, 2)), sim.spawn(user("b", 0, 3))]
        await sim.timeout(0.5)
        for name, prio in [("low", 9), ("high", 1), ("mid", 5)]:
            tasks.append(sim.spawn(user(name, prio, 1)))
        await sim.timeout(0)
        assert (res.count, res.queue_length) == (2, 3)
        await asyncio.gather(*tasks)
        assert (res.count, res.queue_length) == (0, 0)
        return log

    assert desyncio.run(main()) == [
        ("a", 0.0),
        ("b", 0.0),
        ("high", 2.0),  # a releases at 2
        ("mid", 3.0),  # b releases at 3
        ("low", 3.0),  # high releases at 3 (same instant as b)
    ]


def test_two_units_freed_at_same_instant_go_to_two_best() -> None:
    async def main() -> list[str]:
        res = PriorityResource(2)
        log: list[str] = []
        tasks = [
            sim.spawn(_use(res, log, "h1", 0, 2)),
            sim.spawn(_use(res, log, "h2", 0, 2)),
        ]
        await sim.timeout(0.5)
        # Arrival order is the reverse of priority, so Condition's FIFO wake-up
        # order disagrees with key order.
        for name, prio in [("c", 3), ("b", 2), ("a", 1)]:
            tasks.append(sim.spawn(_use(res, log, name, prio, 1)))
        await asyncio.gather(*tasks)
        return log

    assert desyncio.run(main()) == ["h1", "h2", "a", "b", "c"]


def test_priority_cancelled_waiter_is_removed() -> None:
    async def main() -> list[str]:
        res = PriorityResource(1)
        log: list[str] = []
        holder = sim.spawn(_use(res, log, "holder", 0, 3))
        await sim.timeout(1)
        doomed = sim.spawn(_use(res, log, "doomed", 0, 1))  # best key
        later = sim.spawn(_use(res, log, "later", 5, 1))
        await sim.timeout(1)
        assert (res.count, res.queue_length) == (1, 2)
        doomed.cancel()
        with pytest.raises(asyncio.CancelledError):
            await doomed
        assert (res.count, res.queue_length) == (1, 1)
        await asyncio.gather(holder, later)
        assert (res.count, res.queue_length) == (0, 0)
        return log

    assert desyncio.run(main()) == ["holder", "later"]


def test_cancelled_at_instant_unit_is_freed_passes_unit_on() -> None:
    async def main() -> list[str]:
        res = PriorityResource(1)
        log: list[str] = []
        holder = sim.spawn(_use(res, log, "holder", 0, 1))
        await sim.timeout(0.5)
        doomed = sim.spawn(_use(res, log, "doomed", 0, 1))
        later = sim.spawn(_use(res, log, "later", 5, 1))
        await sim.timeout(0.5)  # t=1: holder releases; doomed has been woken
        doomed.cancel()  # ...but has not run yet
        with pytest.raises(asyncio.CancelledError):
            await doomed
        await asyncio.gather(holder, later)
        assert (res.count, res.queue_length) == (0, 0)
        return log

    assert desyncio.run(main()) == ["holder", "later"]


def test_interrupt_of_waiting_request_removes_it() -> None:
    async def main() -> list[str]:
        res = PriorityResource(1)
        log: list[str] = []
        outcome: list[object] = []

        async def interruptible_user() -> None:
            try:
                async with sim.interruptible():
                    async with res.request(priority=0):
                        log.append("victim")
            except Interrupt as i:
                outcome.append(i.cause)

        holder = sim.spawn(_use(res, log, "holder", 0, 3))
        await sim.timeout(1)
        victim = sim.spawn(interruptible_user())
        later = sim.spawn(_use(res, log, "later", 5, 1))
        await sim.timeout(1)
        assert res.queue_length == 2
        sim.interrupt(victim, "boom")
        await victim
        assert outcome == ["boom"]
        assert (res.count, res.queue_length) == (1, 1)
        await asyncio.gather(holder, later)
        assert (res.count, res.queue_length) == (0, 0)
        return log

    assert desyncio.run(main()) == ["holder", "later"]


def test_release_on_exception_and_holder_cancellation() -> None:
    async def main() -> None:
        res = PriorityResource(1)
        with pytest.raises(ValueError, match="body"):
            async with res.request(priority=1):
                raise ValueError("body")
        assert res.count == 0

        async def hold() -> None:
            async with res:
                await sim.timeout(100)

        t = sim.spawn(hold())
        await sim.timeout(1)
        assert res.count == 1
        t.cancel()
        await asyncio.gather(t, return_exceptions=True)
        assert res.count == 0

    desyncio.run(main())


def test_priority_properties_read_only_and_validation() -> None:
    res = PriorityResource(1)
    for name in ("capacity", "count", "queue_length"):
        with pytest.raises(AttributeError):
            setattr(res, name, 5)
    for bad_cap in (0, -1):
        with pytest.raises(ValueError):
            PriorityResource(bad_cap)
    for bad_cap_t in (True, 1.0, "1", None):
        with pytest.raises(TypeError):
            PriorityResource(bad_cap_t)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        PriorityResource(1, monitor=3)  # type: ignore[arg-type]


@pytest.mark.parametrize("good", [0, -3, 2.5, float("inf"), -float("inf")])
def test_priority_accepts_real_numbers(good: float) -> None:
    assert PriorityResource(1).request(priority=good).priority == good


@pytest.mark.parametrize("bad", [True, False, "1", None, 1 + 2j, [1]])
def test_priority_type_rejected(bad: object) -> None:
    with pytest.raises(TypeError):
        PriorityResource(1).request(priority=bad)  # type: ignore[arg-type]


def test_priority_nan_rejected() -> None:
    with pytest.raises(ValueError, match="NaN"):
        PriorityResource(1).request(priority=float("nan"))


def test_request_is_single_use() -> None:
    async def main() -> None:
        res = PriorityResource(1)
        req = res.request(1)
        async with req:
            pass
        with pytest.raises(RuntimeError, match="only once"):
            async with req:
                pass
        assert (res.count, res.queue_length) == (0, 0)

    desyncio.run(main())


def test_priority_monitor_hook_calls() -> None:
    async def main() -> list[tuple[float, int, int]]:
        seen: list[tuple[float, int, int]] = []
        res = PriorityResource(
            1, monitor=lambda r: seen.append((sim.now, r.count, r.queue_length))
        )
        log: list[str] = []
        await asyncio.gather(
            sim.spawn(_use(res, log, "a", 0, 1)), sim.spawn(_use(res, log, "b", 0, 1))
        )
        return seen

    assert desyncio.run(main()) == [
        (0.0, 1, 0),  # a acquires
        (0.0, 1, 1),  # b queues
        (1.0, 0, 1),  # a releases
        (1.0, 1, 0),  # b acquires
        (2.0, 0, 0),  # b releases
    ]


def test_priority_monitor_sees_cancelled_waiter_leave() -> None:
    async def main() -> list[tuple[int, int]]:
        seen: list[tuple[int, int]] = []
        res = PriorityResource(
            1, monitor=lambda r: seen.append((r.count, r.queue_length))
        )
        log: list[str] = []
        sim.spawn(_use(res, log, "h", 0, 10))
        await sim.timeout(1)
        w = sim.spawn(_use(res, log, "w", 0, 10))
        await sim.timeout(1)
        w.cancel()
        await asyncio.gather(w, return_exceptions=True)
        return list(seen)

    assert desyncio.run(main()) == [(1, 0), (1, 1), (1, 0)]


def test_priority_raising_monitor_is_reported_and_nothing_leaks() -> None:
    async def main() -> None:
        errors = _capture_loop_errors()

        def boom(r: PriorityResource) -> None:
            raise MonitorBoom

        res = PriorityResource(1, monitor=boom)
        log: list[str] = []
        holder = sim.spawn(_use(res, log, "holder", 0, 2))
        await sim.timeout(1)
        waiter = sim.spawn(_use(res, log, "waiter", 1, 1))
        doomed = sim.spawn(_use(res, log, "doomed", 0, 1))
        await sim.timeout(0.5)
        doomed.cancel()  # a CancelledError must not be masked by the monitor
        with pytest.raises(asyncio.CancelledError):
            await doomed
        await asyncio.gather(holder, waiter)
        assert log == ["holder", "waiter"]
        assert (res.count, res.queue_length) == (0, 0)
        # holder: enter, release; waiter: queue, enter, release; doomed: queue, leave
        assert len(errors) == 7
        for ctx in errors:
            assert isinstance(ctx["exception"], MonitorBoom)
            assert ctx["resource"] is res
        async with PriorityResource(1) as fresh:
            assert fresh.count == 1

    desyncio.run(main())


def test_priority_monitor_keyboard_interrupt_propagates() -> None:
    async def main() -> None:
        def stop(r: PriorityResource) -> None:
            raise KeyboardInterrupt

        res = PriorityResource(1, monitor=stop)
        async with res:
            pass  # pragma: no cover

    with pytest.raises(KeyboardInterrupt):
        desyncio.run(main())


async def _mm1_two_classes(
    n_customers: int = 200,
) -> list[tuple[int, float, float, float]]:
    """M/M/1 with a priority server; class 0 (30%) is served before class 1."""
    arrivals = sim.random("arrivals")
    service = sim.random("service")
    classes = sim.random("classes")
    server = PriorityResource(1)
    records: list[tuple[int, float, float, float] | None] = [None] * n_customers

    async def customer(index: int, cls: int) -> None:
        arrival = sim.now
        async with server.request(priority=cls):
            start = sim.now
            await sim.timeout(service.expovariate(1.0))
        records[index] = (cls, arrival, start, sim.now)

    tasks = []
    for i in range(n_customers):
        await sim.timeout(arrivals.expovariate(0.85))
        cls = 0 if classes.random() < 0.3 else 1
        tasks.append(sim.spawn(customer(i, cls)))
    await asyncio.gather(*tasks)
    return [r for r in records if r is not None]


def test_mm1_two_classes_high_priority_waits_less() -> None:
    recs = desyncio.run(_mm1_two_classes(), seed=11)
    assert len(recs) == 200

    def mean_wait(cls: int) -> float:
        waits = [s - a for c, a, s, _ in recs if c == cls]
        return sum(waits) / len(waits)

    assert mean_wait(0) < mean_wait(1)
    # One server: service intervals never overlap.
    spans = sorted((s, d) for _, _, s, d in recs)
    assert all(a[1] <= b[0] for a, b in zip(spans, spans[1:], strict=False))


def test_priority_determinism() -> None:
    assert desyncio.run(_mm1_two_classes(), seed=5) == desyncio.run(
        _mm1_two_classes(), seed=5
    )
    assert desyncio.run(_mm1_two_classes(), seed=5) != desyncio.run(
        _mm1_two_classes(), seed=6
    )


# --- RES-02 fix round 1: release is synchronous (D030) ----------------------
#
# The round-0 design released through ``async with self._cond:``, which can
# suspend when the Condition's lock is contended, so a cancel or interrupt
# arriving at the holder's exit skipped the decrement and leaked the unit.
# With the public API alone that lock is never contended, so the reviewer's
# probe held it from a helper task. ``_hold_round0_lock`` reproduces that probe
# when the resource has such a lock (round 0) and does nothing otherwise
# (D030: there is no lock, release cannot suspend). The assertions are on the
# public outcome only, so they hold for any correct design.


async def _hold_round0_lock(res: PriorityResource, until: float) -> None:
    cond = getattr(res, "_cond", None)
    if cond is None:  # per-request futures (D030): nothing to contend
        return
    async with cond:
        await sim.timeout(until - sim.now)


def _spawn_holder_and_waiters(
    res: PriorityResource, log: list[str]
) -> list["asyncio.Task[None]"]:
    """Waiters queued behind a holder that exits at t=5 (see each test)."""
    return [
        sim.spawn(_use(res, log, "w0", 0, 1, delay=1)),
        sim.spawn(_use(res, log, "w1", 1, 1, delay=1)),
    ]


def test_cancel_at_holder_exit_does_not_leak_unit() -> None:
    async def main() -> None:
        res = PriorityResource(1)
        log: list[str] = []

        async def holder() -> None:
            async with res:
                log.append("holder")
                await sim.timeout(5)
                # A cancel arrives at the very instant the holder exits.
                asyncio.current_task().cancel()  # type: ignore[union-attr]

        h = sim.spawn(holder())
        waiters = _spawn_holder_and_waiters(res, log)
        await sim.timeout(4)
        helper = sim.spawn(_hold_round0_lock(res, until=5.5))
        await sim.timeout(16)  # t=20: far past every service
        assert h.cancelled()
        assert (res.count, res.queue_length) == (0, 0)
        assert log == ["holder", "w0", "w1"]
        assert all(w.done() for w in waiters)
        await helper

    desyncio.run(main())


def test_body_exception_and_cancel_at_exit_release_and_keep_body_error() -> None:
    async def main() -> None:
        res = PriorityResource(1)
        log: list[str] = []
        seen: list[BaseException] = []

        async def holder() -> None:
            try:
                async with res:
                    log.append("holder")
                    await sim.timeout(5)
                    asyncio.current_task().cancel()  # type: ignore[union-attr]
                    raise ValueError("body")
            except BaseException as exc:
                seen.append(exc)
            # The pending cancel is delivered at the next await, as in asyncio.
            await sim.timeout(1)

        h = sim.spawn(holder())
        waiters = _spawn_holder_and_waiters(res, log)
        await sim.timeout(4)
        helper = sim.spawn(_hold_round0_lock(res, until=5.5))
        await sim.timeout(16)
        assert len(seen) == 1 and isinstance(seen[0], ValueError)  # not replaced
        assert h.cancelled()
        assert (res.count, res.queue_length) == (0, 0)
        assert log == ["holder", "w0", "w1"]
        assert all(w.done() for w in waiters)
        await helper

    desyncio.run(main())


def test_interrupt_of_holder_at_exit_does_not_leak_unit() -> None:
    async def main() -> None:
        res = PriorityResource(1)
        log: list[str] = []
        outcome: list[object] = []

        async def holder() -> None:
            try:
                async with sim.interruptible():
                    async with res:
                        log.append("holder")
                        await sim.timeout(5)
                    await sim.timeout(1)  # still inside the interruptible block
            except Interrupt as i:
                outcome.append(i.cause)

        h = sim.spawn(holder())
        waiters = _spawn_holder_and_waiters(res, log)
        await sim.timeout(4)
        helper = sim.spawn(_hold_round0_lock(res, until=5.5))
        await sim.timeout(1)  # t=5, right after the holder's exit step
        sim.interrupt(h, "boom")
        await sim.timeout(15)
        assert outcome == ["boom"]
        assert h.done() and not h.cancelled()
        assert (res.count, res.queue_length) == (0, 0)
        assert log == ["holder", "w0", "w1"]
        assert all(w.done() for w in waiters)
        await helper

    desyncio.run(main())


def test_waiter_cancelled_when_granted_passes_unit_to_next() -> None:
    async def main() -> list[tuple[str, float]]:
        seen: list[tuple[int, int]] = []

        def monitor(r: PriorityResource) -> None:
            assert 0 <= r.count <= r.capacity
            seen.append((r.count, r.queue_length))

        res = PriorityResource(1, monitor=monitor)
        log: list[tuple[str, float]] = []

        async def user(name: str, prio: int, hold: float) -> None:
            async with res.request(priority=prio):
                log.append((name, sim.now))
                await sim.timeout(hold)

        holder = sim.spawn(user("holder", 0, 1))
        await sim.timeout(0.5)
        doomed = sim.spawn(user("doomed", 0, 1))
        nxt = sim.spawn(user("next", 1, 1))
        last = sim.spawn(user("last", 2, 1))
        await sim.timeout(0.5)  # t=1: holder released and handed doomed the unit
        doomed.cancel()  # ...but doomed has not resumed yet
        with pytest.raises(asyncio.CancelledError):
            await doomed
        await sim.timeout(0)
        assert sim.now == 1.0
        assert (res.count, res.queue_length) == (1, 1)  # next holds, last waits
        await asyncio.gather(holder, nxt, last)
        assert (res.count, res.queue_length) == (0, 0)
        assert seen[-1] == (0, 0)
        return log

    assert desyncio.run(main()) == [("holder", 0.0), ("next", 1.0), ("last", 2.0)]
