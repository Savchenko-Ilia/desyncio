"""Tests for ``desyncio.resource``: Resource (RES-01), PriorityResource (RES-02)
and PreemptiveResource (RES-03).

``Resource`` and ``PriorityResource`` share a contract, so most of their tests
are parametrized over both classes (``RESOURCES``).
"""

import asyncio
import math

import pytest

import desyncio
from desyncio import sim
from desyncio.interrupt import Interrupt
from desyncio.resource import (
    Preempted,
    PreemptiveResource,
    PriorityResource,
    Resource,
)
from models.mm1 import CustomerRecord, mm1

RESOURCES = [Resource, PriorityResource]
AnyResource = Resource | PriorityResource


# --- Resource (RES-01) ------------------------------------------------------
#
# FIFO order itself is asyncio.Semaphore's job and is not re-tested here (the
# bank example in docs/primitives.md covers it). The no-barging test below is
# kept on purpose: it is a guard in case Resource ever moves off Semaphore.


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


@pytest.mark.parametrize("cls", RESOURCES)
def test_properties_are_read_only(cls: type[AnyResource]) -> None:
    res = cls(1)
    for name in ("capacity", "count", "queue_length"):
        with pytest.raises(AttributeError):
            setattr(res, name, 5)


def test_preempted_is_frozen() -> None:
    async def main() -> None:
        task = asyncio.current_task()
        assert task is not None
        p = Preempted(by=task, usage_since=1.0, resource=PreemptiveResource(1))
        with pytest.raises(AttributeError):
            p.usage_since = 2.0  # type: ignore[misc]

    desyncio.run(main())


@pytest.mark.parametrize("cls", RESOURCES)
def test_release_on_body_exception_and_holder_cancel(cls: type[AnyResource]) -> None:
    async def main() -> None:
        res = cls(1)
        with pytest.raises(ZeroDivisionError):
            async with res:
                assert res.count == 1
                1 / 0  # noqa: B018
        assert res.count == 0

        async def hold() -> None:
            async with res:
                await sim.timeout(100)

        t = sim.spawn(hold())
        await sim.timeout(1)
        assert res.count == 1
        t.cancel()
        with pytest.raises(asyncio.CancelledError):
            await t
        assert res.count == 0
        async with res:  # the unit is available again
            assert res.count == 1

    desyncio.run(main())


@pytest.mark.parametrize("cls", RESOURCES)
def test_monitor_hook_calls(cls: type[AnyResource]) -> None:
    async def main() -> list[tuple[float, int, int]]:
        seen: list[tuple[float, int, int]] = []
        res = cls(1, monitor=lambda r: seen.append((sim.now, r.count, r.queue_length)))

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


@pytest.mark.parametrize("cls", RESOURCES)
def test_monitor_sees_cancelled_waiter_leave(cls: type[AnyResource]) -> None:
    async def main() -> list[tuple[int, int]]:
        seen: list[tuple[int, int]] = []
        res = cls(1, monitor=lambda r: seen.append((r.count, r.queue_length)))

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


@pytest.mark.parametrize("cls", RESOURCES)
@pytest.mark.parametrize(
    ("kwargs", "error", "match"),
    [
        ({"capacity": 0}, ValueError, "capacity"),
        ({"capacity": -1}, ValueError, "capacity"),
        ({"capacity": True}, TypeError, "capacity"),
        ({"capacity": 1.0}, TypeError, "capacity"),
        ({"capacity": "1"}, TypeError, "capacity"),
        ({"capacity": None}, TypeError, "capacity"),
        ({"monitor": 3}, TypeError, "monitor"),
    ],
)
def test_constructor_validation(
    cls: type[AnyResource],
    kwargs: dict[str, object],
    error: type[Exception],
    match: str,
) -> None:
    with pytest.raises(error, match=match):
        cls(**kwargs)  # type: ignore[arg-type]


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


@pytest.mark.parametrize("cls", RESOURCES)
def test_waiter_cancelled_when_granted_passes_unit_to_next(
    cls: type[AnyResource],
) -> None:
    # D030 regression. The holder's release hands the unit to ``doomed`` at
    # t=1, but ``doomed`` has not resumed yet when it is cancelled. The unit
    # must go on to ``next``, not leak.
    async def main() -> list[tuple[str, float]]:
        seen: list[tuple[int, int]] = []

        def monitor(r: AnyResource) -> None:
            assert 0 <= r.count <= r.capacity
            seen.append((r.count, r.queue_length))

        res = cls(1, monitor=monitor)
        log: list[tuple[str, float]] = []

        async def user(name: str, hold: float) -> None:
            async with res:
                log.append((name, sim.now))
                await sim.timeout(hold)

        holder = sim.spawn(user("holder", 1))
        await sim.timeout(0.5)
        doomed = sim.spawn(user("doomed", 1))
        nxt = sim.spawn(user("next", 1))
        last = sim.spawn(user("last", 1))
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


# --- A raising monitor never changes the model (D023 v2) --------------------
#
# Monitor errors go to the loop's exception handler, as asyncio does for a
# failing call_soon callback; the operation completes normally for the task.
# This is the RES-01 regression: a raising monitor used to leave a unit held
# forever.


class MonitorBoom(Exception):
    """Raised by the test monitors."""


def _capture_loop_errors() -> list[dict[str, object]]:
    """Install an exception handler on the running loop that records contexts."""
    errors: list[dict[str, object]] = []
    asyncio.get_running_loop().set_exception_handler(
        lambda loop, context: errors.append(context)
    )
    return errors


async def _assert_clean_and_acquirable(res: AnyResource) -> None:
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


@pytest.mark.parametrize("cls", RESOURCES)
def test_raising_monitor_never_changes_model(cls: type[AnyResource]) -> None:
    async def main() -> None:
        errors = _capture_loop_errors()

        def boom(r: AnyResource) -> None:
            raise MonitorBoom

        res = cls(1, monitor=boom)
        got: list[tuple[str, float]] = []
        cancel_seen: list[BaseException] = []

        async def user(name: str, hold: float) -> None:
            async with res:
                got.append((name, sim.now))
                await sim.timeout(hold)

        async def doomed() -> None:
            try:
                async with res:
                    pytest.fail("must not be granted")
            except BaseException as exc:
                cancel_seen.append(exc)
                raise

        # A holder and a queued waiter, which is still served.
        holder = sim.spawn(user("holder", 2))
        await sim.timeout(1)
        waiter = sim.spawn(user("waiter", 1))
        victim = sim.spawn(doomed())
        await sim.timeout(0.5)
        assert (res.count, res.queue_length) == (1, 2)  # both did queue
        # A cancelled waiter still sees its CancelledError.
        victim.cancel()
        with pytest.raises(asyncio.CancelledError):
            await victim
        assert victim.cancelled()
        assert len(cancel_seen) == 1
        assert isinstance(cancel_seen[0], asyncio.CancelledError)
        await asyncio.gather(holder, waiter)
        assert got == [("holder", 0.0), ("waiter", 2.0)]

        # Exit by exception: the body's exception propagates unchanged.
        with pytest.raises(ValueError, match="body") as info:
            async with res:
                raise ValueError("body")
        assert info.value.__context__ is None

        # Exit by cancelling the holder: the cancel is not masked.
        t = sim.spawn(user("cancelled holder", 100))
        await sim.timeout(1)
        t.cancel()
        with pytest.raises(asyncio.CancelledError):
            await t
        assert t.cancelled()

        # holder: acquire, release; waiter: queue, acquire, release;
        # victim: queue, leave; body exception: acquire, release;
        # cancelled holder: acquire, release.
        assert len(errors) == 11
        for ctx in errors:
            assert isinstance(ctx["exception"], MonitorBoom)
            assert ctx["resource"] is res
            assert ctx["message"] == "Resource monitor raised"
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
        # Arrival order is the reverse of priority, so first-come first-served
        # order would disagree with key order.
        for name, prio in [("c", 3), ("b", 2), ("a", 1)]:
            tasks.append(sim.spawn(_use(res, log, name, prio, 1)))
        await asyncio.gather(*tasks)
        return log

    assert desyncio.run(main()) == ["h1", "h2", "a", "b", "c"]


@pytest.mark.parametrize("how", ["cancel", "interrupt"])
def test_cancelled_or_interrupted_waiter_is_removed(how: str) -> None:
    # An interrupt is a cancel underneath (D024), so both must take the
    # waiting request out of the order.
    async def main() -> list[str]:
        res = PriorityResource(1)
        log: list[str] = []
        causes: list[object] = []

        async def doomed() -> None:
            try:
                async with sim.interruptible():
                    async with res.request(priority=0):  # best key
                        log.append("doomed")
            except Interrupt as exc:
                causes.append(exc.cause)

        holder = sim.spawn(_use(res, log, "holder", 0, 3))
        await sim.timeout(1)
        victim = sim.spawn(doomed())
        later = sim.spawn(_use(res, log, "later", 5, 1))
        await sim.timeout(1)
        assert (res.count, res.queue_length) == (1, 2)
        if how == "cancel":
            victim.cancel()
            with pytest.raises(asyncio.CancelledError):
                await victim
        else:
            sim.interrupt(victim, "boom")
            await victim
            assert causes == ["boom"]
        assert (res.count, res.queue_length) == (1, 1)
        await asyncio.gather(holder, later)
        assert (res.count, res.queue_length) == (0, 0)
        return log

    assert desyncio.run(main()) == ["holder", "later"]


@pytest.mark.parametrize(
    ("priority", "error"),
    [
        (0, None),
        (-3, None),
        (2.5, None),
        (math.inf, None),
        (-math.inf, None),
        (True, TypeError),
        (None, TypeError),
        ("1", TypeError),
        (1 + 2j, TypeError),
        (math.nan, ValueError),
    ],
)
def test_priority_validation(priority: object, error: type[Exception] | None) -> None:
    res = PriorityResource(1)
    if error is None:
        assert res.request(priority=priority).priority == priority  # type: ignore[arg-type]
    else:
        with pytest.raises(error):
            res.request(priority=priority)  # type: ignore[arg-type]


@pytest.mark.parametrize("cls", [PriorityResource, PreemptiveResource])
def test_request_is_single_use(cls: type[PriorityResource]) -> None:
    async def main() -> None:
        res = cls(1)
        req = res.request()
        async with req:
            pass
        with pytest.raises(RuntimeError, match="only once"):
            async with req:
                pass
        assert (res.count, res.queue_length) == (0, 0)

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


def test_mm1_two_classes_is_deterministic_and_high_priority_waits_less() -> None:
    first = desyncio.run(_mm1_two_classes(), seed=5)
    assert first == desyncio.run(_mm1_two_classes(), seed=5)
    assert first != desyncio.run(_mm1_two_classes(), seed=6)

    recs = desyncio.run(_mm1_two_classes(), seed=11)
    assert len(recs) == 200

    def mean_wait(cls: int) -> float:
        waits = [s - a for c, a, s, _ in recs if c == cls]
        return sum(waits) / len(waits)

    assert mean_wait(0) < mean_wait(1)
    # One server: service intervals never overlap.
    spans = sorted((s, d) for _, _, s, d in recs)
    assert all(a[1] <= b[0] for a, b in zip(spans, spans[1:], strict=False))


# --- RES-02 fix round 1: release is synchronous (D030) ----------------------
#
# The round-0 design released through ``async with self._cond:``, which can
# suspend when the Condition's lock is contended, so a cancel or interrupt
# arriving at the holder's exit skipped the decrement and leaked the unit.
# Release is now synchronous. The assertions are on the public outcome only.


def _spawn_holder_and_waiters(
    res: PriorityResource, log: list[str]
) -> list["asyncio.Task[None]"]:
    """Waiters queued behind a holder that exits at t=5 (see each test)."""
    return [
        sim.spawn(_use(res, log, "w0", 0, 1, delay=1)),
        sim.spawn(_use(res, log, "w1", 1, 1, delay=1)),
    ]


@pytest.mark.parametrize("cls", [PriorityResource, PreemptiveResource])
def test_cancel_at_holder_exit_does_not_leak_unit(cls: type[PriorityResource]) -> None:
    # The PreemptiveResource twin goes through the Interruptible scope on exit.
    async def main() -> None:
        res = cls(1)
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
        await sim.timeout(16)  # t=20: far past every service
        assert h.cancelled()
        assert (res.count, res.queue_length) == (0, 0)
        assert log == ["holder", "w0", "w1"]
        assert all(w.done() for w in waiters)

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
        await sim.timeout(16)
        assert len(seen) == 1 and isinstance(seen[0], ValueError)  # not replaced
        assert h.cancelled()
        assert (res.count, res.queue_length) == (0, 0)
        assert log == ["holder", "w0", "w1"]
        assert all(w.done() for w in waiters)

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
        await sim.timeout(1)  # t=5, right after the holder's exit step
        sim.interrupt(h, "boom")
        await sim.timeout(15)
        assert outcome == ["boom"]
        assert h.done() and not h.cancelled()
        assert (res.count, res.queue_length) == (0, 0)
        assert log == ["holder", "w0", "w1"]
        assert all(w.done() for w in waiters)

    desyncio.run(main())


# --- PreemptiveResource (RES-03, D032) ---------------------------------------


async def _victim_job(
    res: PreemptiveResource,
    log: list[tuple[str, str, float]],
    name: str,
    priority: float,
    work: float,
    *,
    preempt: bool = True,
    delay: float = 0,
) -> None:
    """Hold ``res`` for ``work``; log start/done/preempted with virtual times."""
    await sim.timeout(delay)
    try:
        async with res.request(priority=priority, preempt=preempt):
            log.append((name, "start", sim.now))
            await sim.timeout(work)
            log.append((name, "done", sim.now))
    except Interrupt as exc:
        assert isinstance(exc.cause, Preempted)
        log.append((name, "preempted", sim.now))


def test_preemption_basic_same_instant_and_cause() -> None:
    async def main() -> None:
        res = PreemptiveResource(1)
        log: list[tuple[str, str, float]] = []
        causes: list[Preempted] = []

        async def low() -> None:
            try:
                async with res.request(priority=5):
                    await sim.timeout(10)
            except Interrupt as exc:
                assert isinstance(exc.cause, Preempted)
                causes.append(exc.cause)
                log.append(("low", "preempted", sim.now))

        lo = sim.spawn(low())
        await sim.timeout(3)
        hi = sim.spawn(_victim_job(res, log, "high", 0, 2))
        await sim.timeout(0)
        assert (res.count, res.queue_length) == (1, 0)  # swapped at t=3
        await asyncio.gather(lo, hi)
        assert log == [
            ("high", "start", 3.0),  # same virtual time as the request
            ("low", "preempted", 3.0),
            ("high", "done", 5.0),
        ]
        assert causes[0].by is hi
        assert causes[0].usage_since == 0.0
        assert causes[0].resource is res
        assert (res.count, res.queue_length) == (0, 0)

    desyncio.run(main())


def test_preempted_dataclass_is_frozen() -> None:
    async def main() -> None:
        task = asyncio.current_task()
        assert task is not None
        p = Preempted(by=task, usage_since=1.0, resource=PreemptiveResource(1))
        with pytest.raises(AttributeError):
            p.usage_since = 2.0  # type: ignore[misc]

    desyncio.run(main())


def test_machine_shop_resubmits_remaining_work() -> None:
    async def main() -> list[tuple[str, float]]:
        res = PreemptiveResource(1)
        log: list[tuple[str, float]] = []

        async def job() -> None:
            remaining = 10.0
            while remaining > 0:
                try:
                    async with res.request(priority=5):
                        log.append(("job start", sim.now))
                        await sim.timeout(remaining)
                        remaining = 0
                except Interrupt as exc:
                    assert isinstance(exc.cause, Preempted)
                    remaining -= sim.now - exc.cause.usage_since
                    log.append(("job preempted", sim.now))
            log.append(("job done", sim.now))

        async def urgent() -> None:
            await sim.timeout(4)
            async with res.request(priority=0):
                log.append(("urgent start", sim.now))
                await sim.timeout(3)

        await asyncio.gather(sim.spawn(job()), sim.spawn(urgent()))
        return log

    assert desyncio.run(main()) == [
        ("job start", 0.0),
        ("urgent start", 4.0),
        ("job preempted", 4.0),
        ("job start", 7.0),
        ("job done", 13.0),
    ]


def test_preempt_false_never_preempts() -> None:
    async def main() -> list[tuple[str, str, float]]:
        res = PreemptiveResource(1)
        log: list[tuple[str, str, float]] = []
        await asyncio.gather(
            sim.spawn(_victim_job(res, log, "low", 5, 4)),
            sim.spawn(_victim_job(res, log, "high", 0, 1, preempt=False, delay=1)),
        )
        return log

    assert desyncio.run(main()) == [
        ("low", "start", 0.0),
        ("low", "done", 4.0),
        ("high", "start", 4.0),
        ("high", "done", 5.0),
    ]


def test_equal_priority_never_preempts() -> None:
    async def main() -> list[tuple[str, str, float]]:
        res = PreemptiveResource(1)
        log: list[tuple[str, str, float]] = []
        await asyncio.gather(
            sim.spawn(_victim_job(res, log, "first", 3, 4)),
            sim.spawn(_victim_job(res, log, "second", 3, 1, delay=1)),
        )
        return log

    assert desyncio.run(main()) == [
        ("first", "start", 0.0),
        ("first", "done", 4.0),
        ("second", "start", 4.0),
        ("second", "done", 5.0),
    ]


def test_worse_priority_request_never_preempts() -> None:
    async def main() -> list[tuple[str, str, float]]:
        res = PreemptiveResource(1)
        log: list[tuple[str, str, float]] = []
        await asyncio.gather(
            sim.spawn(_victim_job(res, log, "good", 1, 4)),
            sim.spawn(_victim_job(res, log, "bad", 9, 1, delay=1)),
        )
        return log

    assert [e[:2] for e in desyncio.run(main())] == [
        ("good", "start"),
        ("good", "done"),
        ("bad", "start"),
        ("bad", "done"),
    ]


def test_better_waiting_non_preempting_request_is_not_jumped() -> None:
    # H holds (prio 5). W (prio 1, preempt=False) waits. P (prio 3, preempt=True)
    # is better than H but is not at the head of the queue (W's key is better),
    # so it must not preempt (RES-02's no-barging rule).
    async def main() -> list[tuple[str, str, float]]:
        res = PreemptiveResource(1)
        log: list[tuple[str, str, float]] = []
        await asyncio.gather(
            sim.spawn(_victim_job(res, log, "H", 5, 4)),
            sim.spawn(_victim_job(res, log, "W", 1, 1, preempt=False, delay=1)),
            sim.spawn(_victim_job(res, log, "P", 3, 1, delay=2)),
        )
        return log

    assert desyncio.run(main()) == [
        ("H", "start", 0.0),
        ("H", "done", 4.0),
        ("W", "start", 4.0),
        ("W", "done", 5.0),
        ("P", "start", 5.0),
        ("P", "done", 6.0),
    ]


def test_tie_among_worst_holders_picks_most_recent() -> None:
    async def main() -> list[tuple[str, str, float]]:
        res = PreemptiveResource(2)
        log: list[tuple[str, str, float]] = []
        await asyncio.gather(
            sim.spawn(_victim_job(res, log, "early", 5, 10)),
            sim.spawn(_victim_job(res, log, "late", 5, 10, delay=1)),
            sim.spawn(_victim_job(res, log, "urgent", 0, 1, delay=2)),
        )
        return log

    log = desyncio.run(main())
    assert ("late", "preempted", 2.0) in log
    assert ("early", "done", 10.0) in log
    assert ("early", "preempted", 2.0) not in log


def test_worst_priority_holder_is_chosen_not_the_latest() -> None:
    async def main() -> list[tuple[str, str, float]]:
        res = PreemptiveResource(2)
        log: list[tuple[str, str, float]] = []
        await asyncio.gather(
            sim.spawn(_victim_job(res, log, "worst", 7, 10)),
            sim.spawn(_victim_job(res, log, "mid", 3, 10, delay=1)),
            sim.spawn(_victim_job(res, log, "urgent", 0, 1, delay=2)),
        )
        return log

    log = desyncio.run(main())
    assert ("worst", "preempted", 2.0) in log
    assert ("mid", "done", 11.0) in log


def test_plain_async_with_is_preemptible_and_preempts() -> None:
    async def main() -> list[str]:
        res = PreemptiveResource(1)
        log: list[str] = []

        async def plain_holder() -> None:
            try:
                async with res:  # priority 0, preemptible
                    await sim.timeout(10)
            except Interrupt as exc:
                assert isinstance(exc.cause, Preempted)
                log.append(f"plain preempted at {sim.now}")

        async def better() -> None:
            await sim.timeout(2)
            async with res.request(priority=-1):
                log.append(f"better start at {sim.now}")
                await sim.timeout(1)

        async def low() -> None:
            await sim.timeout(5)
            try:
                async with res.request(priority=3):
                    await sim.timeout(10)
            except Interrupt:
                log.append(f"low preempted at {sim.now}")

        async def plain_preemptor() -> None:
            await sim.timeout(8)
            async with res:  # plain: priority 0, may preempt priority 3
                log.append(f"plain start at {sim.now}")
                await sim.timeout(1)

        await asyncio.gather(
            sim.spawn(plain_holder()),
            sim.spawn(better()),
            sim.spawn(low()),
            sim.spawn(plain_preemptor()),
        )
        assert (res.count, res.queue_length) == (0, 0)
        return log

    assert desyncio.run(main()) == [
        "better start at 2.0",  # the preemptor runs first, the victim unwinds after
        "plain preempted at 2.0",
        "plain start at 8.0",
        "low preempted at 8.0",
    ]


def test_victim_exit_does_not_release_twice() -> None:
    async def main() -> None:
        counts: list[tuple[int, int]] = []
        res = PreemptiveResource(
            1, monitor=lambda r: counts.append((r.count, r.queue_length))
        )
        log: list[tuple[str, str, float]] = []
        victim = sim.spawn(_victim_job(res, log, "victim", 5, 10))
        await sim.timeout(1)
        urgent = sim.spawn(_victim_job(res, log, "urgent", 0, 5))
        queued = sim.spawn(_victim_job(res, log, "queued", 9, 1))
        await sim.timeout(2)  # t=3: the victim has long since unwound
        assert (res.count, res.queue_length) == (1, 1)  # urgent holds, queued waits
        await asyncio.gather(victim, urgent, queued)
        assert [e for e in log if e[0] == "queued"] == [
            ("queued", "start", 6.0),
            ("queued", "done", 7.0),
        ]
        assert (res.count, res.queue_length) == (0, 0)
        assert all(0 <= c <= 1 for c, _ in counts)

    desyncio.run(main())


def test_monitor_called_once_after_preemption_settles() -> None:
    async def main() -> list[tuple[float, int, int]]:
        seen: list[tuple[float, int, int]] = []
        res = PreemptiveResource(
            1, monitor=lambda r: seen.append((sim.now, r.count, r.queue_length))
        )
        log: list[tuple[str, str, float]] = []
        v = sim.spawn(_victim_job(res, log, "v", 5, 10))
        await sim.timeout(2)
        p = sim.spawn(_victim_job(res, log, "p", 0, 1))
        await asyncio.gather(v, p)
        return seen

    # grant, preemption (swap: still one unit used), release
    assert desyncio.run(main()) == [(0.0, 1, 0), (2.0, 1, 0), (3.0, 0, 0)]


def test_raising_monitor_on_preemption_is_reported_and_preemption_stands() -> None:
    async def main() -> tuple[list[tuple[str, str, float]], int]:
        errors = _capture_loop_errors()

        def boom(r: PriorityResource) -> None:
            raise MonitorBoom

        res = PreemptiveResource(1, monitor=boom)
        log: list[tuple[str, str, float]] = []
        await asyncio.gather(
            sim.spawn(_victim_job(res, log, "v", 5, 10)),
            sim.spawn(_victim_job(res, log, "p", 0, 1, delay=2)),
        )
        assert (res.count, res.queue_length) == (0, 0)
        return log, len(errors)

    log, n_errors = desyncio.run(main())
    assert ("v", "preempted", 2.0) in log and ("p", "done", 3.0) in log
    assert n_errors > 0  # reported, not raised


def test_preempt_must_be_bool_and_priority_validated() -> None:
    res = PreemptiveResource(1)
    with pytest.raises(TypeError):
        res.request(preempt=1)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        res.request(priority=True)
    with pytest.raises(ValueError):
        res.request(priority=float("nan"))
    assert isinstance(res, PriorityResource)
    assert res.request().priority == 0


# Case a: the preemptor's own task already holds a unit ----------------------


def test_case_a_never_interrupts_itself_takes_next_worst_holder() -> None:
    async def main() -> list[str]:
        res = PreemptiveResource(2)
        log: list[str] = []

        async def other() -> None:
            try:
                async with res.request(priority=3):
                    await sim.timeout(10)
            except Interrupt as exc:
                assert isinstance(exc.cause, Preempted)
                log.append(f"other preempted at {sim.now}")

        async def both() -> None:
            async with res.request(priority=5):  # worst holder overall: ours
                await sim.timeout(1)
                async with res.request(priority=0):  # better request, same task
                    log.append(f"second unit at {sim.now}")
                    assert res.count == 2
                    await sim.timeout(1)
                log.append("inner out")

        o = sim.spawn(other())
        await sim.timeout(0)
        b = sim.spawn(both())
        await asyncio.gather(o, b)
        assert (res.count, res.queue_length) == (0, 0)
        return log

    assert desyncio.run(main()) == [
        "second unit at 1.0",  # the preemptor runs first, the victim unwinds after
        "other preempted at 1.0",
        "inner out",
    ]


def test_case_a_only_own_units_worse_waits_instead_of_self_interrupt() -> None:
    async def main() -> None:
        res = PreemptiveResource(1)
        outcome: list[str] = []

        async def selfish() -> None:
            async with res.request(priority=5):
                try:
                    async with res.request(priority=0):
                        outcome.append("got it")  # pragma: no cover
                except asyncio.CancelledError:
                    outcome.append("cancelled while waiting")
                    raise

        t = sim.spawn(selfish())
        await sim.timeout(5)
        # It waits for itself (like any nested acquire of a busy unit); it was
        # not interrupted and nothing was preempted.
        assert not t.done()
        assert (res.count, res.queue_length) == (1, 1)
        t.cancel()
        with pytest.raises(asyncio.CancelledError):
            await t
        assert outcome == ["cancelled while waiting"]
        assert (res.count, res.queue_length) == (0, 0)

    desyncio.run(main())


# Case b: the victim has its own interruptible block --------------------------


def test_case_b_nested_user_block_receives_preemption() -> None:
    async def main() -> list[str]:
        res = PreemptiveResource(1)
        log: list[str] = []

        async def victim() -> None:
            async with res.request(priority=5):
                try:
                    async with sim.interruptible():
                        await sim.timeout(10)
                except Interrupt as exc:
                    # Inside the async with: the unit is already gone.
                    assert isinstance(exc.cause, Preempted)
                    log.append(f"swallowed at {sim.now}, count={res.count}")
                await sim.timeout(5)  # keeps running WITHOUT the unit
                log.append(f"victim body ends at {sim.now}")
            log.append(f"victim out, count={res.count}")

        async def urgent() -> None:
            await sim.timeout(2)
            async with res.request(priority=0):
                log.append(f"urgent start at {sim.now}")
                await sim.timeout(10)  # holds past the victim's exit
            log.append("urgent out")

        await asyncio.gather(sim.spawn(victim()), sim.spawn(urgent()))
        assert (res.count, res.queue_length) == (0, 0)
        return log

    assert desyncio.run(main()) == [
        "urgent start at 2.0",
        "swallowed at 2.0, count=1",  # urgent's unit, not the victim's
        "victim body ends at 7.0",
        "victim out, count=1",  # the victim's exit did not release urgent's unit
        "urgent out",
    ]


def test_case_b_propagating_the_interrupt_is_the_recommended_pattern() -> None:
    async def main() -> list[str]:
        res = PreemptiveResource(1)
        log: list[str] = []

        async def victim() -> None:
            try:
                async with res.request(priority=5):
                    async with sim.interruptible():  # own block, not swallowed
                        await sim.timeout(10)
            except Interrupt as exc:
                assert isinstance(exc.cause, Preempted)
                log.append(f"victim saw Preempted at {sim.now}")

        async def urgent() -> None:
            await sim.timeout(2)
            async with res.request(priority=0):
                log.append("urgent start")
                await sim.timeout(1)

        await asyncio.gather(sim.spawn(victim()), sim.spawn(urgent()))
        assert (res.count, res.queue_length) == (0, 0)
        return log

    assert desyncio.run(main()) == ["urgent start", "victim saw Preempted at 2.0"]


# Case c: late preemption ------------------------------------------------------


def test_case_c_late_preemption_no_interrupt_no_double_release() -> None:
    # The victim's body swallows the cancellation and has no await left, so
    # INT-01's late-interrupt rule applies: the scope exits normally.
    async def main() -> list[str]:
        res = PreemptiveResource(1)
        log: list[str] = []

        async def victim() -> None:
            async with res.request(priority=5):
                try:
                    await sim.timeout(10)
                except asyncio.CancelledError:
                    log.append("body swallowed the cancel")
            log.append(f"victim exits normally, count={res.count}")

        async def urgent() -> None:
            await sim.timeout(2)
            async with res.request(priority=0):
                log.append("urgent start")
                await sim.timeout(5)
            log.append("urgent out")

        v = sim.spawn(victim())
        u = sim.spawn(urgent())
        await asyncio.gather(v, u)
        assert not v.cancelled()
        assert (res.count, res.queue_length) == (0, 0)
        return log

    assert desyncio.run(main()) == [
        "urgent start",
        "body swallowed the cancel",
        "victim exits normally, count=1",
        "urgent out",
    ]


# Case d: victim cancelled / failing in the instant it is preempted ------------


@pytest.mark.parametrize("cancel_first", [True, False])
def test_case_d_victim_cancelled_in_the_same_instant(cancel_first: bool) -> None:
    async def main() -> None:
        res = PreemptiveResource(1)
        log: list[str] = []

        async def victim() -> None:
            async with res.request(priority=5):
                await sim.timeout(10)

        async def urgent() -> None:
            async with res.request(priority=0):
                log.append(f"urgent start at {sim.now}")
                await sim.timeout(3)

        v = sim.spawn(victim())
        await sim.timeout(2)
        if cancel_first:
            v.cancel()
            u = sim.spawn(urgent())
        else:
            u = sim.spawn(urgent())
            await sim.timeout(0)  # urgent preempts first
            v.cancel()
        with pytest.raises(asyncio.CancelledError):
            await v  # a real cancel wins over the preemption interrupt (INT-01)
        await u
        assert (res.count, res.queue_length) == (0, 0)  # exactly one release each
        assert log == ["urgent start at 2.0"]
        assert sim.now == 5.0

    desyncio.run(main())


def test_case_d_victim_turns_its_cancel_into_another_exception() -> None:
    async def main() -> None:
        res = PreemptiveResource(1)
        seen: list[BaseException] = []

        async def victim() -> None:
            try:
                async with res.request(priority=5):
                    try:
                        await sim.timeout(10)
                    except asyncio.CancelledError:
                        raise ValueError("cleanup failed") from None
            except ValueError as exc:
                seen.append(exc)

        async def urgent() -> None:
            await sim.timeout(2)
            async with res.request(priority=0):
                await sim.timeout(3)

        await asyncio.gather(sim.spawn(victim()), sim.spawn(urgent()))
        assert len(seen) == 1  # the body's own exception, not an Interrupt
        assert (res.count, res.queue_length) == (0, 0)
        assert sim.now == 5.0

    desyncio.run(main())


# Case e: several preemptions in one instant -----------------------------------


def test_case_e_several_preemptions_in_one_instant() -> None:
    async def main() -> None:
        seen: list[int] = []
        res = PreemptiveResource(2, monitor=lambda r: seen.append(r.count))
        log: list[tuple[str, str, float]] = []
        causes: dict[str, Preempted] = {}

        async def victim(name: str, prio: float) -> None:
            try:
                async with res.request(priority=prio):
                    await sim.timeout(10)
            except Interrupt as exc:
                assert isinstance(exc.cause, Preempted)
                causes[name] = exc.cause
                log.append((name, "preempted", sim.now))

        v1 = sim.spawn(victim("a", 8))
        await sim.timeout(1)
        v2 = sim.spawn(victim("b", 6))
        await sim.timeout(1)
        # t=2: three requests in the same instant, capacity 2.
        p1 = sim.spawn(_victim_job(res, log, "p1", 4, 1))  # preempts a (worst)
        p2 = sim.spawn(_victim_job(res, log, "p2", 5, 1))  # b(6) is worst now
        p3 = sim.spawn(_victim_job(res, log, "p3", 9, 1))  # worse than all: waits
        await asyncio.gather(v1, v2, p1, p2, p3)
        assert [e for e in log if e[1] == "preempted"] == [
            ("a", "preempted", 2.0),
            ("b", "preempted", 2.0),
        ]
        assert causes["a"].by is p1 and causes["a"].usage_since == 0.0
        assert causes["b"].by is p2 and causes["b"].usage_since == 1.0
        assert [e for e in log if e[0] == "p3"] == [
            ("p3", "start", 3.0),
            ("p3", "done", 4.0),
        ]
        assert max(seen) <= 2 and (res.count, res.queue_length) == (0, 0)

    desyncio.run(main())


# Reservation: a unit handed to a waiter that has not resumed yet -------------


def test_preempting_a_waiter_that_was_granted_but_has_not_resumed() -> None:
    async def main() -> list[str]:
        res = PreemptiveResource(1)
        log: list[str] = []
        got: list[Preempted] = []

        async def holder() -> None:
            async with res.request(priority=1):
                await sim.timeout(5)

        async def waiter() -> None:
            await sim.timeout(1)
            try:
                async with res.request(priority=5):
                    log.append("waiter body")  # pragma: no cover
            except Interrupt as exc:
                assert isinstance(exc.cause, Preempted)
                got.append(exc.cause)
                log.append(f"waiter preempted at {sim.now}")

        async def urgent() -> None:
            await sim.timeout(5)  # same instant as holder's release, after it
            async with res.request(priority=0):
                log.append(f"urgent start at {sim.now}")
                await sim.timeout(1)

        h = sim.spawn(holder())
        w = sim.spawn(waiter())
        u = sim.spawn(urgent())
        await asyncio.gather(h, w, u)
        assert got[0].by is u and got[0].usage_since == 5.0
        assert (res.count, res.queue_length) == (0, 0)
        return log

    assert desyncio.run(main()) == [
        "urgent start at 5.0",
        "waiter preempted at 5.0",
    ]


# Stress and determinism -------------------------------------------------------


async def _preemption_stress(n: int = 40) -> list[tuple[float, str, str]]:
    rnd = sim.random("stress")
    capacity = rnd.randint(1, 3)
    trace: list[tuple[float, str, str]] = []

    def monitor(r: PriorityResource) -> None:
        assert 0 <= r.count <= r.capacity
        assert r.queue_length >= 0
        trace.append((sim.now, "mon", f"{r.count}/{r.queue_length}"))

    res = PreemptiveResource(capacity, monitor=monitor)

    async def job(i: int, delay: float, prio: int, work: float, pre: bool) -> None:
        await sim.timeout(delay)
        try:
            async with res.request(priority=prio, preempt=pre):
                trace.append((sim.now, f"j{i}", "start"))
                await sim.timeout(work)
            trace.append((sim.now, f"j{i}", "done"))
        except Interrupt as exc:
            assert isinstance(exc.cause, Preempted)
            assert exc.cause.usage_since <= sim.now
            trace.append((sim.now, f"j{i}", "preempted"))
        except asyncio.CancelledError:
            trace.append((sim.now, f"j{i}", "cancelled"))
            raise

    tasks = [
        sim.spawn(
            job(
                i,
                round(rnd.uniform(0, 20), 1),
                rnd.randint(0, 4),
                round(rnd.uniform(0.5, 6), 1),
                rnd.random() < 0.7,
            )
        )
        for i in range(n)
    ]
    for _ in range(n // 4):
        await sim.timeout(round(rnd.uniform(0, 4), 1))
        victim = rnd.choice(tasks)
        if not victim.done():
            victim.cancel()
    results = await asyncio.gather(*tasks, return_exceptions=True)
    assert all(r is None or isinstance(r, asyncio.CancelledError) for r in results)
    assert (res.count, res.queue_length) == (0, 0)  # nothing leaked
    ends = {name: what for _, name, what in trace if what in ("done", "preempted")}
    for i, r in enumerate(results):
        # Everyone finished, was preempted, or was cancelled (possibly before
        # it ever ran, which leaves no trace line).
        assert (f"j{i}" in ends) == (r is None)
    return trace


@pytest.mark.parametrize("seed", range(40))
def test_preemption_stress(seed: int) -> None:
    desyncio.run(_preemption_stress(), seed=seed)


def test_preemption_determinism() -> None:
    a = desyncio.run(_preemption_stress(), seed=11)
    b = desyncio.run(_preemption_stress(), seed=11)
    c = desyncio.run(_preemption_stress(), seed=12)
    assert a == b
    assert a != c


@pytest.mark.parametrize("method", ["__aenter__", "__aexit__"])
def test_interruptible_enter_and_exit_never_await(method: str) -> None:
    """Pin that ``Interruptible.__aenter__``/``__aexit__`` contain no suspension point.

    Why this test exists (D030, D034): ``PreemptiveRequest`` grants a unit and
    then enters an ``Interruptible`` scope; on exit it leaves the scope and
    releases the unit. Both steps are leak-free only because the scope's
    ``__aenter__`` and ``__aexit__`` run to completion without ever yielding to
    the event loop. If either one gained an ``await`` (or ``async for`` /
    ``async with``), a cancel or ``sim.interrupt`` could land at that
    suspension point, between the grant and the scope (or between the scope
    and the release), and a unit would be lost for good -- the same leak D030
    removed from RES-02. The methods are declared ``async`` only to satisfy
    the async context manager protocol, so this check inspects their source:
    a future ``await`` in either one fails here loudly instead of leaking
    silently under rare timing.
    """
    import ast
    import inspect
    import textwrap

    from desyncio.interrupt import Interruptible

    source = textwrap.dedent(inspect.getsource(getattr(Interruptible, method)))
    tree = ast.parse(source)
    suspending = [
        f"{type(node).__name__} at line {node.lineno}"
        for node in ast.walk(tree)
        if isinstance(node, (ast.Await, ast.AsyncFor, ast.AsyncWith))
    ]
    assert suspending == [], (
        f"Interruptible.{method} must never suspend (D030/D034), found: {suspending}"
    )
