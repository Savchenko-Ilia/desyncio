"""Tests for ``desyncio.scheduled`` (CAL-03). Times: hours * 3600, UTC calendar."""

import asyncio
import itertools
from collections.abc import Callable, Iterator
from datetime import UTC, datetime, time
from typing import Any
from zoneinfo import ZoneInfo

import pytest

import desyncio
from desyncio import Interrupt, sim
from desyncio.dates import Calendar
from desyncio.loop import SimulationDeadlock, VirtualTimeLoop
from desyncio.resource import PreemptiveResource, PriorityResource, Resource
from desyncio.schedule import Schedule, Shift
from desyncio.scheduled import ScheduledResource, ShiftEnded
from desyncio.trace import ListSink

H = 3600.0
ALL_DAYS = range(7)
MONDAY = datetime(2026, 1, 5, tzinfo=UTC)


def clinic(*shifts: Shift, start: datetime = MONDAY) -> Schedule:
    return Schedule(Calendar(start), shifts)


def day_shifts() -> Schedule:
    """2 servers 09-17, 1 server 17-22, nobody overnight."""
    return clinic(
        Shift(ALL_DAYS, time(9), time(17), 2), Shift(ALL_DAYS, time(17), time(22), 1)
    )


def test_outside_a_run_raises() -> None:
    with pytest.raises(RuntimeError):
        ScheduledResource(day_shifts())


@pytest.mark.parametrize(
    "make",
    [
        Resource,
        PriorityResource,
        PreemptiveResource,
        lambda **kw: ScheduledResource(day_shifts(), **kw),
    ],
    ids=["Resource", "PriorityResource", "PreemptiveResource", "ScheduledResource"],
)
def test_resource_name(make: Callable[..., Any]) -> None:
    async def main() -> None:
        assert make().name is None
        assert make(name="desk").name == "desk"
        with pytest.raises(TypeError, match="name"):
            make(name=3)
        with pytest.raises(ValueError, match="name"):
            make(name="")

    desyncio.run(main())


def test_a_day_of_customers() -> None:
    caps: list[tuple[float, int]] = []
    waits: dict[str, float] = {}
    cut: list[str] = []

    def watch(r: ScheduledResource) -> None:
        if not caps or caps[-1][1] != r.capacity:
            caps.append((sim.now, r.capacity))

    async def main() -> None:
        res = ScheduledResource(day_shifts(), monitor=watch)

        async def customer(name: str, arrive: float, work: float) -> None:
            await sim.timeout(arrive * H)
            t0 = sim.now
            try:
                async with res:
                    waits[name] = sim.now - t0
                    await sim.timeout(work * H)
            except Interrupt:
                cut.append(name)

        arrivals = [  # (name, arrival hour, hours of work)
            ("a", 8, 1),  # waits for the 09:00 opening
            ("b", 10.5, 2),
            ("c", 11, 3),
            ("d", 11.5, 1),  # both servers busy: waits until b is done at 12:30
            ("f", 21, 2),  # one server left, the 22:00 close cuts the visit
            ("e", 23, 2),  # closed: waits until 09:00 next day
        ]
        await asyncio.gather(*(sim.spawn(customer(*c)) for c in arrivals))

    desyncio.run(main())
    assert caps == [(8 * H, 0), (9 * H, 2), (17 * H, 1), (22 * H, 0), (33 * H, 2)]
    assert waits == {"a": H, "b": 0, "c": 0, "d": H, "f": 0, "e": 10 * H}
    assert cut == ["f"]


def test_shift_end_interrupts_most_recent_holder() -> None:
    sched = clinic(
        Shift(ALL_DAYS, time(0), time(5), 2), Shift(ALL_DAYS, time(5), time(10), 1)
    )
    seen: dict[str, tuple[float, object]] = {}

    async def main() -> None:
        res = ScheduledResource(sched)

        async def holder(name: str, delay: float) -> None:
            await sim.timeout(delay)
            try:
                async with res:
                    await sim.timeout(100 * H)
            except Interrupt as exc:
                seen[name] = (sim.now, exc.cause)

        a = sim.spawn(holder("a", 1 * H))
        b = sim.spawn(holder("b", 2 * H))
        await sim.timeout(5 * H + 1)  # the drop from 2 to 1 units was at 05:00
        assert res.capacity == 1 and res.count == 1  # b's unit is already released
        # b was granted last, so b lost its unit; a is unaffected and still holds one
        assert seen == {"b": (5 * H, ShiftEnded(at=5 * H, resource=res))}
        assert not a.done()
        a.cancel()
        await asyncio.gather(a, b, return_exceptions=True)
        assert res.count == 0  # a's unit came back on cancel; b's was gone already

    desyncio.run(main())


def test_reservation_taken_back_at_same_instant() -> None:
    """Capacity 2 until 02:00, 1 until 05:00, then 0."""
    sched = clinic(
        Shift(ALL_DAYS, time(0), time(2), 2), Shift(ALL_DAYS, time(2), time(5), 1)
    )
    log: list[tuple[object, ...]] = []

    async def main() -> None:
        res = ScheduledResource(sched)

        async def user(name: str, delay: float, work: float) -> None:
            await sim.timeout(delay)
            try:
                async with res:
                    log.append((name, "body", sim.now))
                    await sim.timeout(work)
            except Interrupt as exc:
                log.append((name, "interrupted", sim.now, exc.cause))

        # a and c hold from the start; the drop at 02:00 interrupts c, the later
        # grant. b waits from 03:00. a's release at 05:00 runs before the drop
        # (D054: the driver's timer has priority 1) and hands b a reservation;
        # the drop to 0 then takes it back before b resumes, so b's body never
        # runs.
        users = [user("a", 0, 5 * H), user("c", 1, 99 * H), user("b", 3 * H, H)]
        await asyncio.gather(*(sim.spawn(u) for u in users))
        assert res.count == 0 and res.queue_length == 0

    desyncio.run(main())
    assert [e[:3] for e in log] == [
        ("a", "body", 0.0),
        ("c", "body", 1.0),
        ("c", "interrupted", 2 * H),
        ("b", "interrupted", 5 * H),
    ]
    assert all(isinstance(e[3], ShiftEnded) for e in log if len(e) == 4)


def test_fifo_when_capacity_rises() -> None:
    order: list[tuple[str, float]] = []

    async def main() -> None:
        res = ScheduledResource(clinic(Shift(ALL_DAYS, time(9), time(17), 2)))
        assert res.capacity == 0  # capacity 0: requests wait

        async def customer(name: str, arrive: float) -> None:
            await sim.timeout(arrive)
            async with res:
                order.append((name, sim.now))
                await sim.timeout(H)

        tasks = [sim.spawn(customer(n, i * 60.0)) for i, n in enumerate("abc")]
        await sim.timeout(1)
        assert res.queue_length == 1 and res.count == 0  # only "a" has arrived
        await asyncio.gather(*tasks)

    desyncio.run(main())
    # both units at 09:00 go to a and b in arrival order; c waits for a release
    assert order == [("a", 9 * H), ("b", 9 * H), ("c", 10 * H)]


def test_cancelled_waiter_takes_no_unit() -> None:
    async def main() -> None:
        res = ScheduledResource(clinic(Shift(ALL_DAYS, time(9), time(17), 1)))

        async def wait() -> None:
            async with res:
                await sim.timeout(1)

        w = sim.spawn(wait())
        await sim.timeout(1)
        assert res.queue_length == 1
        w.cancel()
        await asyncio.gather(w, return_exceptions=True)
        assert res.queue_length == 0
        await sim.timeout(10 * H)  # past the 09:00 opening
        assert res.count == 0 and res.capacity == 1

    desyncio.run(main())


def test_driver_does_not_keep_run_alive_or_fake_a_deadlock() -> None:
    async def main() -> float:
        res = ScheduledResource(day_shifts())
        # Only the driver has a timer now; main waits on a request. That is not
        # a deadlock, and run() returns although the changes never end.
        async with res:
            return sim.now

    assert desyncio.run(main()) == 9 * H

    async def stuck() -> None:
        ScheduledResource(clinic(Shift(ALL_DAYS, time(0), time(0), 1)))  # constant
        await asyncio.Event().wait()  # nothing will ever set this

    with pytest.raises(SimulationDeadlock):  # the driver finished: no timer left
        desyncio.run(stuck())


def test_driver_task_ends_when_changes_end() -> None:
    async def main() -> bool:
        res = ScheduledResource(clinic(Shift(ALL_DAYS, time(0), time(0), 3)))
        await sim.timeout(500 * 24 * H)
        left = [t for t in asyncio.all_tasks() if "driver" in t.get_name()]
        return res.capacity == 3 and not left  # capacity stays, driver is done

    assert desyncio.run(main())


def test_monitor_on_capacity_change_and_errors_reported() -> None:
    seen: list[tuple[float, int, int]] = []
    errors: list[str] = []

    async def main() -> None:
        loop = asyncio.get_running_loop()
        loop.set_exception_handler(lambda _l, ctx: errors.append(ctx["message"]))

        def mon(r: ScheduledResource) -> None:
            seen.append((sim.now, r.capacity, r.count))
            raise ValueError("boom")

        ScheduledResource(clinic(Shift(ALL_DAYS, time(0), time(1), 1)), monitor=mon)
        await sim.timeout(2 * H)

    desyncio.run(main())
    assert seen == [(1 * H, 0, 0)]  # called on the capacity change
    assert len(errors) == 1 and "monitor" in errors[0]


def test_berlin_dst_day() -> None:
    """06:00 local on 29 March 2026 is 5 elapsed hours after 00:00 (spring forward)."""
    cal = Calendar(datetime(2026, 3, 28, tzinfo=ZoneInfo("Europe/Berlin")))
    sched = Schedule(cal, [Shift(ALL_DAYS, time(0), time(6), 1)])
    ends: list[float] = []

    async def main() -> None:
        res = ScheduledResource(sched)

        async def worker() -> None:
            await sim.timeout(24 * H + 60)  # 29 March, 00:01 local
            try:
                async with res:
                    await sim.timeout(100 * H)
            except Interrupt as exc:
                assert isinstance(exc.cause, ShiftEnded)
                ends.append(exc.cause.at)

        await sim.spawn(worker())

    desyncio.run(main())
    assert ends == [24 * H + 5 * H]


def test_same_seed_same_trace() -> None:
    async def main() -> None:
        res = ScheduledResource(day_shifts())

        async def customer() -> None:
            rng = sim.random("c")
            await sim.timeout(rng.uniform(0, 20 * H))
            try:
                async with res:
                    await sim.timeout(rng.uniform(0, 4 * H))
            except Interrupt:
                pass

        await asyncio.gather(*(sim.spawn(customer()) for _ in range(30)))

    def run(seed: int) -> list[object]:
        sink = ListSink()
        desyncio.run(main(), seed=seed, trace=sink)
        return list(sink.records)

    a = run(7)
    assert len(a) > 100 and a == run(7) and a != run(8)


def constant(capacity: int) -> Schedule:
    return clinic(Shift(ALL_DAYS, time(0), time(0), capacity))


def test_waiter_cancelled_after_grant_passes_the_unit_on() -> None:
    got: list[str] = []

    async def main() -> None:
        res = ScheduledResource(constant(1))

        async def wait(name: str) -> None:
            async with res:
                got.append(name)

        async with res:
            w1, w2 = sim.spawn(wait("w1")), sim.spawn(wait("w2"))
            await sim.timeout(10)
        # The release granted w1 its unit; w1 has not resumed yet.
        assert res.count == 0 and res.queue_length == 2
        w1.cancel()
        await asyncio.gather(w1, w2, return_exceptions=True)
        assert w1.cancelled() and got == ["w2"]
        assert res.count == 0 and res.queue_length == 0

    desyncio.run(main())


def test_real_cancel_beats_shift_end_at_same_instant() -> None:
    async def main() -> None:
        res = ScheduledResource(clinic(Shift(ALL_DAYS, time(0), time(1), 1)))

        async def hold() -> None:
            async with res:
                await sim.timeout(100 * H)

        victim = sim.spawn(hold())
        await sim.timeout(H)  # priority 0: runs before the 01:00 drop
        victim.cancel()  # then the drop interrupts it in the same instant
        await asyncio.gather(victim, return_exceptions=True)
        assert victim.cancelled() and res.count == 0 and res.capacity == 0

    desyncio.run(main())


def test_taken_back_reservation_then_cancelled() -> None:
    async def main() -> None:
        loop = asyncio.get_running_loop()
        assert isinstance(loop, VirtualTimeLoop)
        res = ScheduledResource(clinic(Shift(ALL_DAYS, time(0), time(1), 1)))

        async def use(dt: float) -> None:
            async with res:
                await sim.timeout(dt)

        holder, w = sim.spawn(use(H)), sim.spawn(use(1))
        # Wake after the driver's 01:00 step (priority 2 > 1), before w resumes.
        after_drop: asyncio.Future[None] = loop.create_future()
        loop.call_at_priority(H, 2, after_drop.set_result, None)
        await after_drop
        assert res.capacity == 0 and res.queue_length == 0  # reservation gone
        w.cancel()
        results = await asyncio.gather(holder, w, return_exceptions=True)
        assert results[0] is None and w.cancelled()
        assert res.count == 0 and res.queue_length == 0

    desyncio.run(main())


@pytest.mark.parametrize("driver_first", [False, True])
def test_visit_ending_at_a_shift_end_completes(driver_first: bool) -> None:
    async def main() -> None:
        res = ScheduledResource(clinic(Shift(ALL_DAYS, time(0), time(2), 1)))
        if driver_first:
            await sim.timeout(0)  # the driver arms its 02:00 timer first
        async with res:  # else: our 02:00 timer is armed before the driver's
            await sim.timeout(2 * H)
        assert sim.now == 2 * H and res.count == 0  # released, not interrupted
        await sim.timeout(0)
        assert res.capacity == 0

    desyncio.run(main())


def test_arrival_at_a_capacity_rise_queues_then_is_granted() -> None:
    seen: list[tuple[float, int, int, int]] = []

    def watch(r: ScheduledResource) -> None:
        seen.append((sim.now, r.capacity, r.count, r.queue_length))

    async def main() -> None:
        res = ScheduledResource(
            clinic(Shift(ALL_DAYS, time(9), time(17), 1)), monitor=watch
        )
        await sim.timeout(0)  # the driver arms its 09:00 timer first
        await sim.timeout(9 * H)
        async with res:
            assert sim.now == 9 * H

    desyncio.run(main(), until=10 * H)
    # queued at 09:00 before the rise, then granted by it
    assert seen[:3] == [(9 * H, 0, 0, 1), (9 * H, 1, 0, 1), (9 * H, 1, 1, 0)]


class _Broken(Schedule):
    def changes(self, after: float) -> Iterator[tuple[float, int]]:
        yield from itertools.islice(super().changes(after), 1)
        raise ValueError("bad schedule")


def test_driver_failure_is_reported_at_once() -> None:
    errors: list[tuple[float, str, object]] = []

    async def main() -> None:
        loop = asyncio.get_running_loop()
        loop.set_exception_handler(
            lambda _l, ctx: errors.append((sim.now, ctx["message"], ctx["exception"]))
        )
        res = ScheduledResource(
            _Broken(Calendar(MONDAY), [Shift(ALL_DAYS, time(9), time(17), 1)])
        )
        await sim.timeout(10 * H)
        assert res.capacity == 1

    desyncio.run(main())
    assert [(t, m) for t, m, _ in errors] == [
        (9 * H, "ScheduledResource driver failed")
    ]
    assert isinstance(errors[0][2], ValueError)


def test_failed_interrupt_leaves_state_applied(monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(task: object, cause: object) -> None:
        raise RuntimeError("cannot interrupt")

    monkeypatch.setattr("desyncio.scheduled.interrupt_task", boom)
    errors: list[str] = []

    async def main() -> None:
        loop = asyncio.get_running_loop()
        loop.set_exception_handler(lambda _l, ctx: errors.append(ctx["message"]))
        res = ScheduledResource(clinic(Shift(ALL_DAYS, time(0), time(1), 2)))

        async def hold() -> None:
            async with res:
                await sim.timeout(H + 1)

        tasks = [sim.spawn(hold()), sim.spawn(hold())]
        await sim.timeout(H + 0.5)
        assert res.capacity == 0 and res.count == 0  # both units taken at 01:00
        await asyncio.gather(*tasks)  # not interrupted: they end, no 2nd release
        assert res.count == 0 and res.queue_length == 0

    desyncio.run(main())
    assert errors == ["ScheduledResource driver failed"]
