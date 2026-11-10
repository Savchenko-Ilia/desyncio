"""``ResourceChanged`` emission from the four resource classes (EVT-04, D060)."""

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime, time
from typing import Any

import pytest

import desyncio
from desyncio import Interrupt, sim
from desyncio.dates import Calendar
from desyncio.events import Event, ResourceChanged, RunFinished
from desyncio.resource import PreemptiveResource, PriorityResource, Resource
from desyncio.schedule import Schedule, Shift
from desyncio.scheduled import ScheduledResource

H = 3600.0
ALL_DAYS = range(7)
MONDAY = datetime(2026, 1, 5, tzinfo=UTC)


def one_unit(**kw: Any) -> ScheduledResource:
    """Capacity 1 from 00:00 to 12:00: constant over the short runs below."""
    schedule = Schedule(Calendar(MONDAY), [Shift(ALL_DAYS, time(0), time(12), 1)])
    return ScheduledResource(schedule, **kw)


ALL = pytest.mark.parametrize(
    "make",
    [Resource, PriorityResource, PreemptiveResource, one_unit],
    ids=["Resource", "PriorityResource", "PreemptiveResource", "ScheduledResource"],
)
# ScheduledResource needs a run to be built, so it can't be used on a plain
# asyncio loop; its reuse in a later run has its own tests below.
REUSABLE = pytest.mark.parametrize(
    "make",
    [Resource, PriorityResource, PreemptiveResource],
    ids=["Resource", "PriorityResource", "PreemptiveResource"],
)

State = tuple[float, int, int, int]


def state(r: Any) -> State:
    return (sim.now, r.count, r.capacity, r.queue_length)


def fields(e: ResourceChanged) -> State:
    return (e.t, e.in_use, e.capacity, e.queue_len)


async def hold(res: Any, until: float) -> None:
    async with res:
        await sim.timeout(until - sim.now)


@ALL
def test_one_event_per_change_after_the_monitor(make: Callable[..., Any]) -> None:
    # Grant, two queued, a waiter cancelled, release with hand-over, release.
    log: list[tuple[str, State]] = []

    def on_event(e: ResourceChanged) -> None:
        assert e.resource == "desk"
        log.append(("event", fields(e)))

    async def main() -> None:
        res = make(name="desk", monitor=lambda r: log.append(("monitor", state(r))))
        a = sim.spawn(hold(res, 2))
        b = sim.spawn(hold(res, 3))
        c = sim.spawn(hold(res, 9))
        await sim.timeout(1)
        c.cancel()
        await asyncio.gather(a, b, c, return_exceptions=True)

    desyncio.run(main(), subscribers=[(on_event, (ResourceChanged,))])
    expected = [
        (0, 1, 1, 0),  # a granted (fast path)
        (0, 1, 1, 1),  # b queued
        (0, 1, 1, 2),  # c queued
        (1, 1, 1, 1),  # c cancelled while queued
        (2, 0, 1, 1),  # a released; b granted, not resumed yet
        (2, 1, 1, 0),  # b resumed
        (3, 0, 1, 0),  # b released
    ]
    assert log == [(who, s) for s in expected for who in ("monitor", "event")]


def test_preemption_swap_is_one_event() -> None:
    events: list[ResourceChanged] = []

    async def low(res: PreemptiveResource) -> None:
        with pytest.raises(Interrupt):
            async with res.request(priority=5):
                await sim.timeout(10)

    async def high(res: PreemptiveResource) -> None:
        await sim.timeout(1)
        async with res.request(priority=0):
            await sim.timeout(1)

    async def main() -> None:
        res = PreemptiveResource()
        await asyncio.gather(low(res), high(res))

    desyncio.run(main(), subscribers=[(events.append, (ResourceChanged,))])
    assert [fields(e) for e in events] == [(0, 1, 1, 0), (1, 1, 1, 0), (2, 0, 1, 0)]


def test_shift_capacity_change_is_one_event() -> None:
    # 2 units 00-01, 1 unit 01-02, closed after: two holders until 02:00.
    events: list[ResourceChanged] = []
    schedule = Schedule(
        Calendar(MONDAY),
        [Shift(ALL_DAYS, time(0), time(1), 2), Shift(ALL_DAYS, time(1), time(2), 1)],
    )

    async def holder(res: ScheduledResource) -> None:
        try:
            await hold(res, 2 * H)
        except Interrupt:
            pass

    async def main() -> None:
        res = ScheduledResource(schedule)
        await asyncio.gather(holder(res), holder(res))
        await sim.timeout(1)  # let the 02:00 change apply

    desyncio.run(main(), subscribers=[(events.append, (ResourceChanged,))])
    assert [fields(e) for e in events] == [
        (0, 1, 2, 0),
        (0, 2, 2, 0),
        (H, 1, 1, 0),  # the drop takes the newest unit: one event
        (2 * H, 0, 1, 0),  # the release at 02:00 runs before the shift (D054)
        (2 * H, 0, 0, 0),
    ]


def test_names_explicit_and_automatic() -> None:
    events: list[ResourceChanged] = []

    async def main() -> None:
        first, second, named = (
            Resource(),
            PriorityResource(),
            PreemptiveResource(name="x"),
        )
        for res in (named, second, first):
            async with res:
                pass
        async with one_unit():
            pass
        async with second:
            pass

    desyncio.run(main(), subscribers=[(events.append, (ResourceChanged,))])
    # Numbered in order of first use; ScheduledResource registers when built.
    assert [e.resource for e in events][::2] == [
        "x",
        "resource-1",
        "resource-2",
        "resource-3",
        "resource-1",
    ]


@REUSABLE
def test_duplicate_name_raises_before_any_change(make: Callable[..., Any]) -> None:
    events: list[ResourceChanged] = []
    calls: list[object] = []

    async def main() -> None:
        async with Resource(name="desk"):
            pass
        dup = make(name="desk", monitor=calls.append)
        with pytest.raises(ValueError, match="desk"):
            async with dup:
                pass
        assert (dup.count, dup.queue_length) == (0, 0)

    desyncio.run(main(), subscribers=[(events.append, (ResourceChanged,))])
    assert calls == []
    assert [e.resource for e in events] == ["desk", "desk"]


def test_scheduled_resource_duplicate_name_raises_when_built() -> None:
    # It registers in its constructor, before its driver task exists.
    events: list[ResourceChanged] = []
    calls: list[object] = []

    async def main() -> None:
        async with Resource(name="desk"):
            pass
        tasks = asyncio.all_tasks()
        with pytest.raises(ValueError, match="desk"):
            one_unit(name="desk", monitor=calls.append)
        assert asyncio.all_tasks() == tasks

    desyncio.run(main(), subscribers=[(events.append, (ResourceChanged,))])
    assert calls == []
    assert [e.resource for e in events] == ["desk", "desk"]


def test_unacquired_scheduled_resource_is_named_when_built() -> None:
    # The driver changes capacity at 01:00 with no acquire ever made, so the
    # name must come from construction-time registration.
    events: list[ResourceChanged] = []
    schedule = Schedule(
        Calendar(MONDAY),
        [Shift(ALL_DAYS, time(0), time(1), 1), Shift(ALL_DAYS, time(1), time(2), 2)],
    )

    async def main() -> None:
        ScheduledResource(schedule)
        await sim.timeout(1.5 * H)

    desyncio.run(main(), subscribers=[(events.append, (ResourceChanged,))])
    assert [(e.resource, *fields(e)) for e in events] == [("resource-1", H, 0, 2, 0)]


@REUSABLE
def test_reuse_across_runs_registers_in_each_run(make: Callable[..., Any]) -> None:
    res, named = make(), make(name="desk")
    names: list[str] = []

    def on_event(e: ResourceChanged) -> None:
        names.append(e.resource)

    async def run_one(first: Any) -> None:
        for r in (first, res, named):
            async with r:
                pass

    desyncio.run(run_one(Resource()), subscribers=[(on_event, (ResourceChanged,))])
    desyncio.run(run_one(res), subscribers=[(on_event, (ResourceChanged,))])
    assert names[::2] == [
        "resource-1",
        "resource-2",
        "desk",
        *["resource-1"] * 2,
        "desk",
    ]


def test_scheduled_resource_reused_in_a_later_run_registers_again() -> None:
    # Built in run A, so it was resource-1 there; in run B it must register
    # on first use and get a name of its own.
    built: list[ScheduledResource] = []
    names: list[str] = []

    async def run_a() -> None:
        built.append(one_unit())

    async def run_b() -> None:
        async with Resource():
            pass
        async with built[0]:
            pass

    desyncio.run(run_a())
    desyncio.run(
        run_b(), subscribers=[(lambda e: names.append(e.resource), (ResourceChanged,))]
    )
    assert names == ["resource-1", "resource-1", "resource-2", "resource-2"]


def test_scheduled_resource_duplicate_name_in_a_later_run_raises() -> None:
    built: list[ScheduledResource] = []
    calls: list[object] = []
    events: list[ResourceChanged] = []

    async def run_a() -> None:
        built.append(one_unit(name="desk", monitor=calls.append))

    async def run_b() -> None:
        (res,) = built
        async with Resource(name="desk"):
            pass
        with pytest.raises(ValueError, match="desk"):
            async with res:
                pass
        assert (res.count, res.queue_length) == (0, 0)

    desyncio.run(run_a())
    desyncio.run(run_b(), subscribers=[(events.append, (ResourceChanged,))])
    assert calls == []
    assert [e.resource for e in events] == ["desk", "desk"]


@REUSABLE
def test_plain_asyncio_loop_works(make: Callable[..., Any]) -> None:
    seen: list[tuple[int, int]] = []

    async def main() -> None:
        res = make(monitor=lambda r: seen.append((r.count, r.queue_length)))

        async def user() -> None:
            async with res:
                await asyncio.sleep(0)

        await asyncio.gather(user(), user())
        assert res.name is None

    asyncio.run(main())
    assert seen == [(1, 0), (1, 1), (0, 1), (1, 0), (0, 0)]


@ALL
def test_nothing_after_run_finished(make: Callable[..., Any]) -> None:
    events: list[Event] = []

    async def main() -> None:
        res = make()
        sim.spawn(hold(res, 100))
        sim.spawn(hold(res, 100))
        await sim.timeout(100)

    desyncio.run(main(), until=5, subscribers=[events.append])
    assert isinstance(events[-1], RunFinished) and events[-1].reason == "until"
    assert [fields(e) for e in events if isinstance(e, ResourceChanged)] == [
        (0, 1, 1, 0),
        (0, 1, 1, 1),
    ]


class Boom(Exception):
    pass


@ALL
def test_raising_handler_on_grant_leaks_nothing(make: Callable[..., Any]) -> None:
    resources: list[Any] = []

    def on_event(e: ResourceChanged) -> None:
        if e.t == 1 and e.in_use == 1:  # b resumes with the unit a released
            raise Boom

    async def main() -> None:
        res = make()
        resources.append(res)
        sim.spawn(hold(res, 1))
        await sim.timeout(0)  # a takes the unit first
        await hold(res, 5)

    with pytest.raises(Boom):
        desyncio.run(main(), subscribers=[(on_event, (ResourceChanged,))])
    # The grant completed; teardown cancelled b inside its body, which released.
    (res,) = resources
    assert (res.count, res.queue_length) == (0, 0)
