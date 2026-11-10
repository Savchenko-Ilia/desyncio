"""Tests for the event bus (EVT-02): subscribe, emit, run lifecycle, errors.

Plain sync tests: ``desyncio.run`` makes its own loop.
"""

import asyncio
import gc
import logging
import sys
import types
import warnings
from dataclasses import dataclass
from typing import Any

import pytest

from desyncio import bus as bus_module
from desyncio import events as events_module
from desyncio.bus import emit_event, register_resource_name
from desyncio.events import Event, RunFinished, RunStarted, Subscription
from desyncio.loop import VirtualTimeLoop
from desyncio.resource import Resource
from desyncio.runners import run
from desyncio.sim import sim
from desyncio.trace import TraceRecord


@dataclass(frozen=True, slots=True, kw_only=True)
class Ping(Event):
    n: int


@dataclass(frozen=True, slots=True, kw_only=True)
class Pong(Ping):
    pass


@dataclass(frozen=True, slots=True, kw_only=True)
class Other(Event):
    pass


BUILT: list[int] = []


@dataclass(frozen=True, slots=True, kw_only=True)
class Counting(Event):
    """Counts its constructions, to prove an event is not built."""

    def __post_init__(self) -> None:
        BUILT.append(self.seq)


class Boom(Exception):
    pass


def _kinds(events: list[Event]) -> list[str]:
    return [type(e).__name__ for e in events]


def test_subscription_is_exported_from_events() -> None:
    assert Subscription is bus_module.Subscription
    assert "Subscription" in events_module.__all__


@pytest.mark.parametrize(
    ("until", "fail", "reason", "end"),
    [
        (None, False, "main_returned", 2.0),
        (1.5, False, "until", 1.5),
        (None, True, "error", 2.0),
    ],
)
def test_run_lifecycle(
    until: float | None, fail: bool, reason: str, end: float
) -> None:
    """RunStarted first (before main's first step), RunFinished last, nothing
    from teardown; the same list in two runs."""

    def once() -> list[Any]:
        log: list[Any] = []
        teardown: list[bool] = []

        async def leftover() -> None:
            try:
                await asyncio.sleep(100)
            finally:
                sim.emit(Ping, n=99)  # teardown: the bus is closed, no-op
                # ... and subscribe returns a closed subscription. An error
                # here would only be logged by teardown, so record the result.
                teardown.append(sim.subscribe(log.append).closed)

        async def main() -> None:
            log.append("main-step")
            sim.spawn(leftover())
            await asyncio.sleep(1)
            sim.emit(Ping, n=1)
            await asyncio.sleep(1)
            if fail:
                raise Boom
            sim.emit(Pong, n=2)

        def record(e: Event) -> None:
            log.append(e.to_dict())

        if fail:
            with pytest.raises(Boom):
                run(main(), until=until, seed=7, subscribers=[record])
        else:
            assert run(main(), until=until, seed=7, subscribers=[record]) is None
        assert teardown == [True]
        return log

    log = once()
    assert log == once()
    started = {"t": 0.0, "seq": 0, "seed": 7, "until": until, "kind": "RunStarted"}
    assert log[:3] == [
        started,
        "main-step",
        {"t": 1.0, "seq": 1, "n": 1, "kind": "Ping"},
    ]
    assert log[-1] == {
        "t": end,
        "seq": len(log) - 2,
        "reason": reason,
        "kind": "RunFinished",
    }
    assert len(log) == (4 if until or fail else 5)


def test_kinds_filter_and_delivery_order() -> None:
    calls: list[tuple[str, str, int]] = []

    def handler(name: str) -> Any:
        return lambda e: calls.append((name, type(e).__name__, e.seq))

    async def main() -> None:
        sim.subscribe(handler("ping"), kinds=(Ping,))
        sim.subscribe(handler("all"))
        sim.subscribe(handler("other"), kinds=(Other, RunFinished))
        sim.emit(Ping, n=1)
        sim.emit(Pong, n=2)  # a Pong is a Ping (isinstance)
        sim.emit(Other)

    run(main())
    assert calls == [
        ("ping", "Ping", 1),
        ("all", "Ping", 1),
        ("ping", "Pong", 2),
        ("all", "Pong", 2),
        ("all", "Other", 3),
        ("other", "Other", 3),
        ("all", "RunFinished", 4),
        ("other", "RunFinished", 4),
    ]


def test_subscribe_and_close_during_dispatch_take_effect_next_event() -> None:
    calls: list[tuple[str, int]] = []
    subs: dict[str, Subscription] = {}

    def first(e: Event) -> None:
        calls.append(("first", e.seq))
        if e.seq == 1:
            subs["second"].close()
            subs["late"] = sim.subscribe(lambda e: calls.append(("late", e.seq)))

    async def main() -> None:
        subs["first"] = sim.subscribe(first, kinds=(Ping,))
        subs["second"] = sim.subscribe(lambda e: calls.append(("second", e.seq)))
        sim.emit(Ping, n=1)  # snapshot: second still gets it, late does not
        sim.emit(Ping, n=2)
        with subs["late"] as late:
            assert late.handler is not None and late.kinds is None
        assert late.closed
        late.close()  # idempotent
        subs["first"].close()
        sim.emit(Ping, n=3)

    run(main())
    assert calls == [("first", 1), ("second", 1), ("first", 2), ("late", 2)]
    assert all(s.closed for s in subs.values())  # the bus closed at the end


async def _async_handler(e: Event) -> None:
    pass


BAD_SUBSCRIPTIONS: list[tuple[Any, Any, type[Exception]]] = [
    (42, None, TypeError),
    (_async_handler, None, TypeError),
    (print, Ping, TypeError),  # one class, not a tuple
    (print, [Ping], TypeError),
    (print, (int,), TypeError),
    (print, (), ValueError),
]


def test_subscribe_validation(monkeypatch: pytest.MonkeyPatch) -> None:
    async def main() -> None:
        for handler, kinds, exc in BAD_SUBSCRIPTIONS:
            with pytest.raises(exc):
                sim.subscribe(handler, kinds=kinds)

    run(main())
    # The same checks for run(subscribers=...), before anything runs: main is
    # closed and no loop is built. The "coroutine was never awaited" and
    # "unclosed event loop" warnings come from __del__, so they go to
    # sys.unraisablehook, not to this frame; record them there.
    unraisable: list[str] = []
    monkeypatch.setattr(
        sys, "unraisablehook", lambda u: unraisable.append(repr(u.exc_value))
    )
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        for handler, kinds, exc in BAD_SUBSCRIPTIONS:
            coro = main()
            with pytest.raises(exc):
                run(coro, subscribers=[(handler, kinds)])
            assert isinstance(coro, types.CoroutineType)
            assert coro.cr_frame is None  # closed by run
            del coro
        gc.collect()
    assert unraisable == []


@pytest.mark.parametrize("subscribers", [5, [(print,)], [(print, None, None)]])
def test_run_subscribers_shape(subscribers: Any) -> None:
    async def main() -> None:
        pass

    with pytest.raises(TypeError):
        run(main(), subscribers=subscribers)


def test_emit_validation() -> None:
    async def main() -> None:
        with pytest.raises(TypeError):
            sim.emit(int)  # type: ignore[arg-type]
        sim.emit(Ping, bogus=1)  # not built (nobody listens), so not checked
        sim.subscribe(print, kinds=(Ping,))
        with pytest.raises(TypeError):
            sim.emit(Ping, bogus=1)

    run(main())
    with pytest.raises(RuntimeError):
        sim.emit(Ping, n=1)
    with pytest.raises(RuntimeError):
        sim.subscribe(print)


def test_zero_cost_when_off_and_seq_ticks_anyway() -> None:
    BUILT.clear()
    seqs: list[int] = []

    async def main() -> None:
        sim.subscribe(lambda e: seqs.append(e.seq), kinds=(Other,))
        for _ in range(3):
            sim.emit(Counting)  # nobody wants it: never constructed
        sim.emit(Other)
        sim.subscribe(print, kinds=(Counting,))  # invalidates the cache
        sim.emit(Counting)

    run(main())
    assert seqs == [4]  # RunStarted is 0; the three unbuilt events took 1..3
    assert BUILT == [5]


def test_handler_error_inside_resource_leaks_nothing() -> None:
    """D058: the handler's exception never reaches the model; the model's own
    exception stands; the resource ends with no unit in use or queued."""
    res = Resource(1)
    model: list[object] = []
    late: list[Event] = []

    class ModelError(Exception):
        pass

    def bad(e: Event) -> None:
        raise Boom

    async def holder() -> None:
        try:
            async with res:
                await asyncio.sleep(1)
                sim.emit(Ping, n=1)  # the handler raises here
                raise ModelError
        except ModelError as exc:
            model.append(exc)  # same step: the loop stops after it

    async def waiter() -> None:
        async with res:  # queued; granted by holder's release, never resumed
            model.append("waiter ran")

    async def main() -> None:
        await asyncio.gather(sim.spawn(holder()), sim.spawn(waiter()))

    with pytest.raises(Boom):
        run(main(), subscribers=[(bad, (Ping,)), late.append])
    assert len(model) == 1 and isinstance(model[0], ModelError)
    assert res.count == 0 and res.queue_length == 0
    assert _kinds(late) == ["RunStarted"]  # the later subscriber missed Ping


@pytest.mark.parametrize("cap_type", [Boom, KeyboardInterrupt])
def test_early_stop_pattern(
    cap_type: type[BaseException], caplog: pytest.LogCaptureFixture
) -> None:
    """D061: a recorder raising its own exception on the Nth event."""
    got: list[Event] = []
    model: list[str] = []
    cap = cap_type("cap reached")

    def recorder(e: Event) -> None:
        got.append(e)
        if e.seq == 3:
            raise cap

    async def ticker(name: str) -> None:
        try:
            while True:  # noqa: ASYNC110 (an endless event source)
                await asyncio.sleep(1)
                sim.emit(Ping, n=1)
                model.append(name)
        finally:
            model.append(f"{name} cancelled")

    async def main() -> None:
        sim.spawn(ticker("a"))
        await ticker("b")

    with caplog.at_level(logging.DEBUG), warnings.catch_warnings():
        warnings.simplefilter("error")
        with pytest.raises(cap_type) as info:
            run(main(), subscribers=[recorder])
        gc.collect()
    assert info.value is cap
    # b's timer is set first (main awaits it inline), so at each t b steps
    # before a. Events 0..3 delivered (b's emit at t=2 raises), nothing after
    # (a's emit in the same iteration is dropped), no RunFinished.
    assert [e.seq for e in got] == [0, 1, 2, 3] and _kinds(got)[0] == "RunStarted"
    # The model ran to the end of that iteration (a's step at t=2), then
    # teardown cancelled both tasks.
    assert model[:4] == ["b", "a", "b", "a"]
    assert sorted(model[4:]) == ["a cancelled", "b cancelled"]
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]


@pytest.mark.parametrize(
    ("handler_kind", "wins"), [(RunStarted, "handler"), (Ping, "sink")]
)
def test_first_observer_error_wins(handler_kind: type[Event], wins: str) -> None:
    """A trace sink and a handler both fail: run raises the earlier one.
    RunStarted comes before main's first trace record; Ping after it."""
    errors = {"handler": Boom("handler"), "sink": Boom("sink")}

    def handler(e: Event) -> None:
        raise errors["handler"]

    def sink(record: TraceRecord) -> None:
        raise errors["sink"]

    async def main() -> None:
        sim.emit(Ping, n=1)

    with pytest.raises(Boom) as info:
        run(main(), trace=sink, subscribers=[(handler, (handler_kind,))])
    assert info.value is errors[wins]


@pytest.mark.parametrize("main_fails", [False, True])
def test_run_finished_handler_error_raised_after_teardown(main_fails: bool) -> None:
    cleaned: list[str] = []
    err = Boom("finished")

    def handler(e: Event) -> None:
        raise err

    async def leftover() -> None:
        try:
            await asyncio.sleep(10)
        finally:
            cleaned.append("leftover")

    async def main() -> None:
        sim.spawn(leftover())
        await asyncio.sleep(1)
        if main_fails:
            raise ValueError("main")

    with pytest.raises(Boom) as info:
        run(main(), subscribers=[(handler, (RunFinished,))])
    assert info.value is err and cleaned == ["leftover"]
    assert isinstance(info.value.__context__, ValueError) is main_fails


@pytest.mark.parametrize("a_raises", [False, True])
def test_nested_emit_failure_stops_the_outer_dispatch(a_raises: bool) -> None:
    calls: list[str] = []

    def a(e: Event) -> None:
        calls.append(f"a:{type(e).__name__}")
        if type(e) is Ping:
            sim.emit(Pong, n=2)  # delivered inline, before c sees Ping
            if a_raises:
                raise Boom("a")  # after b's failure: the first error wins

    def b(e: Event) -> None:
        calls.append(f"b:{type(e).__name__}")
        raise Boom("b")

    async def main() -> None:
        sim.subscribe(a, kinds=(Ping,))
        sim.subscribe(b, kinds=(Pong,))
        sim.subscribe(lambda e: calls.append("c"))
        sim.emit(Ping, n=1)

    with pytest.raises(Boom, match="b"):
        run(main())
    assert calls == ["a:Ping", "a:Pong", "b:Pong"]


async def test_helpers_do_nothing_on_plain_asyncio() -> None:
    assert not isinstance(asyncio.get_running_loop(), VirtualTimeLoop)
    emit_event(Ping, n=1)
    assert register_resource_name(object(), None) is None
    assert register_resource_name(object(), "x") == "x"


def test_helpers_in_a_run() -> None:
    got: list[Event] = []
    names: list[str | None] = []

    async def main() -> None:
        sim.subscribe(got.append, kinds=(Ping,))
        emit_event(Ping, n=5)
        a, b, c, d, x = object(), object(), object(), object(), object()
        names.extend(
            [
                register_resource_name(a, None),
                register_resource_name(b, "teller"),
                register_resource_name(c, None),
                register_resource_name(a, "ignored"),  # already registered
            ]
        )
        # An explicit name taken by an explicit or an automatic one raises,
        # and changes nothing: x stays unregistered, the counter unmoved.
        with pytest.raises(ValueError, match="teller"):
            register_resource_name(x, "teller")
        with pytest.raises(ValueError, match="resource-1"):
            register_resource_name(x, "resource-1")
        # An automatic name skips one taken explicitly; it never raises (D064).
        names.append(register_resource_name(d, "resource-3"))
        names.append(register_resource_name(x, None))

    for _ in range(2):  # names are per run
        got.clear()
        names.clear()
        run(main())
        assert names == [
            "resource-1",
            "teller",
            "resource-2",
            "resource-1",
            "resource-3",
            "resource-4",
        ]
        assert [(e.t, e.seq) for e in got] == [(0.0, 1)]
