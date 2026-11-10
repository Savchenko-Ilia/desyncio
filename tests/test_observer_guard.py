"""Tests for the debug-mode observer guard (EVT-06, D059).

In asyncio debug mode an event handler that tries to change the run (create
a task, schedule a callback, interrupt, draw a random number, emit) gets
``ObserverError``; like any handler error it ends the run (D058).
"""

import asyncio
import gc
import types
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import pytest

from desyncio import events as events_module
from desyncio.bus import ObserverError
from desyncio.events import Event, RunFinished, RunStarted
from desyncio.interrupt import Interrupt
from desyncio.loop import VirtualTimeLoop
from desyncio.runners import run
from desyncio.sim import sim


@dataclass(frozen=True, slots=True, kw_only=True)
class Ping(Event):
    n: int


@dataclass(frozen=True, slots=True, kw_only=True)
class Pong(Event):
    pass


class Env:
    """What a forbidden call needs: the loop and a task it may interrupt."""

    loop: asyncio.AbstractEventLoop
    victim: "asyncio.Task[None]"


async def _child() -> None:
    await asyncio.sleep(0)


def _noop() -> None:
    pass


# One representative per guarded code path: sim.spawn for create_task
# (asyncio.create_task and TaskGroup end in the same loop.create_task),
# call_later for _schedule via call_at, sim.emit for Bus.emit.
FORBIDDEN: dict[str, Callable[[Env], object]] = {
    "sim.spawn": lambda env: sim.spawn(_child()),
    "loop.call_soon": lambda env: env.loop.call_soon(_noop),
    "loop.call_later": lambda env: env.loop.call_later(1.0, _noop),
    "loop.call_at_priority": lambda env: env.loop.call_at_priority(5.0, 1, _noop),  # type: ignore[attr-defined]
    "sim.interrupt": lambda env: sim.interrupt(env.victim, "x"),
    "sim.random": lambda env: sim.random("s"),
    "sim.np_random": lambda env: sim.np_random("s"),
    "sim.emit": lambda env: sim.emit(Pong),
}
# The name the error message must contain for each call.
NAMED = {
    "sim.spawn": "create_task",
    "loop.call_soon": "call_soon",
    "loop.call_later": "loop.call_at ",
    "loop.call_at_priority": "loop.call_at_priority",
    "sim.interrupt": "sim.interrupt",
    "sim.random": "sim.random",
    "sim.np_random": "sim.np_random",
    "sim.emit": "emit (",
}


def _snapshot(env: Env) -> tuple[int, ...]:
    """What a forbidden call would change: tasks, timers, ready callbacks,
    the victim's pending interrupt, the event sequence number."""
    loop: Any = env.loop
    return (
        len(asyncio.all_tasks()),
        len(loop._scheduled),
        len(loop._ready),
        env.victim.cancelling(),
        loop.bus._seq,
    )


def _model(call: Callable[[Env], object], log: list[str]) -> Any:
    """Main: start a victim in an interruptible block, emit Ping, go on.

    The handler makes ``call`` and logs, synchronously, whether it raised
    ``ObserverError`` (and the message) and whether the run's state changed.
    """

    async def victim() -> None:
        try:
            async with sim.interruptible():
                await sim.timeout(10)
            log.append("victim done")
        except Interrupt:
            log.append("victim interrupted")

    async def main() -> None:
        env = Env()
        env.loop = asyncio.get_running_loop()
        env.victim = sim.spawn(victim())
        await sim.timeout(1)

        def handler(event: Ping) -> None:
            before = _snapshot(env)
            try:
                call(env)
            except ObserverError as exc:
                log.append(f"raised: {exc}")
            else:
                log.append("returned")
            log.append("unchanged" if _snapshot(env) == before else "changed")

        sim.subscribe(handler, kinds=(Ping,))
        sim.emit(Ping, n=1)
        log.append("after emit")
        await env.victim
        log.append("main done")

    return main()


@pytest.mark.parametrize("name", list(FORBIDDEN))
def test_forbidden_call_raises_in_debug_mode(name: str) -> None:
    """The call raises ObserverError naming it inside the handler, before it
    changes anything; run ends and raises that error. call_soon only records
    the error: the callback is scheduled and run raises afterwards."""
    log: list[str] = []
    with pytest.raises(ObserverError) as info:
        run(_model(FORBIDDEN[name], log), debug=True)
    message = str(info.value)  # not the notes: they hold the handler's source
    assert NAMED[name] in message
    assert "observers must not change the run" in message
    if name == "loop.call_soon":
        assert log[:2] == ["returned", "changed"]
    else:
        assert log[:2] == [f"raised: {message}", "unchanged"]
    # The emitting task finished its step; the loop stopped after it, so the
    # victim was never interrupted and main never resumed (D058).
    assert log[2:] == ["after emit"]


@pytest.mark.parametrize(
    "name",
    [
        "sim.spawn",
        "loop.call_soon",
        "loop.call_later",
        "sim.interrupt",
        "sim.random",
        "sim.np_random",
    ],
)
def test_forbidden_call_not_checked_outside_debug_mode(name: str) -> None:
    """Outside debug mode nothing is checked (the effect is undefined)."""
    log: list[str] = []
    run(_model(FORBIDDEN[name], log), debug=False)
    assert log[0] == "returned"
    assert log[2] == "after emit"


def test_call_soon_threadsafe_is_not_guarded() -> None:
    """D066: asyncio's async-generator finalizer, which garbage collection can
    run inside a handler, calls call_soon_threadsafe (as do other threads);
    it is not handler code and must not trip the guard."""
    closed: list[str] = []
    held: list[object] = []

    async def agen() -> Any:
        try:
            yield 1
            yield 2  # pragma: no cover
        finally:
            closed.append("finally")

    def handler(event: Ping) -> None:
        held.clear()
        gc.collect()  # finalizer: call_soon_threadsafe(create_task, aclose())
        asyncio.get_running_loop().call_soon_threadsafe(_noop)

    async def main() -> str:
        gen = agen()
        await anext(gen)
        held.append(gen)
        del gen
        sim.emit(Ping, n=1)
        await sim.timeout(1)
        return "done"

    assert run(main(), debug=True, subscribers=[(handler, (Ping,))]) == "done"
    assert closed == ["finally"]


def test_swallowed_observer_error_still_ends_the_run() -> None:
    """A handler cannot hide a violation by catching ObserverError."""
    caught: list[ObserverError] = []

    def handler(event: Ping) -> None:
        try:
            sim.random("s")
        except ObserverError as exc:
            caught.append(exc)

    async def main() -> None:
        sim.emit(Ping, n=1)
        await sim.timeout(1)
        raise AssertionError("not reached")  # pragma: no cover

    with pytest.raises(ObserverError) as info:
        run(main(), debug=True, subscribers=[(handler, (Ping,))])
    assert caught == [info.value]


def test_handler_resolving_an_awaited_future() -> None:
    """call_soon is reached from inside fut.set_result: it records the error
    and lets the wake-up be scheduled, so teardown completes (raising there
    would lose the wake-up and deadlock). The note shows the handler's line."""
    log: list[str] = []

    async def main() -> None:
        fut: asyncio.Future[int] = asyncio.get_running_loop().create_future()

        def handler(event: Ping) -> None:
            fut.set_result(1)  # the waiter's wake-up goes through call_soon
            log.append("handler went on")

        async def emitter() -> None:
            sim.emit(Ping, n=1)

        sim.subscribe(handler, kinds=(Ping,))
        sim.spawn(emitter())
        await fut
        log.append("main resumed")  # pragma: no cover

    with pytest.raises(ObserverError, match="call_soon") as info:
        run(main(), debug=True)
    assert log == ["handler went on"]
    assert "fut.set_result(1)" in "".join(info.value.__notes__)


def test_subscribe_and_close_allowed_during_dispatch() -> None:
    seen: list[str] = []

    def late(event: Event) -> None:
        seen.append(f"late {type(event).__name__}")

    async def main() -> None:
        def handler(event: Ping) -> None:
            seen.append(f"handler {event.n}")
            sim.subscribe(late, kinds=(Ping,))
            sub.close()

        sub = sim.subscribe(handler, kinds=(Ping,))
        sim.emit(Ping, n=1)
        sim.emit(Ping, n=2)
        await sim.timeout(1)

    run(main(), debug=True)
    assert seen == ["handler 1", "late Ping"]


def test_lifecycle_handlers_in_debug_mode() -> None:
    """Benign RunStarted/RunFinished handlers pass: the loop's own scheduling
    (RunStarted comes from a call_soon callback, RunFinished after the loop
    stopped) never trips the guard; forbidden calls in them do."""
    seen: list[str] = []

    async def main() -> int:
        await sim.timeout(1)
        return 7

    def record(event: Event) -> None:
        seen.append(type(event).__name__)

    assert run(main(), debug=True, subscribers=[record]) == 7
    assert seen == ["RunStarted", "RunFinished"]

    def started(event: RunStarted) -> None:
        asyncio.get_running_loop().call_soon(_noop)

    with pytest.raises(ObserverError, match="call_soon"):
        run(main(), debug=True, subscribers=[(started, (RunStarted,))])

    loops: list[asyncio.AbstractEventLoop] = []

    async def keep_loop() -> None:
        loops.append(asyncio.get_running_loop())

    def finished(event: RunFinished) -> None:
        loops[0].call_later(1.0, _noop)

    with pytest.raises(ObserverError, match="call_at"):
        run(keep_loop(), debug=True, subscribers=[(finished, (RunFinished,))])


def test_flag_restored_after_nested_emit_and_handler_error() -> None:
    """Nested dispatch restores the outer flag; a raising handler clears it."""
    loop = VirtualTimeLoop()
    try:
        bus = loop.bus
        results: list[str] = []

        def outer(event: Ping) -> None:
            bus.emit(Pong, {})  # nested dispatch, allowed outside debug mode
            loop.set_debug(True)
            try:
                loop.call_later(1.0, _noop)
            except ObserverError:
                results.append("still guarded after nested emit")
            raise RuntimeError("handler fails")

        bus.subscribe(outer, (Ping,))
        bus.subscribe(lambda e: results.append("inner"), (Pong,))
        bus.emit(Ping, {"n": 1})
        assert results == ["inner", "still guarded after nested emit"]
        loop.call_later(1.0, _noop)  # dispatch over: allowed again
        error = loop.pop_observer_error()
        assert isinstance(error, ObserverError)  # the first error wins
    finally:
        loop.close()


def _determinism_model(events: list[dict[str, object]]) -> Any:
    async def worker(i: int) -> float:
        rng = sim.random("w")
        try:
            async with sim.interruptible():
                await sim.timeout(rng.expovariate(1.0))
        except Interrupt:
            pass
        sim.emit(Ping, n=i)
        return sim.now

    async def main() -> list[float]:
        tasks = [sim.spawn(worker(i)) for i in range(5)]
        await sim.timeout(0.5)
        if not tasks[2].done():
            sim.interrupt(tasks[2], "stop")
        async with asyncio.TaskGroup() as tg:
            tg.create_task(sim.timeout(0.3))
        return [await t for t in tasks]

    return main()


def test_run_identical_with_debug_on_and_off() -> None:
    def record(debug: bool) -> tuple[object, list[dict[str, object]]]:
        events: list[dict[str, object]] = []
        result = run(
            _determinism_model(events),
            seed=3,
            debug=debug,
            subscribers=[lambda e: events.append(e.to_dict())],
        )
        return result, events

    on, off = record(True), record(False)
    assert on == off
    assert len(on[1]) == 7  # RunStarted, five Pings, RunFinished


@pytest.mark.parametrize(
    ("env", "debug", "expected"),
    [(None, None, False), ("1", None, True), ("1", False, False), (None, True, True)],
)
def test_debug_argument_is_forwarded(
    monkeypatch: pytest.MonkeyPatch, env: str | None, debug: bool | None, expected: bool
) -> None:
    """``debug=None`` keeps asyncio's default (PYTHONASYNCIODEBUG works)."""
    if env is None:
        monkeypatch.delenv("PYTHONASYNCIODEBUG", raising=False)
    else:
        monkeypatch.setenv("PYTHONASYNCIODEBUG", env)

    async def main() -> bool:
        return asyncio.get_running_loop().get_debug()

    assert run(main(), debug=debug) is expected


@pytest.mark.parametrize("debug", [1, "yes"])
def test_debug_argument_validation(debug: object) -> None:
    async def main() -> None:
        pass

    coro = main()
    with pytest.raises(TypeError, match="debug must be a bool or None"):
        run(coro, debug=debug)  # type: ignore[call-overload]
    assert isinstance(coro, types.CoroutineType)
    assert coro.cr_frame is None  # closed: no "never awaited" warning


def test_observer_error_is_exported_from_events() -> None:
    assert events_module.ObserverError is ObserverError
    assert "ObserverError" in events_module.__all__
    assert issubclass(ObserverError, RuntimeError)
