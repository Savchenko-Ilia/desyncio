"""Tests for INT-01: ``sim.interrupt(task, cause)``.

Every test runs a tiny model with ``desyncio.run``. The interesting question in
each one is: what did the *asyncio primitive* the target was waiting on do
when the interrupt hit? (It must clean up its wait, exactly as for a cancel.)
"""

import asyncio
from typing import Any

import pytest

import desyncio
from desyncio import sim
from desyncio.interrupt import Interrupt
from desyncio.trace import ListSink

# --- the exception ------------------------------------------------------


def test_interrupt_is_an_exception_with_a_cause() -> None:
    assert issubclass(Interrupt, Exception)
    assert not issubclass(Interrupt, asyncio.CancelledError)
    assert Interrupt("why").cause == "why"
    assert Interrupt().cause is None


# --- blocked on sim.timeout ----------------------------------------------


def test_interrupt_a_timeout_wakes_now_and_cancels_the_timer() -> None:
    async def main() -> tuple[Any, ...]:
        loop = asyncio.get_running_loop()
        seen: list[tuple[float, object]] = []

        async def worker() -> None:
            try:
                await sim.timeout(10)
            except Interrupt as exc:
                seen.append((sim.now, exc.cause))

        task = sim.spawn(worker())
        await sim.timeout(3)
        sim.interrupt(task, cause="boom")
        await task
        # The 10-unit timer was withdrawn: no live timer remains ...
        live = [h for h in loop._scheduled if not h.cancelled()]  # type: ignore[attr-defined]
        # ... so the clock does not jump to t=10.
        await sim.timeout(2)
        return seen, live, sim.now

    seen, live, now = desyncio.run(main())
    assert seen == [(3.0, "boom")]
    assert live == []
    assert now == 5.0


def test_uncaught_interrupt_fails_the_task_like_any_exception() -> None:
    async def main() -> None:
        async def worker() -> None:
            await sim.timeout(10)

        task = sim.spawn(worker())
        await sim.timeout(1)
        sim.interrupt(task, "x")
        with pytest.raises(Interrupt) as info:
            await task
        assert info.value.cause == "x"
        assert not task.cancelled()

    desyncio.run(main())


# --- blocked on asyncio primitives ---------------------------------------


def test_interrupt_a_semaphore_waiter_is_removed_and_next_is_served() -> None:
    async def main() -> list[tuple[str, float]]:
        sem = asyncio.Semaphore(1)
        log: list[tuple[str, float]] = []

        async def holder() -> None:
            async with sem:
                await sim.timeout(5)

        async def waiter(name: str) -> None:
            try:
                async with sem:
                    log.append((name + " got", sim.now))
            except Interrupt:
                log.append((name + " interrupted", sim.now))

        h = sim.spawn(holder())
        await sim.timeout(0)  # let the holder take the unit
        w1 = sim.spawn(waiter("w1"))
        w2 = sim.spawn(waiter("w2"))
        await sim.timeout(1)
        sim.interrupt(w1)
        await asyncio.gather(h, w1, w2)
        assert not sem.locked()
        assert len(sem._waiters or ()) == 0
        return log

    assert desyncio.run(main()) == [("w1 interrupted", 1.0), ("w2 got", 5.0)]


def test_semaphore_grant_race_does_not_leak_the_unit() -> None:
    """The holder releases and interrupts in the same instant.

    ``release()`` has already resolved w1's wait future with ``True`` (the
    unit is w1's), but w1 has not run yet. The interrupt must not undo that
    half-way: w1 owns the unit, and gets the interrupt at its next ``await``
    (inside the ``async with``), which releases the unit for w2.
    """

    async def main() -> list[tuple[str, float]]:
        sem = asyncio.Semaphore(1)
        log: list[tuple[str, float]] = []
        await sem.acquire()

        async def waiter(name: str) -> None:
            try:
                async with sem:
                    log.append((name + " got", sim.now))
                    await sim.timeout(100)
            except Interrupt:
                log.append((name + " interrupted", sim.now))

        w1 = sim.spawn(waiter("w1"))
        w2 = sim.spawn(waiter("w2"))
        await sim.timeout(1)
        sem.release()  # grants the unit to w1 (its future is resolved)
        sim.interrupt(w1)  # ... but w1 has not run yet
        await asyncio.gather(w1, w2)
        assert not sem.locked()
        assert sem._value == 1
        return log

    assert desyncio.run(main()) == [
        ("w1 got", 1.0),
        ("w1 interrupted", 1.0),
        ("w2 got", 1.0),
    ]


def test_interrupt_a_queue_getter_item_goes_to_the_next_getter() -> None:
    async def main() -> list[tuple[str, object]]:
        q: asyncio.Queue[str] = asyncio.Queue()
        log: list[tuple[str, object]] = []

        async def getter(name: str) -> None:
            try:
                log.append((name, await q.get()))
            except Interrupt:
                log.append((name, "interrupted"))

        g1 = sim.spawn(getter("g1"))
        g2 = sim.spawn(getter("g2"))
        await sim.timeout(1)
        sim.interrupt(g1)
        await sim.timeout(1)
        q.put_nowait("item")
        await asyncio.gather(g1, g2)
        assert q.empty()
        return log

    assert desyncio.run(main()) == [("g1", "interrupted"), ("g2", "item")]


def test_interrupt_an_event_waiter() -> None:
    async def main() -> list[object]:
        ev = asyncio.Event()
        log: list[object] = []

        async def waiter(name: str) -> None:
            try:
                await ev.wait()
                log.append((name, "set", sim.now))
            except Interrupt:
                log.append((name, "interrupted", sim.now))

        a = sim.spawn(waiter("a"))
        b = sim.spawn(waiter("b"))
        await sim.timeout(1)
        sim.interrupt(a)
        await sim.timeout(1)
        ev.set()
        await asyncio.gather(a, b)
        assert len(ev._waiters) == 0
        return log

    assert desyncio.run(main()) == [("a", "interrupted", 1.0), ("b", "set", 2.0)]


def test_interrupt_a_condition_waiter_keeps_the_lock_invariant() -> None:
    """``Condition.wait`` re-acquires its lock even when the wait is cut short."""

    async def main() -> list[object]:
        cond = asyncio.Condition()
        log: list[object] = []

        async def waiter() -> None:
            async with cond:
                try:
                    await cond.wait()
                except Interrupt as exc:
                    # Back in the caller, the lock is held again.
                    log.append(("interrupted", cond.locked(), exc.cause))

        task = sim.spawn(waiter())
        await sim.timeout(1)
        sim.interrupt(task, "c")
        await task
        assert not cond.locked()
        assert len(cond._waiters) == 0
        return log

    assert desyncio.run(main()) == [("interrupted", True, "c")]


# --- it is not a cancel --------------------------------------------------


def test_cancelling_is_unchanged_and_the_task_carries_on() -> None:
    async def main() -> tuple[int, int, str]:
        counts: list[int] = []

        async def worker() -> str:
            me = asyncio.current_task()
            assert me is not None
            try:
                await sim.timeout(10)
            except Interrupt:
                counts.append(me.cancelling())
            await sim.timeout(1)
            return "finished"

        task = sim.spawn(worker())
        await sim.timeout(1)
        sim.interrupt(task)
        result = await task
        return counts[0], task.cancelling(), result

    assert desyncio.run(main()) == (0, 0, "finished")


def test_taskgroup_siblings_are_not_cancelled() -> None:
    async def main() -> list[str]:
        log: list[str] = []

        async def victim() -> None:
            try:
                await sim.timeout(10)
            except Interrupt:
                log.append("victim interrupted")
            await sim.timeout(1)
            log.append("victim done")

        async def sibling() -> None:
            await sim.timeout(5)
            log.append("sibling done")

        async with asyncio.TaskGroup() as tg:
            v = tg.create_task(victim())
            tg.create_task(sibling())
            await sim.timeout(2)
            sim.interrupt(v)
        log.append(f"group done at {sim.now}")
        return log

    assert desyncio.run(main()) == [
        "victim interrupted",
        "victim done",
        "sibling done",
        "group done at 5.0",
    ]


def test_asyncio_timeout_deadline_still_works() -> None:
    async def main() -> list[object]:
        log: list[object] = []

        async def worker() -> None:
            try:
                async with asyncio.timeout(5):
                    try:
                        await sim.timeout(100)
                    except Interrupt:
                        log.append(("interrupted", sim.now))
                    await sim.timeout(100)
            except TimeoutError:
                log.append(("deadline", sim.now))

        task = sim.spawn(worker())
        await sim.timeout(2)
        sim.interrupt(task)
        await task
        return log

    assert desyncio.run(main()) == [("interrupted", 2.0), ("deadline", 5.0)]


def test_interrupt_inside_a_timeout_block_is_not_a_timeout_error() -> None:
    async def main() -> object:
        async def worker() -> None:
            async with asyncio.timeout(50):
                await sim.timeout(100)

        task = sim.spawn(worker())
        await sim.timeout(2)
        sim.interrupt(task, "c")
        with pytest.raises(Interrupt):
            await task
        return sim.now

    assert desyncio.run(main()) == 2.0


@pytest.mark.parametrize("interrupt_first", [True, False])
def test_a_real_cancel_arriving_with_an_interrupt_wins(
    interrupt_first: bool,
) -> None:
    async def main() -> tuple[bool, list[str]]:
        log: list[str] = []

        async def worker() -> None:
            try:
                await sim.timeout(10)
            except Interrupt:
                log.append("interrupt")
            except asyncio.CancelledError:
                log.append("cancel")
                raise
            await sim.timeout(1)  # only reached if the cancel were swallowed
            log.append("carried on")

        task = sim.spawn(worker())
        await sim.timeout(1)
        if interrupt_first:
            sim.interrupt(task)
            task.cancel()
        else:
            task.cancel()
            sim.interrupt(task)
        with pytest.raises(asyncio.CancelledError):
            await task
        return task.cancelled(), log

    cancelled, log = desyncio.run(main())
    assert cancelled
    assert log == ["cancel"]


# --- timing of delivery ---------------------------------------------------


def test_target_that_has_not_started_gets_it_at_its_first_await() -> None:
    async def main() -> list[object]:
        log: list[object] = []

        async def worker() -> None:
            log.append(("started", sim.now))
            try:
                await sim.timeout(10)
            except Interrupt as exc:
                log.append(("interrupted", sim.now, exc.cause))

        task = sim.spawn(worker())
        sim.interrupt(task, "early")  # worker has not run a single line
        await task
        return log

    assert desyncio.run(main()) == [("started", 0.0), ("interrupted", 0.0, "early")]


def test_target_woken_but_not_yet_running_gets_it_at_the_next_await() -> None:
    async def main() -> list[object]:
        log: list[object] = []

        async def worker() -> None:
            await sim.timeout(1)
            log.append("working")  # not at an await here
            try:
                await sim.timeout(10)
            except Interrupt:
                log.append(("interrupted", sim.now))

        task = sim.spawn(worker())
        await sim.timeout(1)
        # Both wake at t=1. The worker's timer has fired but its step has not
        # run yet, so it is not waiting on anything: the interrupt waits for
        # its next await.
        sim.interrupt(task)
        await task
        return log

    assert desyncio.run(main()) == ["working", ("interrupted", 1.0)]


def test_several_interrupts_are_delivered_one_per_await_in_order() -> None:
    async def main() -> list[object]:
        log: list[object] = []

        async def worker() -> None:
            for _ in range(3):
                try:
                    await sim.timeout(10)
                except Interrupt as exc:
                    log.append((exc.cause, sim.now))
            await sim.timeout(1)
            log.append(("done", sim.now))

        task = sim.spawn(worker())
        await sim.timeout(1)
        for cause in ("a", "b", "c"):
            sim.interrupt(task, cause)
        await task
        return log

    assert desyncio.run(main()) == [("a", 1.0), ("b", 1.0), ("c", 1.0), ("done", 2.0)]


def test_interrupts_from_different_callers_keep_their_order() -> None:
    async def main() -> list[object]:
        log: list[object] = []

        async def worker() -> None:
            for _ in range(2):
                try:
                    await sim.timeout(10)
                except Interrupt as exc:
                    log.append(exc.cause)

        async def sender(task: asyncio.Task[None], cause: str) -> None:
            sim.interrupt(task, cause)

        task = sim.spawn(worker())
        await sim.timeout(1)
        sim.interrupt(task, "first")
        await sim.spawn(sender(task, "second"))
        await task
        return log

    assert desyncio.run(main()) == ["first", "second"]


# --- errors -----------------------------------------------------------------


def test_interrupting_a_finished_task_raises() -> None:
    async def main() -> None:
        async def worker() -> None:
            return None

        task = sim.spawn(worker())
        await task
        with pytest.raises(RuntimeError, match="finished"):
            sim.interrupt(task)

    desyncio.run(main())


def test_interrupting_the_current_task_raises() -> None:
    async def main() -> None:
        me = asyncio.current_task()
        assert me is not None
        with pytest.raises(RuntimeError, match="itself"):
            sim.interrupt(me)

    desyncio.run(main())


def test_interrupting_a_task_that_awaits_another_task_raises() -> None:
    """Documented limit: a Task cannot be failed from outside, only cancelled."""

    async def main() -> None:
        async def child() -> None:
            await sim.timeout(10)

        async def parent(c: asyncio.Task[None]) -> None:
            await c

        c = sim.spawn(child())
        p = sim.spawn(parent(c))
        await sim.timeout(1)
        with pytest.raises(RuntimeError, match="Task"):
            sim.interrupt(p)
        c.cancel()
        p.cancel()
        await asyncio.gather(c, p, return_exceptions=True)

    desyncio.run(main())


def test_interrupt_outside_a_simulation_raises() -> None:
    async def other() -> None:
        pass

    async def run_plain() -> None:
        task = asyncio.create_task(other())
        with pytest.raises(RuntimeError, match="VirtualTimeLoop"):
            sim.interrupt(task)
        await task

    asyncio.run(run_plain())


def test_interrupt_rejects_a_non_task() -> None:
    async def main() -> None:
        with pytest.raises(TypeError):
            sim.interrupt("nope")  # type: ignore[arg-type]

    desyncio.run(main())


# --- machine breakdown ---------------------------------------------------------


def test_machine_breakdown_worker_resumes_the_remaining_work() -> None:
    """The classic SimPy example: a part needs 10 units of work, a breakdown
    at t=4 stops the machine for 3 units of repair, then work carries on."""

    async def main() -> list[object]:
        log: list[object] = []

        async def machine() -> float:
            remaining = 10.0
            while remaining > 0:
                started = sim.now
                try:
                    await sim.timeout(remaining)
                    remaining = 0.0
                except Interrupt as exc:
                    remaining -= sim.now - started
                    log.append(("broken", sim.now, exc.cause, remaining))
                    await sim.timeout(3)  # repair
                    log.append(("repaired", sim.now))
            log.append(("part done", sim.now))
            return sim.now

        async def breaker(target: asyncio.Task[float]) -> None:
            await sim.timeout(4)
            sim.interrupt(target, cause="breakdown")

        m = sim.spawn(machine())
        sim.spawn(breaker(m))
        assert await m == 13.0
        return log

    assert desyncio.run(main()) == [
        ("broken", 4.0, "breakdown", 6.0),
        ("repaired", 7.0),
        ("part done", 13.0),
    ]


# --- determinism -----------------------------------------------------------------


def _model_with_interrupts() -> Any:
    async def main() -> list[float]:
        rng = sim.rng("breaks")
        done: list[float] = []
        sem = asyncio.Semaphore(2)

        async def job() -> None:
            left = 5.0
            while left > 0:
                t0 = sim.now
                try:
                    async with sem:
                        await sim.timeout(left)
                    left = 0.0
                except Interrupt:
                    left -= sim.now - t0
                    await sim.timeout(0.5)
            done.append(sim.now)

        jobs = [sim.spawn(job()) for _ in range(5)]
        for _ in range(8):
            await sim.timeout(rng.expovariate(1.0))
            victim = jobs[rng.randrange(len(jobs))]
            if not victim.done():
                try:
                    sim.interrupt(victim, "x")
                except RuntimeError:
                    pass  # waiting on a Task or finished: deterministic too
        await asyncio.gather(*jobs)
        return done

    return main()


def test_same_seed_gives_identical_trace_with_interrupts() -> None:
    sink1, sink2, sink3 = ListSink(), ListSink(), ListSink()
    r1 = desyncio.run(_model_with_interrupts(), seed=7, trace=sink1)
    r2 = desyncio.run(_model_with_interrupts(), seed=7, trace=sink2)
    desyncio.run(_model_with_interrupts(), seed=8, trace=sink3)
    assert r1 == r2
    assert sink1.records == sink2.records
    assert len(sink1.records) > 20
    assert sink1.records != sink3.records
