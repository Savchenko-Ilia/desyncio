"""Tests for ``desyncio.interrupt`` (INT-01): timeout-style interrupts.

``async with sim.interruptible():`` behaves like ``async with asyncio.timeout()``
and ``sim.interrupt(task, cause)`` plays the role of the timer firing.
"""

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

import pytest

import desyncio
from desyncio import sim
from desyncio.container import Container
from desyncio.interrupt import Interrupt
from desyncio.resource import Resource
from desyncio.store import Store
from desyncio.trace import ListSink

# --- helpers -----------------------------------------------------------


async def _interrupt_blocked(
    wait: Callable[[], Awaitable[object]], *, at: float = 1.0, cause: object = "boom"
) -> tuple[Interrupt, float, int]:
    """Run ``wait`` inside a block, interrupt the task at ``at``.

    Returns (the Interrupt seen, the virtual time it was seen, cancelling()).
    """
    seen: list[Interrupt] = []

    async def worker() -> tuple[float, int]:
        try:
            async with sim.interruptible():
                await wait()
        except Interrupt as exc:
            seen.append(exc)
        task = asyncio.current_task()
        assert task is not None
        return sim.now, task.cancelling()

    task = sim.spawn(worker())
    await sim.timeout(at)
    sim.interrupt(task, cause)
    now, cancelling = await task
    assert len(seen) == 1
    return seen[0], now, cancelling


# --- facade / exception shape -------------------------------------------


def test_interrupt_is_exception_not_cancelled_error() -> None:
    assert issubclass(Interrupt, Exception)
    assert not issubclass(Interrupt, asyncio.CancelledError)
    assert Interrupt("x").cause == "x"
    assert Interrupt().cause is None


def test_not_exported_at_top_level() -> None:
    assert not hasattr(desyncio, "Interrupt")
    assert "Interrupt" not in desyncio.__all__


def test_facade_outside_run_raises() -> None:
    with pytest.raises(RuntimeError):
        sim.interruptible()
    with pytest.raises(RuntimeError):
        sim.interrupt(None, "x")  # type: ignore[arg-type]


# --- blocked on each asyncio / desyncio primitive ------------------------


def test_interrupt_sim_timeout_clock_does_not_jump() -> None:
    async def main() -> None:
        exc, now, cancelling = await _interrupt_blocked(lambda: sim.timeout(100))
        assert exc.cause == "boom"
        assert now == 1.0
        assert cancelling == 0
        # The cancelled timer is gone: the clock does not advance to t=100.
        await sim.timeout(5)
        assert sim.now == 6.0

    desyncio.run(main())
    # And the run itself ends without the loop sleeping until the dead timer.


def test_interrupt_clock_never_reaches_cancelled_timer() -> None:
    async def main() -> float:
        await _interrupt_blocked(lambda: sim.timeout(100))
        return sim.now

    assert desyncio.run(main()) == 1.0


def test_interrupt_lock_waiter_next_waiter_not_stranded() -> None:
    async def main() -> list[str]:
        lock = asyncio.Lock()
        order: list[str] = []
        await lock.acquire()

        async def waiter(name: str) -> None:
            async with lock:
                order.append(name)

        async def victim() -> None:
            try:
                async with sim.interruptible():
                    await lock.acquire()
            except Interrupt:
                order.append("victim-interrupted")

        v = sim.spawn(victim())
        w = sim.spawn(waiter("next"))
        await sim.timeout(1)
        sim.interrupt(v, None)
        await v
        lock.release()
        await w
        assert not lock.locked()
        return order

    assert desyncio.run(main()) == ["victim-interrupted", "next"]


def test_interrupt_condition_wait_lock_held_again_and_not_others() -> None:
    async def main() -> None:
        cond = asyncio.Condition()
        holder_released = False

        async def victim() -> None:
            async with cond:
                try:
                    async with sim.interruptible():
                        await cond.wait()
                except Interrupt:
                    # Condition.wait re-acquires the lock even on cancel.
                    assert cond.locked()
                    await sim.timeout(1)
                    assert cond.locked()
            assert not cond.locked()

        async def other() -> None:
            nonlocal holder_released
            await sim.timeout(2)
            async with cond:
                await sim.timeout(5)
                assert cond.locked()
            holder_released = True

        v = sim.spawn(victim())
        o = sim.spawn(other())
        await sim.timeout(1)
        sim.interrupt(v, "c")
        await v
        await o
        assert holder_released
        assert not cond.locked()

    desyncio.run(main())


def test_interrupt_condition_wait_while_other_holds_lock() -> None:
    """The lock is taken by someone else when the waiter is interrupted: the
    waiter must wait for it and must not release the other task's hold."""

    async def main() -> list[str]:
        cond = asyncio.Condition()
        log: list[str] = []

        async def victim() -> None:
            async with cond:
                try:
                    async with sim.interruptible():
                        await cond.wait()
                except Interrupt:
                    log.append(f"victim-back@{sim.now}")

        async def other() -> None:
            await sim.timeout(1)
            async with cond:
                cond.notify_all()
                await sim.timeout(4)  # keep the lock while victim is interrupted
                log.append("other-done")
                assert cond.locked()

        v = sim.spawn(victim())
        o = sim.spawn(other())
        await sim.timeout(2)  # other holds the lock now
        sim.interrupt(v, None)
        await asyncio.gather(v, o)
        assert not cond.locked()
        return log

    # victim runs again only once other released the lock at t=5
    assert desyncio.run(main()) == ["other-done", "victim-back@5.0"]


def test_interrupt_semaphore_no_leak() -> None:
    async def main() -> None:
        sem = asyncio.Semaphore(1)
        await sem.acquire()
        got: list[str] = []

        async def nxt() -> None:
            async with sem:
                got.append("next")

        n = sim.spawn(nxt())
        exc, _, _ = await _interrupt_blocked(sem.acquire)
        assert exc.cause == "boom"
        sem.release()
        await n
        assert got == ["next"]
        assert not sem.locked()

    desyncio.run(main())


def test_interrupt_resource_no_unit_leaked_next_waiter_served() -> None:
    async def main() -> list[object]:
        res = Resource(1)
        served: list[tuple[str, float]] = []

        async def holder() -> None:
            async with res:
                await sim.timeout(10)

        async def victim() -> None:
            try:
                async with sim.interruptible():
                    async with res:
                        served.append(("victim", sim.now))
            except Interrupt:
                pass

        async def nxt() -> None:
            async with res:
                served.append(("next", sim.now))

        h = sim.spawn(holder())
        await sim.timeout(0)
        v = sim.spawn(victim())
        n = sim.spawn(nxt())
        await sim.timeout(1)
        assert res.queue_length == 2
        sim.interrupt(v, "x")
        await v
        assert res.queue_length == 1
        await asyncio.gather(h, n)
        assert res.count == 0 and res.queue_length == 0
        return list(served)

    assert desyncio.run(main()) == [("next", 10.0)]


def test_interrupt_resource_holder_releases_unit() -> None:
    async def main() -> None:
        res = Resource(2)

        async def holder() -> None:
            try:
                async with sim.interruptible():
                    async with res:
                        await sim.timeout(100)
            except Interrupt:
                pass

        h = sim.spawn(holder())
        await sim.timeout(1)
        assert res.count == 1
        sim.interrupt(h, None)
        await h
        assert res.count == 0
        assert sim.now == 1.0

    desyncio.run(main())


def test_interrupt_queue_get_item_goes_to_next_getter() -> None:
    async def main() -> None:
        q: asyncio.Queue[int] = asyncio.Queue()
        second = sim.spawn(q.get())
        await sim.timeout(0)
        await _interrupt_blocked(q.get)
        # The interrupted getter is no longer queued, so the item reaches the
        # getter that was already waiting... which is "second" (first in line).
        q.put_nowait(7)
        assert await second == 7

    desyncio.run(main())


def test_interrupt_store_get_and_put() -> None:
    async def main() -> None:
        store: Store[int] = Store(capacity=1)
        await _interrupt_blocked(store.get)
        assert len(store) == 0
        await store.put(1)
        await _interrupt_blocked(lambda: store.put(2))
        assert store.items == (1,)
        assert await store.get() == 1
        assert store.items == ()

    desyncio.run(main())


def test_interrupt_container_get_and_put() -> None:
    async def main() -> None:
        c = Container(capacity=10, init=3)
        later = sim.spawn(c.get(5))
        await sim.timeout(0)
        await _interrupt_blocked(lambda: c.get(8))
        assert c.level == 3
        await _interrupt_blocked(lambda: c.put(9))
        assert c.level == 3
        await c.put(2)  # serves the get(5) that was ahead of the others
        await later
        assert c.level == 0

    desyncio.run(main())


def test_interrupt_event_wait() -> None:
    async def main() -> None:
        ev = asyncio.Event()
        exc, now, _ = await _interrupt_blocked(ev.wait)
        assert (exc.cause, now) == ("boom", 1.0)
        ev.set()
        await ev.wait()

    desyncio.run(main())


def test_interrupt_taskgroup_inside_block_cancels_children_then_interrupt() -> None:
    async def main() -> list[str]:
        log: list[str] = []

        async def child() -> None:
            try:
                await sim.timeout(100)
            except asyncio.CancelledError:
                log.append("child-cancelled")
                raise

        async def worker() -> None:
            try:
                async with sim.interruptible():
                    async with asyncio.TaskGroup() as tg:
                        tg.create_task(child())
                        tg.create_task(child())
                        await sim.timeout(100)
            except Interrupt as exc:
                log.append(f"interrupt:{exc.cause}")

        w = sim.spawn(worker())
        await sim.timeout(1)
        sim.interrupt(w, "tg")
        await w
        assert sim.now == 1.0
        return log

    assert desyncio.run(main()) == [
        "child-cancelled",
        "child-cancelled",
        "interrupt:tg",
    ]


# --- scope object --------------------------------------------------------


def test_scope_attributes_before_and_after() -> None:
    async def main() -> None:
        flags: list[tuple[bool, object]] = []

        async def worker() -> None:
            scope = sim.interruptible()
            assert (scope.interrupted, scope.cause) == (False, None)
            try:
                async with scope as entered:
                    assert entered is scope
                    assert not scope.interrupted
                    await sim.timeout(100)
            except Interrupt as exc:
                assert exc.cause == "why"
                assert isinstance(exc.__cause__, asyncio.CancelledError)
            flags.append((scope.interrupted, scope.cause))

        w = sim.spawn(worker())
        await sim.timeout(1)
        sim.interrupt(w, "why")
        await w
        assert flags == [(True, "why")]

    desyncio.run(main())


def test_block_not_interrupted_is_transparent() -> None:
    async def main() -> None:
        async with sim.interruptible() as scope:
            await sim.timeout(2)
        assert not scope.interrupted and scope.cause is None
        task = asyncio.current_task()
        assert task is not None and task.cancelling() == 0
        with pytest.raises(ValueError):
            async with sim.interruptible():
                raise ValueError

    desyncio.run(main())


# --- nesting and combination ---------------------------------------------


def test_nested_blocks_innermost_receives() -> None:
    async def main() -> list[str]:
        log: list[str] = []

        async def worker() -> None:
            outer = sim.interruptible()
            try:
                async with outer:
                    try:
                        async with sim.interruptible() as inner:
                            await sim.timeout(100)
                    except Interrupt as exc:
                        log.append(f"inner:{exc.cause}")
                    assert inner.interrupted and not outer.interrupted
                    # Inner exited: the outer block is innermost now. The
                    # translation to Interrupt happens at the *outer* exit.
                    await sim.timeout(100)
            except Interrupt as exc:
                log.append(f"outer:{exc.cause}")
            assert outer.interrupted

        w = sim.spawn(worker())
        await sim.timeout(1)
        sim.interrupt(w, "one")
        await sim.timeout(1)
        sim.interrupt(w, "two")
        await w
        return log

    assert desyncio.run(main()) == ["inner:one", "outer:two"]


def test_interrupt_inside_asyncio_timeout_interrupt_fires_first() -> None:
    async def main() -> list[object]:
        log: list[object] = []

        async def worker() -> None:
            try:
                async with asyncio.timeout(10):
                    async with sim.interruptible():
                        await sim.timeout(100)
            except Interrupt as exc:
                log.append(("interrupt", exc.cause, sim.now))
            except TimeoutError:
                log.append("timeout")
            task = asyncio.current_task()
            assert task is not None
            log.append(task.cancelling())

        w = sim.spawn(worker())
        await sim.timeout(1)
        sim.interrupt(w, "i")
        await w
        return log

    assert desyncio.run(main()) == [("interrupt", "i", 1.0), 0]


def test_asyncio_timeout_inside_interruptible_timeout_fires_first() -> None:
    async def main() -> list[object]:
        log: list[object] = []

        async def worker() -> None:
            try:
                async with sim.interruptible():
                    async with asyncio.timeout(1):
                        await sim.timeout(100)
            except Interrupt:
                log.append("interrupt")
            except TimeoutError:
                log.append(("timeout", sim.now))
            task = asyncio.current_task()
            assert task is not None
            log.append(task.cancelling())

        w = sim.spawn(worker())
        await w
        return log

    assert desyncio.run(main()) == [("timeout", 1.0), 0]


def test_interruptible_outside_asyncio_timeout_interrupt_fires_first() -> None:
    async def main() -> list[object]:
        log: list[object] = []

        async def worker() -> None:
            try:
                async with sim.interruptible():
                    async with asyncio.timeout(10):
                        await sim.timeout(100)
            except Interrupt as exc:
                log.append(("interrupt", exc.cause, sim.now))
            except TimeoutError:
                log.append("timeout")
            task = asyncio.current_task()
            assert task is not None
            log.append(task.cancelling())

        w = sim.spawn(worker())
        await sim.timeout(1)
        sim.interrupt(w, "i")
        await w
        return log

    assert desyncio.run(main()) == [("interrupt", "i", 1.0), 0]


def test_asyncio_timeout_outside_interruptible_timeout_fires_first() -> None:
    async def main() -> list[object]:
        log: list[object] = []

        async def worker() -> None:
            try:
                async with asyncio.timeout(1):
                    async with sim.interruptible():
                        await sim.timeout(100)
            except Interrupt:
                log.append("interrupt")
            except TimeoutError:
                log.append(("timeout", sim.now))

        await sim.spawn(worker())
        return log

    assert desyncio.run(main()) == [("timeout", 1.0)]


# --- real cancel -----------------------------------------------------------


def test_real_cancel_alone_is_not_turned_into_interrupt() -> None:
    async def main() -> None:
        async def worker() -> None:
            async with sim.interruptible() as scope:
                try:
                    await sim.timeout(100)
                finally:
                    assert not scope.interrupted

        w = sim.spawn(worker())
        await sim.timeout(1)
        w.cancel()
        with pytest.raises(asyncio.CancelledError):
            await w
        assert w.cancelled()

    desyncio.run(main())


def test_cancel_then_interrupt_cancel_wins() -> None:
    async def main() -> None:
        async def worker() -> None:
            async with sim.interruptible():
                await sim.timeout(100)

        w = sim.spawn(worker())
        await sim.timeout(1)
        w.cancel()
        sim.interrupt(w, "late")  # same instant, before the task ran
        with pytest.raises(asyncio.CancelledError):
            await w
        assert w.cancelled()

    desyncio.run(main())


def test_interrupt_then_cancel_cancel_wins() -> None:
    async def main() -> list[object]:
        log: list[object] = []

        async def worker() -> None:
            scope = sim.interruptible()
            try:
                async with scope:
                    await sim.timeout(100)
            finally:
                task = asyncio.current_task()
                assert task is not None
                log.append((scope.interrupted, task.cancelling()))

        w = sim.spawn(worker())
        await sim.timeout(1)
        sim.interrupt(w, "first")
        w.cancel()
        with pytest.raises(asyncio.CancelledError):
            await w
        assert w.cancelled()
        return log

    # The real cancel is still counted (cancelling() == 1), the interrupt is not.
    assert desyncio.run(main()) == [(True, 1)]


def test_cancelling_restored_after_interrupt_with_prior_real_cancel_request() -> None:
    """A task already cancelling() == 1 on entry has the same value after."""

    async def main() -> None:
        result: list[int] = []

        async def worker() -> None:
            task = asyncio.current_task()
            assert task is not None
            task.cancel()  # a pending real cancel request (count 1)...
            try:
                await asyncio.sleep(0)
            except asyncio.CancelledError:
                pass  # ... which this task chose to swallow
            assert task.cancelling() == 1
            try:
                async with sim.interruptible():
                    await sim.timeout(100)
            except Interrupt:
                result.append(task.cancelling())

        w = sim.spawn(worker())
        await sim.timeout(1)
        sim.interrupt(w, None)
        await w
        assert result == [1]

    desyncio.run(main())


# --- several interrupts / delivery timing ---------------------------------


def test_several_interrupts_first_cause_wins() -> None:
    async def main() -> tuple[object, int]:
        out: list[object] = []

        async def worker() -> None:
            scope = sim.interruptible()
            try:
                async with scope:
                    await sim.timeout(100)
            except Interrupt as exc:
                out.append(exc.cause)
                out.append(scope.cause)
            task = asyncio.current_task()
            assert task is not None
            out.append(task.cancelling())

        w = sim.spawn(worker())
        await sim.timeout(1)
        sim.interrupt(w, "first")
        sim.interrupt(w, "second")
        sim.interrupt(w, "third")
        await w
        assert out[0] == out[1]
        return out[0], int(out[2])  # type: ignore[call-overload]

    assert desyncio.run(main()) == ("first", 0)


def test_interrupt_after_block_exit_needs_new_block() -> None:
    async def main() -> None:
        async def worker() -> None:
            async with sim.interruptible():
                await sim.timeout(1)
            await sim.timeout(5)  # outside any block

        w = sim.spawn(worker())
        await sim.timeout(2)
        with pytest.raises(RuntimeError):
            sim.interrupt(w, "late")
        await w

    desyncio.run(main())


def test_interrupt_delivered_at_current_await_same_time() -> None:
    """The target's wait already completed this instant (its wakeup is queued):
    the interrupt still wins and is delivered at that await."""

    async def main() -> list[object]:
        log: list[object] = []

        async def worker() -> None:
            try:
                async with sim.interruptible():
                    await sim.timeout(1)  # fires at t=1, same instant
                    log.append("not reached")
            except Interrupt as exc:
                log.append((exc.cause, sim.now))

        w = sim.spawn(worker())
        await sim.timeout(1)
        sim.interrupt(w, "x")
        await w
        return log

    # The main task's timer (registered first) fires first, and it interrupts
    # while the worker's own t=1 wakeup is still queued: the interrupt is
    # delivered at that await, so "not reached" is never logged.
    assert desyncio.run(main()) == [("x", 1.0)]


def test_interrupt_before_first_await_of_block() -> None:
    """Interrupter runs before the target task has started its block."""

    async def main() -> list[object]:
        log: list[object] = []
        ready = asyncio.Event()

        async def worker() -> None:
            try:
                async with sim.interruptible():
                    ready.set()
                    await asyncio.sleep(0)  # first await of the block
                    log.append("ran-past-first-await")
                    await sim.timeout(100)
            except Interrupt as exc:
                log.append((exc.cause, sim.now))

        w = sim.spawn(worker())
        await ready.wait()
        # The worker is parked at its first await; the wakeup is queued.
        sim.interrupt(w, "early")
        await w
        return log

    assert desyncio.run(main()) == [("early", 0.0)]


# --- RuntimeError cases ----------------------------------------------------


def test_runtime_error_task_not_in_block() -> None:
    async def main() -> None:
        w = sim.spawn(sim.timeout(5))
        await sim.timeout(1)
        with pytest.raises(RuntimeError):
            sim.interrupt(w, None)
        await w

    desyncio.run(main())


def test_runtime_error_task_not_started() -> None:
    async def main() -> None:
        w = sim.spawn(sim.timeout(5))
        with pytest.raises(RuntimeError):
            sim.interrupt(w, None)
        await w

    desyncio.run(main())


def test_runtime_error_finished_task() -> None:
    async def main() -> None:
        async def worker() -> None:
            async with sim.interruptible():
                await sim.timeout(1)

        w = sim.spawn(worker())
        await w
        with pytest.raises(RuntimeError):
            sim.interrupt(w, None)

    desyncio.run(main())


def test_runtime_error_current_task() -> None:
    async def main() -> None:
        async with sim.interruptible():
            me = asyncio.current_task()
            assert me is not None
            with pytest.raises(RuntimeError):
                sim.interrupt(me, None)

    desyncio.run(main())


def test_runtime_error_enter_twice_and_outside_task_misuse() -> None:
    async def main() -> None:
        scope = sim.interruptible()
        async with scope:
            pass
        with pytest.raises(RuntimeError):
            async with scope:
                pass
        busy = sim.interruptible()
        async with busy:
            with pytest.raises(RuntimeError):
                await busy.__aenter__()

    desyncio.run(main())


def test_runtime_error_enter_outside_task() -> None:
    async def main() -> None:
        scope = sim.interruptible()
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[None] = loop.create_future()

        def attempt() -> None:
            # Runs as a plain callback: there is no current task.
            coro = scope.__aenter__()
            try:
                coro.send(None)
            except RuntimeError as exc:
                fut.set_result(None)
                assert "task" in str(exc)
            except BaseException as exc:  # pragma: no cover
                fut.set_exception(exc)

        loop.call_soon(attempt)
        await fut

    desyncio.run(main())


# --- machine breakdown (D024) ----------------------------------------------


def test_machine_breakdown_worker_resumes_remaining_work() -> None:
    async def main() -> list[object]:
        log: list[object] = []

        async def worker() -> None:
            remaining = 10.0
            while remaining > 0:
                started = sim.now
                try:
                    async with sim.interruptible():
                        await sim.timeout(remaining)
                    remaining = 0
                except Interrupt as exc:
                    remaining -= sim.now - started
                    log.append(("broken", sim.now, exc.cause, remaining))
                    await sim.timeout(exc.cause)  # repair takes `cause` units
                    log.append(("repaired", sim.now))
            log.append(("done", sim.now))

        w = sim.spawn(worker())
        await sim.timeout(3)
        sim.interrupt(w, 2.0)
        await sim.timeout(4)  # t=7: work resumed at 5, 2 units done... interrupt
        sim.interrupt(w, 1.0)
        await w
        return log

    assert desyncio.run(main()) == [
        ("broken", 3.0, 2.0, 7.0),
        ("repaired", 5.0),
        ("broken", 7.0, 1.0, 5.0),
        ("repaired", 8.0),
        ("done", 13.0),
    ]


# --- determinism ------------------------------------------------------------


async def _chaos(n_workers: int = 6) -> dict[str, object]:
    """Workers use Resource, Container and Store inside interruptible blocks while
    a chaos task randomly interrupts and cancels them. Returns final invariants."""
    rng = sim.rng("chaos")
    res = Resource(2)
    cont = Container(capacity=20, init=10)
    store: Store[int] = Store(capacity=3)
    mirror_ok = True
    moved_in = 0.0
    moved_out = 0.0
    stats = {"interrupts": 0, "cancels": 0}

    def check_store(s: Store[int]) -> None:
        nonlocal mirror_ok
        mirror_ok &= len(s) == len(s.items)

    store = Store(capacity=3, monitor=check_store)

    async def step(wrng_val: float, kind: int) -> None:
        nonlocal moved_in, moved_out
        if kind == 0:
            async with res:
                await sim.timeout(wrng_val)
        elif kind == 1:
            amount = 1 + int(wrng_val) % 4
            await cont.get(amount)
            moved_out += amount
            await sim.timeout(wrng_val)
            await cont.put(amount)
            moved_in += amount
        elif kind == 2:
            await store.put(int(wrng_val))
        else:
            await store.get()

    async def worker(i: int) -> None:
        wrng = sim.rng(f"w{i}")
        while True:
            try:
                async with sim.interruptible():
                    await step(wrng.random() * 3, wrng.randrange(4))
            except Interrupt:
                pass
            await sim.timeout(wrng.random())

    tasks = [sim.spawn(worker(i)) for i in range(n_workers)]
    for _ in range(150):
        await sim.timeout(rng.random())
        victim = rng.choice(tasks)
        if victim.done():
            continue
        if rng.random() < 0.2:
            stats["cancels"] += 1
            victim.cancel()
            tasks[tasks.index(victim)] = sim.spawn(worker(99))
        else:
            try:
                sim.interrupt(victim, rng.random())
                stats["interrupts"] += 1
            except RuntimeError:
                pass  # not inside a block at this instant
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await sim.timeout(0)
    return {
        "res_count": res.count,
        "res_queue": res.queue_length,
        "level": cont.level,
        "store_len": len(store),
        "store_items": store.items,
        "mirror_ok": mirror_ok,
        "stats": stats,
    }


@pytest.mark.parametrize("seed", range(20))
def test_stress_invariants_hold(seed: int) -> None:
    out: Any = desyncio.run(_chaos(), seed=seed)
    assert out["res_count"] == 0 and out["res_queue"] == 0
    assert 0 <= out["level"] <= 20
    assert out["mirror_ok"]
    assert len(out["store_items"]) == out["store_len"] <= 3
    assert out["stats"]["interrupts"] + out["stats"]["cancels"] > 0


def test_stress_conserves_container_amount() -> None:
    """Every get is matched by a put unless its task was stopped in between, so
    the level equals init minus what was taken and never returned; verify it
    stays within [0, capacity] and sums are consistent for every seed."""

    async def main(seed_level: float) -> float:
        c = Container(capacity=50, init=seed_level)
        total_in = 0.0
        total_out = 0.0

        async def worker(i: int) -> None:
            nonlocal total_in, total_out
            r = sim.rng(f"c{i}")
            while True:
                amount = float(r.randint(1, 5))
                try:
                    async with sim.interruptible():
                        if r.random() < 0.5:
                            await c.put(amount)
                            total_in += amount
                        else:
                            await c.get(amount)
                            total_out += amount
                        await sim.timeout(r.random())
                except Interrupt:
                    pass

        tasks = [sim.spawn(worker(i)) for i in range(5)]
        chaos = sim.rng("chaos")
        for _ in range(200):
            await sim.timeout(chaos.random() * 0.5)
            t = chaos.choice(tasks)
            try:
                sim.interrupt(t, None)
            except RuntimeError:
                pass
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        assert c.level == pytest.approx(seed_level + total_in - total_out)
        assert 0 <= c.level <= 50
        return c.level

    for seed in range(10):
        desyncio.run(main(25.0), seed=seed)


def test_same_seed_gives_identical_trace() -> None:
    def run_once(seed: int) -> tuple[list[tuple[object, ...]], object]:
        sink = ListSink()
        out = desyncio.run(_chaos(), seed=seed, trace=sink)
        return [tuple(r) for r in sink.records], out

    first = run_once(5)
    assert len(first[0]) > 100
    assert run_once(5) == first
    assert run_once(6) != first


# --- licensing ---------------------------------------------------------


def test_cpython_adapted_code_carries_psf_notice() -> None:
    """AGENTS.md §3: the code adapted from ``asyncio.Timeout`` keeps the PSF notice."""
    import inspect
    from pathlib import Path

    import desyncio.interrupt as interrupt_module

    notice = Path(__file__).resolve().parent.parent / "LICENSES" / "PSF.txt"
    assert notice.is_file()
    text = notice.read_text(encoding="utf-8")
    assert "PYTHON SOFTWARE FOUNDATION LICENSE VERSION 2" in text
    header = text.split("Copyright (c) 2001 Python Software Foundation", 1)[0]
    assert "src/desyncio/interrupt.py" in header

    assert "LICENSES/PSF.txt" in inspect.getsource(interrupt_module)
