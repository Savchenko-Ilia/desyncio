"""LOOP-06: stock asyncio primitives behave correctly in virtual time.

Every test runs a small model through the public API (``desyncio.run`` or
``asyncio.Runner(loop_factory=VirtualTimeLoop)``) and checks *when*, in
virtual seconds, each thing happened, not just what the result was.

Conventions used throughout:

* ``sim.now`` and ``asyncio.get_running_loop().time()`` are the same clock.
* Events are recorded as ``(virtual_time, label)`` tuples and compared after
  the run, so a failure shows the whole timeline.
* Times are small whole numbers, which are exact in floating point, so we
  compare with ``==``.
* We never assert the order of two things that land at the same instant unless
  asyncio (or desyncio's timer tie-break, D008) guarantees it.
* These are plain sync tests: ``desyncio.run`` makes its own event loop and
  cannot be called from inside pytest-asyncio's running loop.
"""

import asyncio
from typing import Any

import pytest

import desyncio
from desyncio import sim

Log = list[tuple[float, str]]


def now() -> float:
    """Current virtual time, read from the running loop."""
    return asyncio.get_running_loop().time()


# ---------------------------------------------------------------------------
# The clock
# ---------------------------------------------------------------------------


def test_sim_now_is_the_loop_clock() -> None:
    """sim.now equals loop.time() throughout; asyncio.sleep(2.5) and then
    sim.timeout(2) move both to t=4.5, so they share one clock."""

    async def main() -> list[tuple[float, float]]:
        out = [(sim.now, now())]
        await asyncio.sleep(2.5)
        out.append((sim.now, now()))
        await sim.timeout(2)
        out.append((sim.now, now()))
        return out

    assert desyncio.run(main()) == [(0.0, 0.0), (2.5, 2.5), (4.5, 4.5)]


# ---------------------------------------------------------------------------
# Queue
# ---------------------------------------------------------------------------


def test_queue_get_blocks_until_put() -> None:
    """A get() on an empty Queue suspends from t=0 and resumes exactly when put()
    runs at t=3.
    """

    async def main() -> tuple[float, str]:
        q: asyncio.Queue[str] = asyncio.Queue()

        async def producer() -> None:
            await asyncio.sleep(3)
            q.put_nowait("x")

        task = asyncio.create_task(producer())
        item = await q.get()
        await task
        return now(), item

    assert desyncio.run(main()) == (3.0, "x")


def test_queue_put_blocks_when_full_until_get() -> None:
    """put() on a full Queue(maxsize=1) blocks at t=0 and completes when a get()
    frees space at t=4.
    """

    async def main() -> Log:
        q: asyncio.Queue[int] = asyncio.Queue(maxsize=1)
        log: Log = []

        async def consumer() -> None:
            await asyncio.sleep(4)
            log.append((now(), f"got {await q.get()}"))

        await q.put(1)
        log.append((now(), "put 1"))
        task = asyncio.create_task(consumer())
        await q.put(2)  # blocks: queue is full
        log.append((now(), "put 2"))
        await task
        return log

    log = desyncio.run(main())
    assert (0.0, "put 1") in log
    assert (4.0, "got 1") in log
    assert (4.0, "put 2") in log


def test_queue_is_fifo_for_waiting_getters() -> None:
    """Getters blocked from t=0 are served in arrival order as items arrive at t=1
    and t=2.
    """

    async def main() -> Log:
        q: asyncio.Queue[str] = asyncio.Queue()
        log: Log = []

        async def getter(name: str) -> None:
            item = await q.get()
            log.append((now(), f"{name}:{item}"))

        t1 = asyncio.create_task(getter("a"))
        await asyncio.sleep(0)  # let a block first
        t2 = asyncio.create_task(getter("b"))
        await asyncio.sleep(1)
        q.put_nowait("one")
        await asyncio.sleep(1)
        q.put_nowait("two")
        await asyncio.gather(t1, t2)
        return log

    assert desyncio.run(main()) == [(1.0, "a:one"), (2.0, "b:two")]


def test_queue_shutdown_wakes_blocked_getter_at_shutdown_time() -> None:
    """A getter blocked since t=0 is woken with QueueShutDown at the instant
    shutdown() runs (t=6).
    """

    async def main() -> float:
        q: asyncio.Queue[int] = asyncio.Queue()

        async def getter() -> float:
            with pytest.raises(asyncio.QueueShutDown):
                await q.get()
            return now()

        task = asyncio.create_task(getter())
        await asyncio.sleep(6)
        q.shutdown()
        return await task

    assert desyncio.run(main()) == 6.0


def test_queue_shutdown_immediate_drains_and_unblocks_join() -> None:
    """shutdown(immediate=True) at t=5 empties the queue, raises in get(), and
    releases join() at t=5.
    """

    async def main() -> tuple[float, int, float]:
        q: asyncio.Queue[int] = asyncio.Queue()
        q.put_nowait(1)
        q.put_nowait(2)

        async def waiter() -> float:
            await q.join()  # would block forever: nothing calls task_done
            return now()

        w = asyncio.create_task(waiter())
        await asyncio.sleep(5)
        q.shutdown(immediate=True)
        with pytest.raises(asyncio.QueueShutDown):
            await q.get()
        return await w, q.qsize(), now()

    assert desyncio.run(main()) == (5.0, 0, 5.0)


def test_queue_shutdown_wakes_blocked_putter() -> None:
    """A put() blocked on a full queue since t=0 raises QueueShutDown at t=3 when
    shutdown() runs.
    """

    async def main() -> float:
        q: asyncio.Queue[int] = asyncio.Queue(maxsize=1)
        q.put_nowait(0)

        async def putter() -> float:
            with pytest.raises(asyncio.QueueShutDown):
                await q.put(1)
            return now()

        task = asyncio.create_task(putter())
        await asyncio.sleep(3)
        q.shutdown(immediate=True)
        return await task

    assert desyncio.run(main()) == 3.0


# ---------------------------------------------------------------------------
# Lock, Event, Semaphore
# ---------------------------------------------------------------------------


def test_lock_serialises_critical_sections_in_virtual_time() -> None:
    """Three tasks each hold a Lock for 2s; they get it in FIFO order at t=0, 2, 4."""

    async def main() -> Log:
        lock = asyncio.Lock()
        log: Log = []

        async def worker(i: int) -> None:
            async with lock:
                log.append((now(), f"w{i}"))
                await asyncio.sleep(2)

        tasks = []
        for i in range(3):
            tasks.append(asyncio.create_task(worker(i)))
            await asyncio.sleep(0)  # fix arrival order: Lock waiters are FIFO
        await asyncio.gather(*tasks)
        return log

    assert desyncio.run(main()) == [(0.0, "w0"), (2.0, "w1"), (4.0, "w2")]


def test_event_wakes_all_waiters_at_set_time() -> None:
    """Three waiters blocked from t=0 on an Event all resume at t=7 when it is set."""

    async def main() -> list[float]:
        ev = asyncio.Event()
        times: list[float] = []

        async def waiter() -> None:
            await ev.wait()
            times.append(now())

        tasks = [asyncio.create_task(waiter()) for _ in range(3)]
        await asyncio.sleep(7)
        ev.set()
        await asyncio.gather(*tasks)
        return times

    assert desyncio.run(main()) == [7.0, 7.0, 7.0]


def test_semaphore_limits_concurrency_in_virtual_time() -> None:
    """Semaphore(2) with four 10s jobs: two start at t=0 and two at t=10, all done
    by t=20.
    """

    async def main() -> tuple[dict[int, float], float]:
        sem = asyncio.Semaphore(2)
        started: dict[int, float] = {}

        async def job(i: int) -> None:
            async with sem:
                started[i] = now()
                await asyncio.sleep(10)

        await asyncio.gather(*(job(i) for i in range(4)))
        return started, now()

    started, end = desyncio.run(main())
    # gather starts jobs in argument order and Semaphore waiters are FIFO.
    assert started == {0: 0.0, 1: 0.0, 2: 10.0, 3: 10.0}
    assert end == 20.0


# ---------------------------------------------------------------------------
# gather
# ---------------------------------------------------------------------------


def test_gather_finishes_with_slowest_child_and_keeps_argument_order() -> None:
    """gather of sleeps 3, 1, 2 returns at t=3 with results in argument order, not
    finish order.
    """

    async def work(delay: float) -> float:
        await asyncio.sleep(delay)
        return delay

    async def main() -> tuple[list[float], float]:
        results = await asyncio.gather(work(3), work(1), work(2))
        return list(results), now()

    assert desyncio.run(main()) == ([3, 1, 2], 3.0)


@pytest.mark.parametrize("return_exceptions", [False, True])
def test_gather_with_a_failing_child(return_exceptions: bool) -> None:
    """A child fails at t=1, its sibling finishes at t=5. Without
    return_exceptions gather raises at t=1 and does NOT cancel the sibling; with
    it, gather waits and returns both outcomes at t=5."""

    async def main() -> tuple[float, float, list[Any]]:
        finished: list[float] = []

        async def slow() -> str:
            await asyncio.sleep(5)
            finished.append(now())
            return "ok"

        async def bad() -> str:
            await asyncio.sleep(1)
            raise ValueError("boom")

        slow_task = asyncio.create_task(slow())
        results: list[Any] = []
        if return_exceptions:
            results = list(
                await asyncio.gather(slow_task, bad(), return_exceptions=True)
            )
        else:
            with pytest.raises(ValueError):
                await asyncio.gather(slow_task, bad())
        returned_at = now()
        await slow_task
        return returned_at, finished[0], results

    returned_at, finished_at, results = desyncio.run(main())
    assert finished_at == 5.0
    if return_exceptions:
        assert returned_at == 5.0
        assert results[0] == "ok" and isinstance(results[1], ValueError)
    else:
        assert returned_at == 1.0


def test_gather_cancellation_propagates_to_children_at_cancel_time() -> None:
    """Cancelling a task awaiting gather at t=2 cancels both children at t=2 and
    the task ends at t=2.
    """

    async def main() -> tuple[Log, float]:
        log: Log = []

        async def child(name: str, delay: float) -> None:
            try:
                await asyncio.sleep(delay)
            except asyncio.CancelledError:
                log.append((now(), f"{name} cancelled"))
                raise

        async def parent() -> None:
            await asyncio.gather(child("a", 10), child("b", 20))

        task = asyncio.create_task(parent())
        await asyncio.sleep(2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        return log, now()

    log, end = desyncio.run(main())
    assert sorted(log) == [(2.0, "a cancelled"), (2.0, "b cancelled")]
    assert end == 2.0


# ---------------------------------------------------------------------------
# TaskGroup
# ---------------------------------------------------------------------------


def test_taskgroup_error_arrives_as_exception_group() -> None:
    """Two children failing at t=2 arrive together as one ExceptionGroup raised at
    t=2.
    """

    async def fail(exc: Exception) -> None:
        await asyncio.sleep(2)
        raise exc

    async def main() -> tuple[ExceptionGroup[Exception], float]:
        try:
            async with asyncio.TaskGroup() as tg:
                tg.create_task(fail(ValueError("v")))
                tg.create_task(fail(KeyError("k")))
        except ExceptionGroup as eg:
            return eg, now()
        raise AssertionError("TaskGroup did not raise")

    eg, end = desyncio.run(main())
    assert end == 2.0
    assert {type(e) for e in eg.exceptions} == {ValueError, KeyError}


@pytest.mark.parametrize("source", ["child_fails", "body_fails"])
def test_taskgroup_failure_cancels_siblings_at_that_time(source: str) -> None:
    """A failure at t=3, from a child or from the `async with` body, cancels a
    sibling sleeping until t=100 at t=3; the group exits at t=3."""

    async def main() -> tuple[Log, float, list[BaseException]]:
        log: Log = []

        async def sibling() -> None:
            try:
                await asyncio.sleep(100)
            except asyncio.CancelledError:
                log.append((now(), "sibling cancelled"))
                raise

        async def failing() -> None:
            await asyncio.sleep(3)
            raise ValueError("boom")

        caught: list[BaseException] = []
        try:
            async with asyncio.TaskGroup() as tg:
                tg.create_task(sibling())
                if source == "child_fails":
                    tg.create_task(failing())
                else:
                    await failing()
        except* ValueError as eg:
            caught.extend(eg.exceptions)
        return log, now(), caught

    log, end, caught = desyncio.run(main())
    assert log == [(3.0, "sibling cancelled")]
    assert end == 3.0
    assert len(caught) == 1 and isinstance(caught[0], ValueError)


# ---------------------------------------------------------------------------
# as_completed (async iterator, 3.13+)
# ---------------------------------------------------------------------------


def test_as_completed_async_iterator_yields_in_completion_order() -> None:
    """`async for` over as_completed gives sleeps 3, 1, 2 back at t=1, 2, 3
    (completion order).
    """

    async def work(delay: float) -> float:
        await asyncio.sleep(delay)
        return delay

    async def main() -> tuple[Log, float]:
        log: Log = []
        async for fut in asyncio.as_completed([work(3), work(1), work(2)]):
            log.append((now(), f"r{await fut:g}"))
        return log, now()

    log, end = desyncio.run(main())
    assert log == [(1.0, "r1"), (2.0, "r2"), (3.0, "r3")]
    assert end == 3.0


def test_as_completed_timeout_raises_at_deadline() -> None:
    """as_completed(timeout=2) over 1s and 5s work yields one result at t=1, then
    TimeoutError at t=2.
    """

    async def work(delay: float) -> float:
        await asyncio.sleep(delay)
        return delay

    async def main() -> tuple[Log, float]:
        log: Log = []
        with pytest.raises(TimeoutError):
            async for fut in asyncio.as_completed([work(1), work(5)], timeout=2):
                log.append((now(), f"r{await fut:g}"))
        return log, now()

    assert desyncio.run(main()) == ([(1.0, "r1")], 2.0)


# ---------------------------------------------------------------------------
# wait_for
# ---------------------------------------------------------------------------


def test_wait_for_times_out_at_the_deadline_and_cancels_work() -> None:
    """wait_for(sleep(10), timeout=4) raises TimeoutError at t=4 and cancels the
    inner work at t=4.
    """

    async def main() -> tuple[float, float]:
        cancelled_at: list[float] = []

        async def work() -> None:
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                cancelled_at.append(now())
                raise

        with pytest.raises(TimeoutError):
            await asyncio.wait_for(work(), timeout=4)
        return now(), cancelled_at[0]

    assert desyncio.run(main()) == (4.0, 4.0)


# ---------------------------------------------------------------------------
# asyncio.timeout() / timeout_at()
# ---------------------------------------------------------------------------


def test_timeout_context_expires_at_deadline() -> None:
    """asyncio.timeout(5) around a 20s sleep raises TimeoutError at t=5 and reports
    expired().
    """

    async def main() -> tuple[float, bool]:
        with pytest.raises(TimeoutError):
            async with asyncio.timeout(5) as cm:
                await asyncio.sleep(20)
        return now(), cm.expired()

    assert desyncio.run(main()) == (5.0, True)


def test_timeout_context_does_not_fire_if_body_finishes_first() -> None:
    """asyncio.timeout(5) around 2s work exits at t=2 without expiring."""

    async def main() -> tuple[float, bool]:
        async with asyncio.timeout(5) as cm:
            await asyncio.sleep(2)
        return now(), cm.expired()

    assert desyncio.run(main()) == (2.0, False)


def test_timeout_at_uses_absolute_virtual_deadline() -> None:
    """timeout_at(7) entered at t=3 fires at absolute t=7, i.e. after 4 more seconds."""

    async def main() -> float:
        await asyncio.sleep(3)
        with pytest.raises(TimeoutError):
            async with asyncio.timeout_at(7):
                await asyncio.sleep(100)
        return now()

    assert desyncio.run(main()) == 7.0


@pytest.mark.parametrize(
    ("initial", "new_deadline", "end", "expired"),
    [
        (5, 11, 11.0, True),  # later: fires at the new deadline, not t=5
        (50, 4, 4.0, True),  # earlier: shortened
        (5, None, 31.0, False),  # None: never fires, the 30s sleep completes
        (50, 0.5, 1.0, True),  # into the past: expires at once, at t=1
    ],
)
def test_timeout_reschedule(
    initial: float, new_deadline: float | None, end: float, expired: bool
) -> None:
    """A timeout(initial) rescheduled at t=1 ends the 30s sleep at `end`."""

    async def main() -> tuple[float, bool]:
        try:
            async with asyncio.timeout(initial) as cm:
                await asyncio.sleep(1)
                cm.reschedule(new_deadline)
                assert cm.when() == new_deadline
                await asyncio.sleep(30)
        except TimeoutError:
            pass
        return now(), cm.expired()

    assert desyncio.run(main()) == (end, expired)


# ---------------------------------------------------------------------------
# Cancellation landing
# ---------------------------------------------------------------------------


def test_task_cancel_delivers_cancelled_error_at_cancel_time() -> None:
    """task.cancel() called at t=7 raises CancelledError inside a 1000s sleep at
    exactly t=7.
    """

    async def main() -> tuple[float, float]:
        landed: list[float] = []

        async def victim() -> None:
            try:
                await asyncio.sleep(1000)
            except asyncio.CancelledError:
                landed.append(now())
                raise

        task = asyncio.create_task(victim())
        await asyncio.sleep(7)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert task.cancelled()
        return landed[0], now()

    assert desyncio.run(main()) == (7.0, 7.0)


def test_cancel_handler_can_take_virtual_time() -> None:
    """A task cancelled at t=3 that cleans up with sleep(2) finishes its cleanup at
    t=5.
    """

    async def main() -> tuple[Log, float]:
        log: Log = []

        async def victim() -> None:
            try:
                await asyncio.sleep(1000)
            except asyncio.CancelledError:
                log.append((now(), "cancelled"))
                await asyncio.sleep(2)  # cleanup takes virtual time
                log.append((now(), "cleaned up"))
                raise

        task = asyncio.create_task(victim())
        await asyncio.sleep(3)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        return log, now()

    assert desyncio.run(main()) == ([(3.0, "cancelled"), (5.0, "cleaned up")], 5.0)


@pytest.mark.parametrize("how", ["cancel_waiter", "wait_for_timeout"])
def test_shield_inner_work_survives_the_outer_being_stopped(how: str) -> None:
    """The waiter on shield(inner) is stopped at t=2, by task.cancel() or by a
    wait_for timeout. It sees CancelledError / TimeoutError at t=2; the inner
    task is not cancelled and still finishes at t=5."""

    async def main() -> tuple[float, float, str]:
        async def inner() -> str:
            await asyncio.sleep(5)
            return "inner done"

        inner_task = asyncio.create_task(inner())

        if how == "cancel_waiter":

            async def outer() -> None:
                await asyncio.shield(inner_task)

            outer_task = asyncio.create_task(outer())
            await asyncio.sleep(2)
            outer_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await outer_task
        else:
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(asyncio.shield(inner_task), timeout=2)
        stopped_at = now()
        assert not inner_task.done()
        result = await inner_task
        return stopped_at, now(), result

    assert desyncio.run(main()) == (2.0, 5.0, "inner done")
