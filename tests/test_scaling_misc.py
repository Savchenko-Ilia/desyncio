"""Scaling regression tests for ``ScheduledResource`` (PERF-03, D075).

Cancelling n tasks parked on one ``ScheduledResource`` used to cost O(n²): a
cancelled waiter left the FIFO with ``deque.remove`` and a released holder left
the grant-order list with ``list.remove``. Both are linear scans that compare
the wanted hold with every hold in front of it using ``==``.

Three kinds of check:

* Comparison counts (deterministic): the tests count calls of ``_Hold.__eq__``
  (a counting version patched in), which is exactly the work those scans do.
  With the scans, cancelling n tasks in reverse creation order costs about
  n²/2 comparisons; now a hold leaves its queue or the grant-order dict
  without any, so the count must stay below n.
* CPU-time ratio: a quadratic cost that compares nothing (rebuilding the deque
  or the dict on every cancel, compacting too eagerly) is invisible to the
  count, so the CPU time of the same cancellation is compared between n and
  4n tasks. Linear work gives a ratio of about 4, quadratic about 16; the
  threshold, 8, is 2x from both. Each size is measured 3 times and the
  fastest kept, which removes most scheduler noise (noise only adds time);
  the cyclic GC is paused while timing.
* Memory: lazy deletion must not let stale entries pile up while the head of
  the line is never served.
"""

import gc
import time
from collections.abc import Iterator
from datetime import UTC, datetime
from datetime import time as dtime

import pytest

import desyncio
from desyncio import sim
from desyncio.dates import Calendar
from desyncio.schedule import Schedule, Shift
from desyncio.scheduled import ScheduledResource, _Hold

N = 400
RATIO_N = 2_000  # compared with 4 * RATIO_N
RATIO_THRESHOLD = 8.0  # linear: ~4, quadratic: ~16
REPEATS = 3


@pytest.fixture
def comparisons(monkeypatch: pytest.MonkeyPatch) -> Iterator[list[int]]:
    """Count ``_Hold.__eq__`` calls (identity semantics are kept)."""
    count = [0]

    def counting_eq(self: _Hold, other: object) -> bool:
        count[0] += 1
        return self is other

    # Setting __eq__ after class creation keeps the identity-based __hash__.
    monkeypatch.setattr(_Hold, "__eq__", counting_eq)
    yield count


def constant(capacity: int) -> Schedule:
    """A schedule whose capacity is ``capacity`` around the clock, every day."""
    return Schedule(
        Calendar(datetime(2026, 1, 5, tzinfo=UTC)),
        [Shift(range(7), dtime(0), dtime(0), capacity)],
    )


async def _hold(res: ScheduledResource) -> None:
    async with res:
        await sim.timeout(10**9)


def _mid_run_cancel(n: int, capacity: int, comparisons: list[int]) -> int:
    """Comparisons made while n tasks on one resource are cancelled and leave."""
    made = -1

    async def main() -> None:
        nonlocal made
        res = ScheduledResource(constant(capacity))
        tasks = [sim.spawn(_hold(res)) for _ in range(n)]
        await sim.timeout(1)
        assert res.count + res.queue_length == n
        before = comparisons[0]
        # Reverse creation order: the worst case for a scan from the front.
        for task in reversed(tasks):
            task.cancel()
        await sim.timeout(1)
        made = comparisons[0] - before
        assert res.count == 0
        assert res.queue_length == 0

    desyncio.run(main())
    return made


def test_cancelling_waiters_makes_no_scan(comparisons: list[int]) -> None:
    # Capacity 1: one holder, everyone else waits in the FIFO.
    made = _mid_run_cancel(N, 1, comparisons)
    assert made < N, f"{made} comparisons to cancel {N} waiters"


def test_cancelling_holders_makes_no_scan(comparisons: list[int]) -> None:
    # Capacity above n: everyone holds a unit, in grant order.
    made = _mid_run_cancel(N, 10**9, comparisons)
    assert made < N, f"{made} comparisons to cancel {N} holders"


def test_teardown_after_until_makes_no_scan(comparisons: list[int]) -> None:
    # Half hold a unit and half wait; ``run`` cancels all of them after ``until``.
    seen: list[int] = []
    before = -1

    async def main() -> None:
        nonlocal before
        res = ScheduledResource(constant(N // 2))
        for _ in range(N):
            sim.spawn(_hold(res))
        await sim.timeout(1)
        seen.extend((res.count, res.queue_length))
        before = comparisons[0]
        await sim.timeout(10)

    desyncio.run(main(), until=2)
    made = comparisons[0] - before
    assert seen == [N // 2, N // 2]
    assert made < N, f"{made} comparisons to tear down {N} tasks"


def _cancel_cpu_time(n: int, capacity: int) -> float:
    """CPU seconds to cancel n tasks on one resource in reverse order, mid-run."""
    spent = -1.0

    async def main() -> None:
        nonlocal spent
        res = ScheduledResource(constant(capacity))
        tasks = [sim.spawn(_hold(res)) for _ in range(n)]
        await sim.timeout(1)
        assert res.count + res.queue_length == n
        # A cyclic GC pass costs time in proportion to every live object, not
        # to the work measured, so it is kept out of the timed part.
        gc.collect()
        gc.disable()
        try:
            start = time.process_time()
            for task in reversed(tasks):
                task.cancel()
            await sim.timeout(1)
            spent = time.process_time() - start
        finally:
            gc.enable()
        assert res.count == 0
        assert res.queue_length == 0

    desyncio.run(main())
    return spent


@pytest.mark.parametrize("capacity", [1, 10**9], ids=["waiters", "holders"])
def test_cancelling_scales_linearly_in_cpu_time(capacity: int) -> None:
    # Capacity 1: one holder and n - 1 waiters; capacity 10**9: n holders.
    # Interleaved, so a burst of load on the machine hits both sizes alike.
    smalls: list[float] = []
    larges: list[float] = []
    for _ in range(REPEATS):
        smalls.append(_cancel_cpu_time(RATIO_N, capacity))
        larges.append(_cancel_cpu_time(4 * RATIO_N, capacity))
    small, large = min(smalls), min(larges)
    ratio = large / max(small, 1e-6)
    assert ratio < RATIO_THRESHOLD, (
        f"cancelling {4 * RATIO_N} tasks took {ratio:.1f}x the CPU time of "
        f"{RATIO_N} ({large:.3f}s vs {small:.3f}s): not linear"
    )


def test_stale_waiters_are_compacted() -> None:
    # The head waiter is never served (the holder keeps the only unit), so the
    # grant never pops stale entries; compaction alone must bound the deque.
    stale: list[int] = []

    async def main() -> None:
        res = ScheduledResource(constant(1))
        holder = sim.spawn(_hold(res))
        await sim.timeout(1)
        waiters = [sim.spawn(_hold(res)) for _ in range(N)]
        await sim.timeout(1)
        assert res.count == 1
        assert res.queue_length == N
        # Cancel all but the first ten, from the back of the line.
        for task in reversed(waiters[10:]):
            task.cancel()
        await sim.timeout(1)
        assert res.queue_length == 10
        stale.extend((len(res._waiters), res.queue_length))
        holder.cancel()

    desyncio.run(main())
    kept, live = stale
    assert kept <= 2 * live + 16, f"{kept} entries kept for {live} live waiters"
