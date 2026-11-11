"""Scaling regression tests for ``ScheduledResource`` (PERF-03, D075).

Cancelling n tasks parked on one ``ScheduledResource`` used to cost O(n²): a
cancelled waiter left the FIFO with ``deque.remove`` and a released holder left
the grant-order list with ``list.remove``. Both are linear scans that compare
the wanted hold with every hold in front of it using ``==``.

The measure is deterministic: the tests count calls of ``_Hold.__eq__`` (a
counting version patched in), which is exactly the work those scans do. With
the scans, cancelling n tasks in reverse creation order costs about n²/2
comparisons; now a hold leaves its queue or the grant-order dict without any,
so the count must stay below n.
"""

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
