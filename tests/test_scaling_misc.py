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
* Element counts (deterministic): a quadratic cost that compares nothing
  (rebuilding the deque or the dict on every cancel, compacting too eagerly)
  is invisible to the comparison count, so a resource subclass counts the
  elements of its FIFO and grant-order dict that are iterated or copied into
  a rebuilt container, for n and 4n cancelled tasks. Linear work gives a
  ratio of about 4, quadratic about 16; the threshold, 8, is 2x from both.
  These replace CPU-time ratios, which were flaky on a loaded machine.
* Memory: lazy deletion must not let stale entries pile up while the head of
  the line is never served.
"""

import random
from collections import deque
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
TOUCH_N = 500  # compared with 4 * TOUCH_N
TOUCH_THRESHOLD = 8.0  # linear: ~4, quadratic: ~16


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
    """A schedule whose capacity is ``capacity`` around the clock, every day.

    Capacity 0 is a schedule with no shifts (a ``Shift`` needs capacity >= 1).
    """
    shifts = [Shift(range(7), dtime(0), dtime(0), capacity)] if capacity else []
    return Schedule(Calendar(datetime(2026, 1, 5, tzinfo=UTC)), shifts)


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
    # N waiters on a closed resource; ``run`` cancels all of them after
    # ``until``. Capacity 0, so no release at teardown grants and pops the
    # cancelled waiters off the front before they leave by themselves (with a
    # holder, its release would, and the runner picks the cancel order by
    # task address). The waiters queue in a shuffled order, so whatever order
    # the runner cancels them in, a scan from the front would compare ~N²/4.
    seen: list[int] = []
    before = -1

    async def wait_then_hold(res: ScheduledResource, delay: float) -> None:
        await sim.timeout(delay)
        await _hold(res)

    async def main() -> None:
        nonlocal before
        res = ScheduledResource(constant(0))
        delays = [i / N for i in range(N)]
        random.Random(0).shuffle(delays)
        for delay in delays:
            sim.spawn(wait_then_hold(res, delay))
        await sim.timeout(1)
        seen.extend((res.count, res.queue_length))
        before = comparisons[0]
        await sim.timeout(10)

    desyncio.run(main(), until=2)
    made = comparisons[0] - before
    assert seen == [0, N]
    assert made < N, f"{made} comparisons to tear down {N} waiters"


class _CountingDeque(deque[_Hold]):
    """A deque that counts the elements its iterator yields."""

    touched: list[int]

    def __iter__(self) -> Iterator[_Hold]:
        for hold in super().__iter__():
            self.touched[0] += 1
            yield hold


class _CountingDict(dict[_Hold, None]):
    """A dict that counts the keys its iterator yields."""

    touched: list[int]

    def __iter__(self) -> Iterator[_Hold]:
        for hold in super().__iter__():
            self.touched[0] += 1
            yield hold


class _CountingResource(ScheduledResource):
    """A ``ScheduledResource`` that counts the elements of its waiter FIFO and
    grant-order record that are iterated, or copied into a new container.

    ``_waiters`` and ``_units`` are properties here: whatever deque or dict the
    resource stores in them (at creation, or a rebuild later) is wrapped in a
    counting copy, and every rebuild adds its length to the count. Only
    iteration and rebuilds count; O(1) operations (``append``, ``popleft``,
    ``del d[k]``, ``popitem``, ``len``) add nothing.
    """

    touched: list[int]

    def _store(self, name: str, value: object) -> None:
        if not hasattr(self, "touched"):
            self.touched = [0]
        else:
            self.touched[0] += len(value)  # type: ignore[arg-type]
        if type(value) is deque:
            value = _CountingDeque(value)
        elif type(value) is dict:
            value = _CountingDict(value)
        if isinstance(value, _CountingDeque | _CountingDict):
            value.touched = self.touched
        self.__dict__[name] = value

    @property
    def _waiters(self) -> deque[_Hold]:
        return self.__dict__["_waiters"]  # type: ignore[no-any-return]

    @_waiters.setter
    def _waiters(self, value: deque[_Hold]) -> None:
        self._store("_waiters", value)

    @property
    def _units(self) -> dict[_Hold, None]:
        return self.__dict__["_units"]  # type: ignore[no-any-return]

    @_units.setter
    def _units(self, value: dict[_Hold, None]) -> None:
        self._store("_units", value)


def _cancel_touches(n: int, capacity: int) -> int:
    """Elements touched (see ``_CountingResource``) while n tasks on one
    resource are cancelled in reverse creation order, mid-run."""
    made = -1

    async def main() -> None:
        nonlocal made
        res = _CountingResource(constant(capacity))
        tasks = [sim.spawn(_hold(res)) for _ in range(n)]
        await sim.timeout(1)
        assert res.count + res.queue_length == n
        assert isinstance(res._waiters, _CountingDeque)
        assert isinstance(res._units, _CountingDict)
        before = res.touched[0]
        for task in reversed(tasks):
            task.cancel()
        await sim.timeout(1)
        made = res.touched[0] - before
        assert res.count == 0
        assert res.queue_length == 0

    desyncio.run(main())
    return made


@pytest.mark.parametrize("capacity", [1, 10**9], ids=["waiters", "holders"])
def test_cancelling_touches_scale_linearly(capacity: int) -> None:
    # Capacity 1: one holder and n - 1 waiters; capacity 10**9: n holders.
    # Deterministic counts: linear work gives a ratio of about 4, a rebuild of
    # the deque or the dict on every cancel (or compaction that is too eager)
    # about 16. The threshold, 8, is 2x from both.
    small = _cancel_touches(TOUCH_N, capacity)
    large = _cancel_touches(4 * TOUCH_N, capacity)
    ratio = large / max(small, 1)
    assert ratio < TOUCH_THRESHOLD, (
        f"cancelling {4 * TOUCH_N} tasks touched {ratio:.1f}x the elements "
        f"of {TOUCH_N} ({large} vs {small}): not linear"
    )
    assert large <= 4 * 4 * TOUCH_N, f"{large} elements touched for {4 * TOUCH_N}"


def test_stale_waiters_are_compacted() -> None:
    # The head waiter is never served (the holder keeps the only unit), so the
    # grant never pops stale entries; compaction alone must bound the deque.
    # Memory is checked through the private ``_waiters`` on purpose: stale
    # entries are invisible in the public API (``queue_length`` counts only
    # live waiters), so no public observation can see them pile up.
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
