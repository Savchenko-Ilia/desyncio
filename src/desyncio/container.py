"""Container: a level of homogeneous stuff (fuel, water, money) with blocking put/get.

asyncio has no primitive for "wait until an amount is available". Until batch
34 ``Container`` was built on the closest stock piece, ``asyncio.Condition``
(``wait_for`` plus ``notify_all``). It now parks its waiters itself, on asyncio
futures, because the ``Condition`` made cancellation quadratic (D075, D076).

Why not ``asyncio.Condition`` (D075)
-----------------------------------
A waiter cancelled inside ``Condition.wait`` removes its future with
``deque.remove`` (a linear search), then calls ``Condition._notify(1)`` to
pass a possible notification on, which walks the deque from the head past
every future that is already done; and our cleanup then called
``notify_all()``, another walk of the whole deque. Cancelling n parked waiters
at once (the run ends at ``until``, a ``TaskGroup`` aborts, a mass interrupt)
cancels all n futures before any cleanup runs, so each cleanup walks past up
to n cancelled futures: O(n^2), about 117 s for 50,000 waiters. Besides,
``notify_all`` on every change woke every parked waiter, O(n) per put or get,
although only the head of each line can proceed. desyncio cannot change
asyncio, so AGENTS.md principle 6 allows this exception (D075), and
``tests/test_scaling_container.py`` pins the linear cost.

How waiting works
-----------------
Delegated to asyncio: the waiting itself. A parked request awaits its own
``loop.create_future()``; a wake-up is ``future.set_result(None)``, which
schedules the task to run in a later step; cancellation (``task.cancel()``,
``asyncio.timeout()``, a ``TaskGroup``, ``sim.interrupt``, which is a cancel
underneath, D024) arrives as the ``CancelledError`` asyncio raises at
``await future``. Timers and virtual time: nothing here knows about time.

Ours (no code is copied from CPython):

* one FIFO line per direction (:class:`_Line`), a deque of :class:`_Waiter`
  records. A request stays in its line from the moment it parks until it is
  served or leaves, and it keeps its place when it re-parks;
* strict FIFO with head-of-line blocking (D021): only the head of a line is
  ever woken, and it re-checks whether its amount fits when it resumes. If it
  fits, it changes the level itself, in that step; if not (the level moved
  again before it ran), it parks again, still at the head;
* no barging: a new request is served at once (no suspension) only if its line
  is empty and the amount fits. A head that was woken but has not run yet is
  still in the line, so a newcomer queues behind it, as before;
* wake-ups: every level change wakes the parked head of each line (the oldest
  park first, the order ``Condition.notify_all`` used). A head that leaves
  with an exception (a cancel, a timeout, an interrupt) wakes the next head of
  its own line, so a cancelled big request never blocks the line and a
  wake-up is never lost. Nothing else wakes anyone: a parked head that does
  not fit stays unfit until the level changes;
* lazy deletion: a waiter that leaves from the middle of its line is only
  marked; marked records are popped when they reach the head, and the deque
  is rebuilt without them once they outnumber the live ones by more than
  ``_SLACK`` (O(1) amortised per cancellation, memory bounded by about twice
  the live waiters);
* no lock: the level and both lines are read and changed only in synchronous
  code (no ``await`` between the check and the change), so two tasks can never
  interleave inside one operation. The old ``Condition`` lock was never
  contended for the same reason;
* loop binding: like ``asyncio.Condition``, a container is bound to the first
  event loop a task parks on it in (``RuntimeError`` on another loop), through
  the stores' ``_LoopBinding`` helper.

What changed for models (D076, accepted by the owner): strict FIFO per
direction, head-of-line blocking, the no-barging rule, the monitor contract
and the guide's examples are unchanged. The *order* in which tasks run within
one instant can differ: a waiter behind the head used to be woken together
with it by ``notify_all`` and now is woken by the head's own level change, one
step later. For requests made at given instants, each is still served (or
cancelled) at the same instant and the level at the end of every instant is
the same (``tests/test_scaling_container.py`` checks this against the old
code). But a task that resumes a step earlier or later may make its next
request in a different order relative to other tasks, so a model can give a
different result for the same seed than desyncio versions before batch 34.

Also ours: amount and capacity validation, ``level``/``capacity``, and the
``monitor=`` hook (D021, D023 v2).
"""

import asyncio
import itertools
import math
import numbers
from collections import deque
from collections.abc import Callable

from desyncio._monitor import report_monitor_error
from desyncio.store import _LoopBinding

__all__ = ["Container"]

# A line's deque is rebuilt without its departed records once they outnumber
# the live ones by more than this, so a few departures never cost a rebuild.
_SLACK = 16


def _check_real(name: str, value: object) -> float:
    # bool is an int subclass but "True units of fuel" is always a bug.
    if isinstance(value, bool) or not isinstance(value, numbers.Real):
        raise TypeError(f"{name} must be a real number, not {value!r}")
    return float(value)


class _Waiter:
    """A parked request: its place in a line and the future it waits on."""

    __slots__ = ("future", "parked", "queued")

    def __init__(self, future: "asyncio.Future[None]", parked: int) -> None:
        self.future = future
        self.parked = parked  # park number, for the order of wake-ups
        self.queued = True  # False once it has left its line


class _Line:
    """One direction's FIFO of waiters, with O(1) amortised departures.

    ``live`` counts the waiters still queued; the deque may also hold records
    of waiters that left from the middle of the line (``queued`` is False).
    Those are popped when they reach the front, or dropped by a rebuild.
    """

    __slots__ = ("_waiters", "live")

    def __init__(self) -> None:
        self._waiters: deque[_Waiter] = deque()
        self.live = 0

    def join(self, waiter: _Waiter) -> None:
        """Queue ``waiter`` at the back."""
        self._waiters.append(waiter)
        self.live += 1

    def head(self) -> _Waiter | None:
        """The first queued waiter, popping departed records in front of it."""
        waiters = self._waiters
        while waiters and not waiters[0].queued:
            waiters.popleft()  # each record is popped at most once
        return waiters[0] if waiters else None

    def leave(self, waiter: _Waiter) -> bool:
        """Take ``waiter`` out of the line; return True if it was the head.

        The head is popped. Any other waiter is only marked (no search, unlike
        ``deque.remove``); the deque is rebuilt once departed records outnumber
        live ones by more than ``_SLACK``, which costs O(len) at most once per
        about len/2 departures.
        """
        was_head = self.head() is waiter
        waiter.queued = False
        self.live -= 1
        if was_head:
            self._waiters.popleft()
        elif len(self._waiters) > 2 * self.live + _SLACK:
            self._waiters = deque(w for w in self._waiters if w.queued)
        return was_head


class Container:
    """A bounded or unbounded level that processes can ``put`` into and ``get`` from.

    ``await c.put(n)`` blocks while ``level + n > capacity``; ``await c.get(n)``
    blocks while ``level < n``. Puts are served strictly FIFO among puts, and
    gets strictly FIFO among gets (head-of-line blocking): a large waiting
    ``get`` holds back smaller later ones until it is served. Puts and gets do
    not block each other's queues.

    ``monitor``, if given, is called as ``monitor(container)`` after every level
    change (not on blocked or cancelled requests), once the state is fully
    settled: the served request has left its line and the heads it may have
    unblocked have been woken. It is called synchronously in the task that
    changed the level, with no ``await`` in between, so ``level`` cannot change
    under it. It reads public properties (``level``, ``capacity``) and must not
    block or call ``put``/``get`` (D023).

    A monitor that raises an ``Exception`` does not affect the model (D023 v2):
    the ``put``/``get`` still completes normally and the error goes to
    ``loop.call_exception_handler`` (logged by default), just as asyncio does
    for a raising callback. Set ``loop.set_exception_handler(...)`` to make such
    errors fatal. ``KeyboardInterrupt`` and ``SystemExit`` still propagate.

    Like asyncio's primitives, a container is bound to the first event loop a
    task waits on it in; waiting on it from another loop raises
    ``RuntimeError``.
    """

    def __init__(
        self,
        capacity: float = math.inf,
        init: float = 0,
        *,
        monitor: Callable[["Container"], object] | None = None,
    ) -> None:
        capacity = _check_real("capacity", capacity)
        if not capacity > 0:  # also rejects NaN
            raise ValueError(f"capacity must be > 0, got {capacity!r}")
        init = _check_real("init", init)
        if not 0 <= init <= capacity:
            raise ValueError(f"init must satisfy 0 <= init <= capacity, got {init!r}")
        self._capacity = capacity
        self._level = init
        self._monitor = monitor
        # One line per direction. No lock: every read and change of the level
        # and the lines happens in one synchronous step (see the module
        # docstring).
        self._putters = _Line()
        self._getters = _Line()
        # Park numbers: wake-ups go to the head that parked first.
        self._parks = itertools.count()
        self._binding = _LoopBinding()

    @property
    def level(self) -> float:
        """Current amount held (read-only)."""
        return self._level

    @property
    def capacity(self) -> float:
        """Maximum amount that can be held (read-only)."""
        return self._capacity

    def _check_amount(self, amount: object) -> float:
        value = _check_real("amount", amount)
        if not 0 < value < math.inf:  # also rejects NaN
            raise ValueError(f"amount must be > 0 and finite, got {amount!r}")
        if value > self._capacity:
            raise ValueError(
                f"amount {amount!r} exceeds capacity {self._capacity!r} "
                "and could never be satisfied"
            )
        return value

    async def put(self, amount: float) -> None:
        """Add ``amount``, waiting (FIFO among puts) until it fits under capacity."""
        value = self._check_amount(amount)
        room = self._capacity - value
        await self._transfer(self._putters, value, lambda: self._level <= room)

    async def get(self, amount: float) -> None:
        """Remove ``amount``, waiting (FIFO among gets) until the level covers it."""
        value = self._check_amount(amount)
        await self._transfer(self._getters, -value, lambda: self._level >= value)

    async def _transfer(
        self, line: _Line, delta: float, fits: Callable[[], bool]
    ) -> None:
        if not line.live and fits():
            # Nobody queued in this direction and it fits: no suspension.
            self._level += delta
            self._level_changed()
            return
        loop = self._binding.get(self)
        waiter = _Waiter(loop.create_future(), next(self._parks))
        line.join(waiter)
        try:
            # Only a head is ever woken. It re-checks here, because the level
            # may have moved again between the wake-up and this step.
            await waiter.future
            while not fits():
                waiter.future = loop.create_future()
                waiter.parked = next(self._parks)
                await waiter.future
        except BaseException:
            # Cancelled, timed out or interrupted while parked, or after a
            # wake-up but before this step ran. The level is untouched. If
            # this was the head, the next waiter is the head now and may fit
            # (a big request no longer blocks a small one): wake it, so a
            # wake-up is never lost.
            if line.leave(waiter):
                self._wake(line.head())
            raise
        line.leave(waiter)  # the head: woken heads are the only ones to run
        self._level += delta
        self._level_changed()

    def _level_changed(self) -> None:
        """Wake the parked head of each line (oldest park first), then monitor.

        A level change is the only thing that can make a waiting head fit, so
        both heads re-check; a head that was already woken and has not run yet
        will see the new level when it runs. ``set_result`` only schedules the
        woken tasks: they run after this task's current step, so the monitor
        below sees the settled level.
        """
        heads = [w for w in (self._putters.head(), self._getters.head()) if w]
        heads.sort(key=lambda w: w.parked)
        for waiter in heads:
            self._wake(waiter)
        self._notify_monitor()

    @staticmethod
    def _wake(waiter: _Waiter | None) -> None:
        if waiter is not None and not waiter.future.done():
            waiter.future.set_result(None)

    def _notify_monitor(self) -> None:
        """Call the monitor, routing its errors to the loop's exception handler.

        This mirrors what asyncio's ``Handle._run`` does for callbacks: catch
        ``Exception`` (not ``BaseException``, so ``KeyboardInterrupt`` and
        ``SystemExit`` propagate) and report it via ``call_exception_handler``.
        The calling task's operation has already completed, so it is unaffected.
        Outside a running loop the error is logged on the ``desyncio`` logger.
        """
        if self._monitor is None:
            return
        try:
            self._monitor(self)
        except Exception as exc:
            report_monitor_error("Container monitor raised", exc, "container", self)
