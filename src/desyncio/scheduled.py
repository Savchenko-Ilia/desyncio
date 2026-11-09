"""``ScheduledResource``: a resource whose capacity follows a ``Schedule`` (CAL-03).

Usage::

    clinic = ScheduledResource(schedule)       # inside a run

    async def patient():
        try:
            async with clinic:                 # waits while capacity is 0 or full
                await sim.timeout(20 * 60)     # being treated
        except Interrupt as exc:               # outside the ``async with``
            assert isinstance(exc.cause, ShiftEnded)   # the shift ended on us

What happens when
-----------------
* At creation, ``capacity == schedule.capacity_at(sim.now)``.
* A background **driver task** (named ``"ScheduledResource driver"``) walks
  ``schedule.changes(sim.now)``: it sleeps until each change time and applies
  the new capacity. When ``changes`` ends (CAL-02: the capacity is constant for
  good, D053) the driver finishes and the last capacity stays.
* **Capacity rises**: waiters are granted in FIFO order until in-use reaches
  the new capacity.
* **Capacity drops below the units in use**: the most recently *granted*
  holders (D051 item 2) lose their units until in-use <= capacity. A unit
  that was handed to a waiter whose task has not resumed yet counts as held and
  is the most recent of all (as in RES-03, D034). Each loser's ``async with``
  raises ``Interrupt(ShiftEnded(at=now, resource=self))`` and its body does not
  continue.
* **Capacity 0**: every request waits.

When exactly does the interrupt land?
-------------------------------------
The capacity change is applied *synchronously* inside one step of the driver
task: counters change, the victims' units are taken back and waiters (if any)
are granted, all before the driver yields. Only the *delivery* is
asynchronous. ``interrupt_task`` calls ``task.cancel()``, and a cancel makes the
victim raise ``CancelledError`` the next time the loop runs that task, which is
a later iteration of the loop at the **same virtual time**. So between the
capacity change and the victim noticing it, other tasks that were already
scheduled for this instant may run; the victim's body does not run again. The
victim's ``async with`` then turns the cancellation into ``Interrupt`` (D024).
The unit is already gone, so ``__aexit__`` does not release a second time.
Release stays free of awaits (D030): a cancel can never skip it.

Same instant: ordinary events first, then the shift change (D054)
-----------------------------------------------------------------
The driver arms its timers with ``loop.call_at_priority(t, 1, ...)``. Timers
fire in ``(time, priority, sequence)`` order, so at time ``t`` every ordinary
timer (priority 0: ``sim.timeout``, ``asyncio.sleep``, ``call_at``...) fires
before the driver's, whichever was armed first. How that carries over to the
task steps: in one ``_run_once`` iteration all timers due at ``t`` move to the
ready queue in that order and run. Each callback only resolves a future, and
the future schedules its task's next step with ``call_soon`` (FIFO). So in the
next iteration the woken tasks step in the same order: a holder whose visit
ends at ``t`` runs its ``async with`` exit (a synchronous release) and an
arrival at ``t`` queues its request, both before the driver's step applies the
change. Hence:

* a visit ending exactly at a shift end completes and releases its unit; the
  drop then has one unit fewer to take back, and the holder is not interrupted;
* arrivals at ``t`` are queued first, then a capacity rise at ``t`` grants them
  in FIFO order.

The guarantee covers work one step away from an ordinary timer at ``t``. Work
that needs more steps (say a task that waits for *another* task woken at ``t``)
may still come after the driver's step. If a release at ``t`` hands the unit
to a waiter (a reservation) and the drop at ``t`` takes it straight back, that
waiter gets the ``Interrupt`` without its body running.

Cancelling a waiter
-------------------
A waiter cancelled while queued leaves the queue and never takes a unit. One
cancelled after it was granted a unit but before it resumed hands the unit to
the next waiter (``asyncio.Semaphore._acquire``'s fix-up).

What is delegated to asyncio, and what is ours (principle 6)
------------------------------------------------------------
asyncio has no semaphore whose size changes over time, so the queue is ours,
built on asyncio's building blocks like ``PriorityResource`` (D030):

* delegated: the waiting itself (each waiter awaits its own
  ``loop.create_future()``, and a cancel, timeout or ``TaskGroup`` arrives as
  the ``CancelledError`` raised at ``await fut``); the driver is an ordinary
  task on the loop with a loop timer (``loop.call_at_priority``, the
  ``call_at`` of ``VirtualTimeLoop`` with a tie-break priority); interrupts
  reuse
  ``interrupt_task`` (cancel plus translation in ``__aexit__``, INT-01);
* ours: the FIFO deque of waiters, the grant rule, the grant-order list used
  to pick victims, the counters, and the monitor hook.

The driver task is cancelled when ``run`` ends, like any leftover task
(LOOP-04b), so it does not keep ``run(main)`` alive. While it still has a change
ahead it owns a timer, so the loop is never "deadlocked" because of it. The
other side of that: a ``main`` that waits forever on a schedule that keeps
changing is not detected as a deadlock, the clock just advances.

Monitor (D023 v2): ``monitor(resource)`` runs synchronously after every state
change (queueing, grant, leaving the queue, release, capacity change). Errors
go to the loop's exception handler.
"""

import asyncio
from collections import deque
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from types import TracebackType
from typing import Any, Self

from desyncio._monitor import report_monitor_error
from desyncio.interrupt import Interrupt, Interruptible, interrupt_task
from desyncio.loop import VirtualTimeLoop
from desyncio.schedule import Schedule
from desyncio.sim import _loop, sim


@dataclass(frozen=True)
class ShiftEnded:
    """The cause of the ``Interrupt`` a holder gets when its shift ends.

    ``at`` is the virtual time of the capacity drop and ``resource`` the
    ``ScheduledResource`` that took the unit away.
    """

    at: float
    resource: "ScheduledResource"


@dataclass(eq=False)
class _Hold:
    """One acquisition. It "has a unit" from assignment until release or loss."""

    task: "asyncio.Task[Any]"
    fut: "asyncio.Future[None] | None" = None  # set while it waits in the queue
    held: bool = False  # the task has resumed with the unit (else: reservation)
    lost: ShiftEnded | None = None  # set when the shift took the unit away


class ScheduledResource:
    """A resource whose capacity follows ``schedule``; see the module docstring.

    Args:
        schedule: the ``Schedule`` that gives the capacity over sim time.
        monitor: optional ``monitor(resource)`` called after every state change.

    Create it inside a run (``RuntimeError`` otherwise): it starts the driver
    task. Use it as ``async with res:``; the body is interruptible, and a shift
    end raises ``Interrupt(ShiftEnded(...))`` from the ``async with``.
    """

    def __init__(
        self,
        schedule: Schedule,
        *,
        monitor: Callable[["ScheduledResource"], object] | None = None,
    ) -> None:
        if not isinstance(schedule, Schedule):
            raise TypeError(
                f"schedule must be a Schedule, got {type(schedule).__name__}"
            )
        if monitor is not None and not callable(monitor):
            raise TypeError(f"monitor must be callable, got {type(monitor).__name__}")
        now = sim.now  # RuntimeError outside a run
        self._schedule = schedule
        self._monitor = monitor
        self._capacity = schedule.capacity_at(now)
        self._waiters: deque[_Hold] = deque()
        self._units: list[_Hold] = []  # every hold with a unit, in grant order
        self._count = 0  # holds whose task has resumed
        self._plain: dict[asyncio.Task[Any], list[tuple[_Hold, Interruptible]]] = {}
        loop = _loop()
        self._driver = loop.create_task(
            self._drive(loop, now), name="ScheduledResource driver"
        )

    @property
    def capacity(self) -> int:
        """Units available right now (from the schedule)."""
        return self._capacity

    @property
    def count(self) -> int:
        """Units in use by tasks that have resumed holding them."""
        return self._count

    @property
    def queue_length(self) -> int:
        """Requests waiting for a unit, including ones granted but not resumed."""
        return len(self._waiters) + len(self._units) - self._count

    def _changed(self) -> None:
        if self._monitor is None:
            return
        try:
            self._monitor(self)
        except Exception as exc:  # KeyboardInterrupt/SystemExit propagate
            report_monitor_error(
                "ScheduledResource monitor raised", exc, "resource", self
            )

    # --- the driver ----------------------------------------------------------

    async def _drive(self, loop: VirtualTimeLoop, now: float) -> None:
        for t, capacity in self._schedule.changes(now):
            # Sleep until exactly t. ``asyncio.sleep(t - now)`` would compute
            # ``now + (t - now)``, which may differ from t by a rounding error;
            # call_at takes the absolute time. Priority 1 (D054): at time t
            # every ordinary timer (priority 0) fires first; see "Same instant".
            wake: asyncio.Future[None] = loop.create_future()
            handle = loop.call_at_priority(t, 1, wake.set_result, None)
            try:
                await wake
            finally:
                handle.cancel()  # no-op once fired; frees the timer if cancelled
            self._set_capacity(capacity)

    def _set_capacity(self, capacity: int) -> None:
        """Apply a new capacity in one synchronous step (no await)."""
        self._capacity = capacity
        # Drop: take units from the most recently granted holds first.
        while len(self._units) > capacity:
            self._take_away(self._units[-1])
        # Rise: hand units to waiters.
        self._grant_free_units()
        self._changed()

    def _take_away(self, hold: _Hold) -> None:
        self._units.remove(hold)
        hold.lost = ShiftEnded(at=sim.now, resource=self)
        if hold.held:
            self._count -= 1
            # cancel() underneath: the task raises at its next turn, at this
            # same virtual time, and its ``async with`` translates it.
            interrupt_task(hold.task, hold.lost)
        # else: a reservation. The task is waking up from its future and raises
        # the Interrupt itself in _acquire; nothing is cancelled underneath.

    # --- granting and releasing ---------------------------------------------

    def _grant_free_units(self) -> None:
        """Hand free units to the head waiters, FIFO, synchronously."""
        while self._waiters and len(self._units) < self._capacity:
            hold = self._waiters.popleft()
            assert hold.fut is not None
            if hold.fut.done():
                continue  # cancelled while queued; its task has left or will
            self._units.append(hold)
            hold.fut.set_result(None)  # the waiter resumes at its next turn

    async def _acquire(self, task: "asyncio.Task[Any]") -> _Hold:
        hold = _Hold(task)
        # No barging: a free unit is taken at once only if nobody is queued.
        if len(self._units) < self._capacity and not self._waiters:
            self._units.append(hold)
            hold.held = True
            self._count += 1
            self._changed()
            return hold

        hold.fut = asyncio.get_running_loop().create_future()
        self._waiters.append(hold)
        self._changed()  # queued
        try:
            await hold.fut
        except BaseException:
            if hold.fut.done() and not hold.fut.cancelled():
                # Granted, then cancelled before resuming: pass the unit on.
                # (If the shift took it back meanwhile there is nothing to do.)
                if hold.lost is None:
                    self._units.remove(hold)
            else:
                # Still queued, or already skipped by a grant: leave if present.
                with suppress(ValueError):
                    self._waiters.remove(hold)
            self._grant_free_units()
            self._changed()
            raise
        if hold.lost is not None:
            raise Interrupt(hold.lost)  # reserved, then taken back: no body
        hold.held = True
        self._count += 1
        self._changed()
        return hold

    def _release(self, hold: _Hold) -> None:
        if hold.lost is not None:
            return  # the unit is already gone; releasing again would free another's
        self._units.remove(hold)
        self._count -= 1
        self._grant_free_units()
        self._changed()

    async def __aenter__(self) -> Self:
        task = asyncio.current_task()
        if task is None:
            raise RuntimeError("a ScheduledResource must be used inside a task")
        hold = await self._acquire(task)
        # No suspension from here to the end of the method, so no shift change
        # can take the unit before the interruptible scope exists.
        scope = Interruptible()
        await scope.__aenter__()  # declared async, but never suspends
        self._plain.setdefault(task, []).append((hold, scope))
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        task = asyncio.current_task()
        assert task is not None
        stack = self._plain[task]
        hold, scope = stack.pop()
        if not stack:
            del self._plain[task]
        # Release first, synchronously (D030), then leave the scope (which
        # never suspends), so a cancel cannot slip in between.
        self._release(hold)
        await scope.__aexit__(exc_type, exc, tb)
