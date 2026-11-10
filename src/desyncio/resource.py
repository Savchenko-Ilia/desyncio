"""``Resource`` (RES-01), ``PriorityResource`` (RES-02) and ``PreemptiveResource``.

``PriorityResource`` is described at the end of this docstring; the rest
describes ``Resource``, a counted resource with a FIFO queue.
``PreemptiveResource`` is documented on its own class (D032).

Usage::

    bank = Resource(capacity=2)          # two tellers

    async def customer():
        async with bank:                 # waits (in virtual time) for a teller
            await sim.timeout(5)         # being served
        # the teller is released here, even on exception or cancellation

What is delegated to asyncio, and why
-------------------------------------
Everything that is hard to get right is done by a stock ``asyncio.Semaphore``:

* the FIFO queue of waiters (a deque of futures inside the semaphore);
* "no barging": ``Semaphore.acquire`` refuses the fast path while anyone is
  queued, so a newcomer arriving at the same instant as a release still
  queues behind existing waiters;
* cancellation: a cancelled waiter is removed from the queue, and a waiter
  cancelled *after* being granted gives the unit back and wakes the next one.

Nothing in the virtual-time loop needs special support, so the semaphore
works unchanged in simulated time.

What is ours
------------
* argument validation;
* the read-only ``capacity``, ``count`` and ``queue_length`` numbers. They are
  our own counters, updated around ``acquire``/``release``; we never read the
  semaphore's private attributes;
* the optional ``monitor`` hook (D021) and its error contract (D023 v2).

PriorityResource: per-request futures, not Condition (D030)
-----------------------------------------------------------
asyncio has no priority semaphore, so ``PriorityResource`` follows the shape
of ``asyncio.Semaphore`` (CPython 3.13 ``asyncio/locks.py``) with one change:
its waiters are kept in ``(priority, request order)`` order instead of FIFO.

Delegated to asyncio: the waiting itself. Each waiter awaits its own
``loop.create_future()``; waking it is ``fut.set_result(None)``, and
cancellation (a plain ``task.cancel()``, an enclosing ``asyncio.timeout()``
or ``TaskGroup``, or ``sim.interrupt``, which is a cancel underneath, D024)
arrives as the ``CancelledError`` that asyncio raises at ``await fut``.

Ours: the ordering (a list kept sorted with ``bisect.insort``), the grant
rule, the counters, validation and the monitor. A sorted list rather than a
heap because a pending waiter that is cancelled must leave the order at once
(``queue_length`` and the monitor see it go); with a sorted list that is one
``bisect`` and a ``del``, where a heap would need lazy deletion plus a
separate live counter. Inserts and removals shift the list (O(waiters)), which
is fine for simulation queue sizes.

How it mirrors ``Semaphore``:

* release is synchronous (``Semaphore.release()``): it frees the unit and, in
  the same step, hands it to the best live waiter by resolving its future.
  There is no ``await`` before the decrement, so a cancel or interrupt at the
  holder's exit can never skip it, and a body exception is never replaced;
* no barging: because the hand-over happens inside release, a newcomer at the
  same instant finds no free unit and queues, unless its key is strictly
  better than every live waiter's (then it legitimately goes first);
* the fast path takes a free unit without suspending, as ``Semaphore``
  (and so RES-01's ``Resource``) does;
* a waiter cancelled after its future was resolved, but before it resumed,
  gives the unit to the next waiter (``Semaphore._acquire``'s fix-up for
  "done and not cancelled"); a waiter cancelled while still pending removes
  itself from the order.

Why not ``asyncio.Condition`` (round 0, rejected in D030): ``notify_all()``
needs the Condition's lock, so the release in ``__aexit__`` had to ``await``
that lock. Right after a ``notify_all`` the woken waiters queue on the lock,
so the acquire really suspends, and a cancel at that point skipped the
decrement: the unit leaked for good and the ``CancelledError`` replaced the
body's exception. ``Condition`` cannot express a synchronous release. It also
woke every waiter on every change (O(waiters)); one future per request wakes
exactly the one that gets the unit.
"""

import asyncio
import itertools
import math
from bisect import bisect_left, insort
from collections.abc import Callable
from dataclasses import dataclass
from types import TracebackType
from typing import Any, Self

from desyncio._monitor import report_monitor_error
from desyncio.interrupt import Interrupt, Interruptible, interrupt_task


def _call_monitor(primitive: object, monitor: Callable[[Any], object] | None) -> None:
    """Call ``monitor(primitive)`` once state is settled (D023 v2), shared by
    ``Resource`` and ``PriorityResource``.

    Reuses asyncio's callback error handling (``Handle._run``): an
    ``Exception`` goes to the loop's exception handler instead of the calling
    task, so it can neither leave the primitive half-updated nor mask an
    exception (e.g. ``CancelledError``) already in flight. Outside a running
    loop the error is logged on the ``desyncio`` logger instead (D045).
    """
    if monitor is None:
        return
    try:
        monitor(primitive)
    except Exception as exc:  # KeyboardInterrupt/SystemExit propagate
        report_monitor_error("Resource monitor raised", exc, "resource", primitive)


def _check_name(name: str | None) -> str | None:
    """Validate the optional resource ``name`` (shared by all four resources)."""
    if name is not None:
        if not isinstance(name, str):
            raise TypeError(f"name must be a str, got {type(name).__name__}")
        if not name:
            raise ValueError("name must not be empty")
    return name


class Resource:
    """``capacity`` interchangeable units, handed out first-come first-served.

    ``name`` (keyword-only, optional) labels the resource: a non-``str`` raises
    ``TypeError``, an empty string ``ValueError``; the ``name`` property returns
    it, or ``None``. It will appear in ``ResourceChanged.resource`` once events
    exist. All four resource classes take it.

    ``monitor``, if given, is called as ``monitor(resource)`` synchronously
    after every state change: a request queueing, a request being granted,
    a queued request leaving (cancelled) and a release. Read ``count`` and
    ``queue_length`` inside it. It must not block or acquire the resource.

    A Resource is bound to the run in which it is first used (its
    ``asyncio.Semaphore`` binds to one event loop), so create it inside the run.

    Monitor errors (D023 v2) follow asyncio's rule for callbacks: an
    ``Exception`` raised by the monitor is passed to
    ``loop.call_exception_handler()`` (which logs it by default) and the
    operation carries on, exactly as ``Handle._run`` does for a ``call_soon``
    callback. Observation never changes the model: the request still queues,
    the grant stands, the release happens, and a cancelled waiter still sees
    its ``CancelledError``. ``KeyboardInterrupt`` and ``SystemExit`` propagate
    unchanged. To make monitor errors fatal, install your own handler with
    ``loop.set_exception_handler()``.
    """

    def __init__(
        self,
        capacity: int = 1,
        *,
        monitor: Callable[["Resource"], object] | None = None,
        name: str | None = None,
    ) -> None:
        if isinstance(capacity, bool) or not isinstance(capacity, int):
            raise TypeError(f"capacity must be an int, got {type(capacity).__name__}")
        if capacity < 1:
            raise ValueError(f"capacity must be >= 1, got {capacity}")
        if monitor is not None and not callable(monitor):
            raise TypeError(f"monitor must be callable, got {type(monitor).__name__}")
        self._name = _check_name(name)
        self._capacity = capacity
        self._monitor = monitor
        self._sem = asyncio.Semaphore(capacity)  # does all waiting and queueing
        self._count = 0
        self._queue_length = 0

    @property
    def name(self) -> str | None:
        """The name given at construction, or ``None``."""
        return self._name

    @property
    def capacity(self) -> int:
        """Total number of units."""
        return self._capacity

    @property
    def count(self) -> int:
        """Units currently in use (held by a task that finished acquiring)."""
        return self._count

    @property
    def queue_length(self) -> int:
        """Requests waiting for a unit.

        A request that has been granted a unit but whose task has not yet
        resumed still counts as waiting until it does.
        """
        return self._queue_length

    def _changed(self) -> None:
        """Call the monitor; called only once the state is fully settled."""
        _call_monitor(self, self._monitor)

    async def _acquire(self) -> None:
        # Semaphore.acquire has no await before its fast path, so this check
        # agrees with what acquire() will decide a moment later.
        must_wait = self._sem.locked()
        if must_wait:
            self._queue_length += 1
            self._changed()
        try:
            await self._sem.acquire()
        except BaseException:
            # Cancelled while queued, or granted and cancelled before resuming.
            # In the second case the semaphore itself returns the unit, and
            # because we have not incremented ``count`` there is nothing to undo.
            if must_wait:
                self._queue_length -= 1
                self._changed()
            raise
        if must_wait:
            self._queue_length -= 1
        self._count += 1
        self._changed()

    def _release(self) -> None:
        self._count -= 1
        self._sem.release()
        self._changed()

    async def __aenter__(self) -> Self:
        await self._acquire()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        # Runs on normal exit, exception and cancellation alike.
        self._release()


class PriorityRequest:
    """One request for a unit of a ``PriorityResource`` (RES-02).

    Returned by ``PriorityResource.request(priority=...)`` and used as
    ``async with res.request(priority=2): ...``. A request is single-use.
    It is a class, not a bare context manager, so RES-03 can add options
    (preemption) without changing the call shape.
    """

    def __init__(self, resource: "PriorityResource", priority: float) -> None:
        self._resource = resource
        self._priority = priority
        self._used = False

    @property
    def priority(self) -> float:
        """The priority; lower is served first."""
        return self._priority

    async def __aenter__(self) -> Self:
        if self._used:
            raise RuntimeError("a PriorityRequest can be used only once")
        self._used = True
        await self._resource._acquire(self._priority)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        # Synchronous on purpose (D030): no await, so nothing can interrupt
        # the release, and an exception from the body propagates unchanged.
        self._resource._release()


class PriorityResource:
    """``capacity`` units served in ``(priority, request order)`` order.

    ``async with res.request(priority=p):`` -- a lower ``p`` is served first;
    equal priorities are served in the order they began waiting. Plain
    ``async with res:`` is priority 0. "Request order" is the moment the
    ``async with`` is entered (not when ``request()`` was called).

    Priorities may be ``int`` or ``float`` (negative and infinite allowed).
    ``bool``, NaN and other types are rejected with ``TypeError`` /
    ``ValueError``, because NaN breaks ordering and ``True`` is almost
    certainly a mistake.

    No barging: a newcomer takes a unit at once only if its key is better
    than every waiter's; otherwise it queues, even when a unit is free at
    that very instant. A newcomer with a strictly better key legitimately
    goes first. Introspection (``capacity``, ``count``, ``queue_length``),
    cancellation and the ``monitor`` contract (D023 v2) are as for
    ``Resource`` (``name`` too: it will appear in ``ResourceChanged.resource``
    once events exist): ``queue_length`` counts a request that has been handed
    a unit but whose task has not resumed yet, and ``count`` includes it only
    once it has. Release is synchronous, so a cancel, timeout or interrupt
    arriving at the holder's exit cannot leak the unit. See the module
    docstring for how waiting is built (per-request futures, D030).
    Create it inside the run (its futures belong to the running loop).
    """

    def __init__(
        self,
        capacity: int = 1,
        *,
        monitor: Callable[["PriorityResource"], object] | None = None,
        name: str | None = None,
    ) -> None:
        if isinstance(capacity, bool) or not isinstance(capacity, int):
            raise TypeError(f"capacity must be an int, got {type(capacity).__name__}")
        if capacity < 1:
            raise ValueError(f"capacity must be >= 1, got {capacity}")
        if monitor is not None and not callable(monitor):
            raise TypeError(f"monitor must be callable, got {type(monitor).__name__}")
        self._name = _check_name(name)
        self._capacity = capacity
        self._monitor = monitor
        # Waiters in key order. The seq number is unique, so tuples compare on
        # (priority, seq) only and never reach the (uncomparable) future.
        self._waiters: list[
            tuple[float, int, asyncio.Future[None], _Ticket | None]
        ] = []
        self._seq = itertools.count()
        self._count = 0  # units held by tasks that have resumed
        self._granted = 0  # units handed to a waiter that has not resumed yet

    @property
    def name(self) -> str | None:
        """The name given at construction, or ``None``."""
        return self._name

    @property
    def capacity(self) -> int:
        """Total number of units."""
        return self._capacity

    @property
    def count(self) -> int:
        """Units currently in use."""
        return self._count

    @property
    def queue_length(self) -> int:
        """Requests waiting for a unit.

        As for ``Resource``, a request that has been handed a unit but whose
        task has not yet resumed still counts as waiting until it does.
        """
        return len(self._waiters) + self._granted

    def request(self, priority: float = 0) -> PriorityRequest:
        """Return a request to use as ``async with res.request(priority=p):``."""
        if isinstance(priority, bool) or not isinstance(priority, int | float):
            raise TypeError(
                f"priority must be an int or float, got {type(priority).__name__}"
            )
        if isinstance(priority, float) and math.isnan(priority):
            raise ValueError("priority must not be NaN")
        return PriorityRequest(self, priority)

    def _changed(self) -> None:
        """Call the monitor once state is settled (same contract as Resource)."""
        _call_monitor(self, self._monitor)

    def _free(self) -> int:
        return self._capacity - self._count - self._granted

    def _blocked_by_waiter(self, priority: float) -> bool:
        """True if a live waiter has a better-or-equal key than a newcomer.

        A newcomer's request order is the newest, so "better or equal" means
        any priority <= its own.
        """
        return any(not w[2].done() and w[0] <= priority for w in self._waiters)

    # Hooks for PreemptiveResource; no-ops here. ``ticket`` is None unless a
    # subclass passes one to ``_acquire``.
    def _arriving(self, ticket: "_Ticket | None") -> None:
        """A request is about to look for a unit (before the fast path)."""

    def _unit_assigned(self, ticket: "_Ticket | None") -> None:
        """A unit was assigned to ``ticket`` (fast path or hand-over)."""

    def _became_holder(self, ticket: "_Ticket | None") -> None:
        """``ticket``'s task has resumed holding its unit."""

    def _unit_dropped(self, ticket: "_Ticket | None") -> None:
        """A granted-but-not-resumed ``ticket`` gave its unit back (cancelled)."""

    def _grant_free_units(self) -> None:
        """Hand every free unit to the best live waiters, synchronously.

        Like ``Semaphore._wake_up_next``: a waiter whose future is already
        done was cancelled while pending; it is skipped here and removes
        itself from the order when its task runs.
        """
        i = 0
        while self._free() > 0 and i < len(self._waiters):
            fut, ticket = self._waiters[i][2], self._waiters[i][3]
            if fut.done():
                i += 1
                continue
            del self._waiters[i]
            self._granted += 1
            self._unit_assigned(ticket)
            fut.set_result(None)  # the waiter resumes at its next turn

    async def _acquire(self, priority: float, ticket: "_Ticket | None" = None) -> None:
        key = (priority, next(self._seq))
        if ticket is not None:
            ticket.seq = key[1]
        self._arriving(ticket)
        # Fast path, no suspension (as Semaphore.acquire). Release hands
        # units over at once, so a free unit means no live waiter is left;
        # the explicit check states the no-barging rule anyway: a live waiter
        # with a better-or-equal key (any priority <= ours, since our seq is
        # the newest) keeps the newcomer out.
        if self._free() > 0 and not self._blocked_by_waiter(priority):
            self._count += 1
            self._unit_assigned(ticket)
            self._became_holder(ticket)
            self._changed()
            return

        fut: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        entry = (priority, key[1], fut, ticket)
        insort(self._waiters, entry)
        self._changed()  # queued
        try:
            await fut
        except BaseException:
            # CancelledError (a cancel, timeout or sim.interrupt) is the only
            # exception expected here; anything else thrown in is treated the
            # same way so that no unit can leak.
            if fut.done() and not fut.cancelled():
                # Granted, then cancelled before resuming (Semaphore._acquire's
                # fix-up): the unit was ours; pass it to the next waiter.
                # (A preempted reservation was already taken back: see
                # PreemptiveResource, so there is nothing to give back.)
                if ticket is None or ticket.evicted is None:
                    self._granted -= 1
                    self._unit_dropped(ticket)
            else:
                # Still in the order: leave it.
                del self._waiters[bisect_left(self._waiters, key)]
            # Wake whoever can now go (Semaphore's ``finally`` does the same).
            self._grant_free_units()
            self._changed()
            raise
        if ticket is not None and ticket.evicted is not None:
            # Granted, then preempted before resuming (PreemptiveResource):
            # the unit is already the preemptor's; the monitor was called then.
            raise Interrupt(ticket.evicted)
        # Granted by _grant_free_units, which already removed our entry.
        self._granted -= 1
        self._count += 1
        self._became_holder(ticket)
        self._changed()

    def _release(self) -> None:
        # Synchronous, like Semaphore.release(): decrement and hand-over in
        # one step, so neither a cancel nor a same-instant newcomer can get
        # between them.
        self._count -= 1
        self._grant_free_units()
        self._changed()

    async def __aenter__(self) -> Self:
        await self._acquire(0)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self._release()  # synchronous, see PriorityRequest.__aexit__


@dataclass(frozen=True)
class Preempted:
    """The cause of the ``Interrupt`` a preempted holder receives (RES-03).

    ``by`` is the task whose request took the unit, ``usage_since`` is the
    virtual time at which the victim got the unit it just lost, and
    ``resource`` is the resource it was preempted on (useful when a process
    uses several). Catch it as ``except Interrupt as exc`` *outside* the
    ``async with``; ``exc.cause`` is this object.
    """

    by: "asyncio.Task[Any]"
    usage_since: float
    resource: "PreemptiveResource"


@dataclass(eq=False)
class _Ticket:
    """Book-keeping for one request of a ``PreemptiveResource``.

    A ticket "has a unit" from the moment the unit is assigned to it (fast
    path or hand-over) until it is released or preempted. ``held`` says the
    task has resumed with its unit (a holder); a ticket with a unit that is
    not ``held`` is a reservation (granted, task not resumed yet).
    """

    task: "asyncio.Task[Any]"
    priority: float
    preempt: bool
    seq: int = -1
    since: float = 0.0  # virtual time the unit was assigned
    held: bool = False
    evicted: Preempted | None = None  # set when the unit was taken away


class PreemptiveRequest(PriorityRequest):
    """A request on a ``PreemptiveResource`` (RES-03): ``async with res.request(...)``.

    After the unit is acquired, the body runs inside a ``sim.interruptible()``
    block (INT-01), so the holder can be preempted. Single-use.
    """

    def __init__(
        self, resource: "PreemptiveResource", priority: float, preempt: bool
    ) -> None:
        super().__init__(resource, priority)
        self._preemptive_resource = resource
        self._preempt = preempt
        self._ticket: _Ticket | None = None
        self._scope: Interruptible | None = None

    @property
    def preempt(self) -> bool:
        """Whether this request may preempt a worse holder."""
        return self._preempt

    async def __aenter__(self) -> Self:
        if self._used:
            raise RuntimeError("a PriorityRequest can be used only once")
        self._used = True
        task = asyncio.current_task()
        if task is None:
            raise RuntimeError("a PreemptiveRequest must be used inside a task")
        ticket = _Ticket(task, self._priority, self._preempt)
        self._ticket = ticket
        # May raise Interrupt(Preempted) if the unit was granted to us and
        # preempted before we resumed: then we never held it, nothing to undo.
        await self._preemptive_resource._acquire(self._priority, ticket)
        # No suspension between the line above and the next: nothing can
        # preempt us before the scope exists (Interruptible.__aenter__ does
        # not await anything that suspends).
        scope = Interruptible()
        await scope.__aenter__()
        self._scope = scope
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        assert self._ticket is not None and self._scope is not None
        # 1. Release, synchronously (D030). A preempted holder's unit is gone
        #    already: releasing again would free the preemptor's unit.
        if self._ticket.evicted is None:
            self._preemptive_resource._release_ticket(self._ticket)
        # 2. Only then leave the scope. Interruptible.__aexit__ is declared
        #    ``async`` but contains no ``await``, so it never suspends: a
        #    cancel cannot arrive between 1 and 2, and a cancel or interrupt
        #    arriving earlier has already been delivered inside the body.
        await self._scope.__aexit__(exc_type, exc, tb)


class PreemptiveResource(PriorityResource):
    """A ``PriorityResource`` whose holders can be preempted (RES-03, D032).

    Usage (SimPy-style)::

        try:
            async with res.request(priority=5):           # a low-priority job
                await sim.timeout(work)
        except Interrupt as exc:                          # outside the async with
            assert isinstance(exc.cause, Preempted)
            work -= sim.now - exc.cause.usage_since       # resubmit the rest

        async with res.request(priority=0):               # may preempt (default)
            ...
        async with res.request(priority=0, preempt=False):  # waits politely
            ...

    Rules:

    * Every holder is preemptible: the body of ``request(...)`` and of plain
      ``async with res:`` (priority 0, ``preempt=True``) runs inside a
      ``sim.interruptible()`` block.
    * ``preempt=True`` means *this request may preempt*. With ``preempt=False``
      it waits exactly like a ``PriorityResource`` request.
    * A ``preempt=True`` request preempts only when (1) no unit is free,
      (2) it would be at the head of the queue, i.e. no live waiter has a
      better-or-equal key (RES-02's no-barging rule), and (3) its priority is
      strictly lower than the victim's. The victim is the holder with the
      largest ``(priority, request order)``: among the worst priority, the most
      recent holder. Equal priority never preempts. This is decided once, when
      the request is made, as in SimPy.
    * A unit that was handed to a waiter whose task has not resumed yet counts
      as held: it can be preempted, and then the waiter's ``async with`` raises
      the ``Interrupt`` straight away (its body never starts).
    * Preemption is synchronous (D030), in one step and at the same virtual
      time: the victim is removed from the holders and ``count`` is decremented,
      the unit goes to the preemptor without passing through the queue, then
      ``sim.interrupt(victim_task, Preempted(...))`` is called. The victim's
      ``async with`` exits with ``Interrupt`` and does **not** release again.
      The monitor is called once, after the swap.
    * Release stays free of awaits: ``__aexit__`` releases synchronously
      first, then leaves the interruptible scope (which never suspends), so
      no cancel can leak a unit.

    Cases (documented decisions):

    * **Own task.** A preemptor never interrupts itself. Units already held
      by the preemptor's own task are not candidates: it takes the next-worst
      holder of another task if that one is strictly worse, otherwise it
      waits (like any nested acquire of a busy unit; if all units are its own
      it waits for itself, so avoid nesting there).
    * **Nested ``sim.interruptible()`` in the body.** ``sim.interrupt``
      delivers to the innermost block, so the ``Interrupt(Preempted)`` lands
      inside the user's block, and *the unit is already gone*. If the body
      swallows that ``Interrupt`` and carries on, it runs without the unit
      (the exit does not release; nothing is double-released, but the work is
      no longer exclusive). Recommended: do not swallow an ``Interrupt``
      whose ``cause`` is ``Preempted``; re-raise it, and catch it outside the
      ``async with``.
    * **Late preemption.** If the body swallows the cancellation (or has no
      await left), INT-01's late-interrupt rule applies: the scope exits with
      no ``Interrupt``. The unit is the preemptor's; the exit does not release.
    * **Cancel or failure in the same instant.** A real cancel always wins
      over the preemption (INT-01): the victim ends with ``CancelledError``; a
      body that turns the cancel into another exception propagates that
      exception. Either way the unit goes to the preemptor and is released
      once.
    * **Several preemptions in one instant** are processed one by one in the
      order the preemptors run; each sees the holders as left by the previous
      one (a preemptor that has just taken a unit is itself a holder).

    ``count`` and ``queue_length`` keep their ``PriorityResource`` meaning.
    The monitor contract (D023 v2) is unchanged, and ``name`` is as for
    ``Resource``. Create it inside the run.
    """

    def __init__(
        self,
        capacity: int = 1,
        *,
        monitor: Callable[[PriorityResource], object] | None = None,
        name: str | None = None,
    ) -> None:
        super().__init__(capacity, monitor=monitor, name=name)
        # Every ticket that currently has a unit (reservation or holder).
        self._unit_tickets: list[_Ticket] = []
        # Plain ``async with res:`` per task, innermost last.
        self._plain: dict[asyncio.Task[Any], list[PreemptiveRequest]] = {}

    def request(self, priority: float = 0, preempt: bool = True) -> PreemptiveRequest:
        """Return a request to use as ``async with res.request(priority=p):``."""
        if not isinstance(preempt, bool):
            raise TypeError(f"preempt must be a bool, got {type(preempt).__name__}")
        base = super().request(priority)  # validates the priority
        return PreemptiveRequest(self, base.priority, preempt)

    # --- hooks called by PriorityResource._acquire -------------------------

    def _arriving(self, ticket: _Ticket | None) -> None:
        assert ticket is not None
        if (
            not ticket.preempt
            or self._free() > 0
            or self._blocked_by_waiter(ticket.priority)
        ):
            return
        # Never the preemptor's own task (sim.interrupt refuses to interrupt
        # the current task, and it would be pointless).
        candidates = [t for t in self._unit_tickets if t.task is not ticket.task]
        if not candidates:
            return
        victim = max(candidates, key=lambda t: (t.priority, t.seq))
        if victim.priority <= ticket.priority:
            return  # equal priority never preempts
        self._evict(victim, by=ticket.task)

    def _evict(self, victim: _Ticket, by: "asyncio.Task[Any]") -> None:
        """Take the unit from ``victim`` (synchronously) and tell it."""
        self._unit_tickets.remove(victim)
        victim.evicted = Preempted(by=by, usage_since=victim.since, resource=self)
        if victim.held:
            self._count -= 1
            interrupt_task(victim.task, victim.evicted)
        else:
            # Reservation: the task raises the Interrupt itself when it resumes.
            self._granted -= 1
        # The caller (_acquire's fast path) now assigns the freed unit and
        # calls the monitor once with the settled state.

    def _unit_assigned(self, ticket: _Ticket | None) -> None:
        assert ticket is not None
        ticket.since = asyncio.get_running_loop().time()
        self._unit_tickets.append(ticket)

    def _became_holder(self, ticket: _Ticket | None) -> None:
        assert ticket is not None
        ticket.held = True

    def _unit_dropped(self, ticket: _Ticket | None) -> None:
        assert ticket is not None
        self._unit_tickets.remove(ticket)

    def _release_ticket(self, ticket: _Ticket) -> None:
        self._unit_tickets.remove(ticket)
        self._release()

    # --- plain ``async with res:`` -----------------------------------------

    async def __aenter__(self) -> Self:
        task = asyncio.current_task()
        if task is None:
            raise RuntimeError("a PreemptiveResource must be used inside a task")
        req = self.request()
        await req.__aenter__()
        self._plain.setdefault(task, []).append(req)
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
        req = stack.pop()
        if not stack:
            del self._plain[task]
        await req.__aexit__(exc_type, exc, tb)
