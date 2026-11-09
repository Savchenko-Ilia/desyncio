"""``Resource`` (RES-01) and ``PriorityResource`` (RES-02).

``PriorityResource`` is described at the end of this docstring; the rest
describes ``Resource``, a counted resource with a FIFO queue.

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
from types import TracebackType
from typing import Self


class Resource:
    """``capacity`` interchangeable units, handed out first-come first-served.

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
    ) -> None:
        if isinstance(capacity, bool) or not isinstance(capacity, int):
            raise TypeError(f"capacity must be an int, got {type(capacity).__name__}")
        if capacity < 1:
            raise ValueError(f"capacity must be >= 1, got {capacity}")
        if monitor is not None and not callable(monitor):
            raise TypeError(f"monitor must be callable, got {type(monitor).__name__}")
        self._capacity = capacity
        self._monitor = monitor
        self._sem = asyncio.Semaphore(capacity)  # does all waiting and queueing
        self._count = 0
        self._queue_length = 0

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
        """Call the monitor; called only once the state is fully settled.

        Reuses asyncio's callback error handling (``Handle._run``): an
        ``Exception`` goes to the loop's exception handler instead of the
        calling task, so it can neither leave the resource half-updated nor
        mask an exception (e.g. ``CancelledError``) already in flight.
        """
        if self._monitor is None:
            return
        try:
            self._monitor(self)
        except Exception as exc:  # KeyboardInterrupt/SystemExit propagate
            asyncio.get_running_loop().call_exception_handler(
                {
                    "message": "Resource monitor raised",
                    "exception": exc,
                    "resource": self,
                }
            )

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
    ``Resource``: ``queue_length`` counts a request that has been handed a
    unit but whose task has not resumed yet, and ``count`` includes it only
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
    ) -> None:
        if isinstance(capacity, bool) or not isinstance(capacity, int):
            raise TypeError(f"capacity must be an int, got {type(capacity).__name__}")
        if capacity < 1:
            raise ValueError(f"capacity must be >= 1, got {capacity}")
        if monitor is not None and not callable(monitor):
            raise TypeError(f"monitor must be callable, got {type(monitor).__name__}")
        self._capacity = capacity
        self._monitor = monitor
        # Waiters in key order. The seq number is unique, so tuples compare on
        # (priority, seq) only and never reach the (uncomparable) future.
        self._waiters: list[tuple[float, int, asyncio.Future[None]]] = []
        self._seq = itertools.count()
        self._count = 0  # units held by tasks that have resumed
        self._granted = 0  # units handed to a waiter that has not resumed yet

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
        if self._monitor is None:
            return
        try:
            self._monitor(self)
        except Exception as exc:  # KeyboardInterrupt/SystemExit propagate
            asyncio.get_running_loop().call_exception_handler(
                {
                    "message": "Resource monitor raised",
                    "exception": exc,
                    "resource": self,
                }
            )

    def _free(self) -> int:
        return self._capacity - self._count - self._granted

    def _grant_free_units(self) -> None:
        """Hand every free unit to the best live waiters, synchronously.

        Like ``Semaphore._wake_up_next``: a waiter whose future is already
        done was cancelled while pending; it is skipped here and removes
        itself from the order when its task runs.
        """
        i = 0
        while self._free() > 0 and i < len(self._waiters):
            fut = self._waiters[i][2]
            if fut.done():
                i += 1
                continue
            del self._waiters[i]
            self._granted += 1
            fut.set_result(None)  # the waiter resumes at its next turn

    async def _acquire(self, priority: float) -> None:
        key = (priority, next(self._seq))
        # Fast path, no suspension (as Semaphore.acquire). Release hands
        # units over at once, so a free unit means no live waiter is left;
        # the explicit check states the no-barging rule anyway: a live waiter
        # with a better-or-equal key (any priority <= ours, since our seq is
        # the newest) keeps the newcomer out.
        if self._free() > 0 and not any(
            not fut.done() and p <= priority for p, _, fut in self._waiters
        ):
            self._count += 1
            self._changed()
            return

        fut: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        entry = (priority, key[1], fut)
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
                self._granted -= 1
            else:
                # Still in the order: leave it.
                del self._waiters[bisect_left(self._waiters, key)]
            # Wake whoever can now go (Semaphore's ``finally`` does the same).
            self._grant_free_units()
            self._changed()
            raise
        # Granted by _grant_free_units, which already removed our entry.
        self._granted -= 1
        self._count += 1
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
