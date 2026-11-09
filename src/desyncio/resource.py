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

PriorityResource: Condition, not hand-rolled futures
-----------------------------------------------------
asyncio has no priority semaphore. ``asyncio.Condition`` can express priority
order, although its own wake-up order is FIFO: every waiter sleeps in
``Condition.wait_for(predicate)`` and the predicate is "I hold the best
``(priority, request order)`` key among the waiters, and a unit is free".
Every state change calls ``notify_all()``; all waiters wake, re-check, and
only the best one proceeds, the rest go back to sleep. So no per-request
futures are needed.

Delegated to asyncio: the sleeping and waking (``Condition``), cancellation
(``wait`` re-acquires its lock and re-raises ``CancelledError``, which is also
how ``sim.interruptible()`` arrives, D024), and the lock around the state.
Ours: the sorted list of waiting keys (asyncio has no priority order to
borrow), the counters, validation and the monitor. Cost: a state change wakes
every waiter (O(waiters)); acceptable for a simulation library, and simple.

Because a unit is taken only inside the predicate, in the same step that
ends the wait, a unit is never "in transit" to a waiter: a waiter cancelled
at the instant a unit frees up simply leaves, and the next waiter is woken.
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
        await self._resource._release()


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
    ``Resource``; see the module docstring for how waiting is built.
    Create it inside the run.
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
        self._cond = asyncio.Condition()  # does all waiting and waking
        self._waiting: list[tuple[float, int]] = []  # sorted keys of waiters
        self._seq = itertools.count()
        self._count = 0

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
        """Requests waiting for a unit."""
        return len(self._waiting)

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

    def _my_turn(self, key: tuple[float, int]) -> bool:
        return self._waiting[0] == key and self._count < self._capacity

    async def _acquire(self, priority: float) -> None:
        key = (priority, next(self._seq))
        # The lock is never held across a suspension except inside
        # Condition.wait (which releases it), so this never actually waits.
        async with self._cond:
            insort(self._waiting, key)
            try:
                if not self._my_turn(key):
                    self._changed()  # queued
                    await self._cond.wait_for(lambda: self._my_turn(key))
            except BaseException:
                # Cancelled or interrupted while queued. wait() has re-acquired
                # the lock. No unit was taken, so none can leak; wake the rest
                # because we may have been the head they were waiting behind.
                del self._waiting[bisect_left(self._waiting, key)]
                self._cond.notify_all()
                self._changed()
                raise
            self._waiting.pop(0)  # we are the head
            self._count += 1
            if self._waiting:
                self._cond.notify_all()  # more units may be free for the next
            self._changed()

    async def _release(self) -> None:
        async with self._cond:
            self._count -= 1
            self._cond.notify_all()
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
        await self._release()
