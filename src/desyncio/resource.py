"""``Resource``: a counted resource with a FIFO queue (RES-01).

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
* the optional ``monitor`` hook (D021).
"""

import asyncio
from collections.abc import Callable
from types import TracebackType
from typing import Self


class Resource:
    """``capacity`` interchangeable units, handed out first-come first-served.

    ``monitor``, if given, is called as ``monitor(resource)`` synchronously
    after every state change: a request queueing, a request being granted,
    a queued request leaving (cancelled) and a release. Read ``count`` and
    ``queue_length`` inside it. It must not block or acquire the resource.
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
        if self._monitor is not None:
            self._monitor(self)

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
