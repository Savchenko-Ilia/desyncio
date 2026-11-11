"""``Store``, ``PriorityStore`` and ``FilterStore``: buffers of discrete items.

What asyncio does for us (principle 6, "reuse asyncio first")
-------------------------------------------------------------
``Store`` and ``PriorityStore`` keep their items in an ``asyncio.Queue`` /
``asyncio.PriorityQueue`` and use only its non-blocking public methods:

* FIFO order of items (``Queue``'s deque) and the heap order of
  ``PriorityQueue``;
* ``put_nowait`` / ``get_nowait``, which decide "full", "empty" and "shut
  down" (``QueueFull``, ``QueueEmpty``, ``QueueShutDown``);
* ``qsize()`` and ``shutdown()``: refusing new puts and (``immediate=True``)
  discarding queued items.

``FilterStore`` uses an ``asyncio.Lock`` for mutual exclusion and FIFO lock
hand-over. Every waiter is an asyncio future.

What is ours, and why (D075)
----------------------------
Parking blocked waiters. ``Queue.get``/``put`` and ``Condition.wait`` remove
a cancelled waiter's future with ``deque.remove`` (a linear scan), and
``Condition`` then rescans its deque to pass a notification on. Cancelling n
parked waiters (a ``TaskGroup`` abort, a mass interrupt, or the teardown at
the end of ``run(until=...)``) therefore costs O(n²): about 9 s for 50,000
getters on a ``Store`` and about 60 s for 50,000 on a ``FilterStore``. We
cannot change asyncio, so the waiting part is a small FIFO of futures,
:class:`_Waiters`, with the same semantics and O(1) amortised cancellation:

* a cancelled waiter does not search for its future; the future is just
  cancelled ("dead") and stays in the deque;
* waking pops futures from the head and skips dead ones, as
  ``Queue._wakeup_next`` already does, so each dead future is popped once;
* when dead futures could make up over half the deque, it is rebuilt without
  them, so memory stays bounded by about twice the live waiters.

The control flow of ``put``/``get`` is ``Queue.put``/``get``'s (adapted, see
the PSF note at :class:`_WaitingQueue`): park at the tail, re-check after a
wake-up (so a fresh call in the same instant can still barge, D031), and a
waiter that was woken but is cancelled before it ran passes the wake-up on to
the next waiter, so an item or a free slot is never lost. ``FilterStore``'s
wait is ``Condition.wait``'s (adapted, see :meth:`FilterStore._wait`).

Also ours: ``capacity`` validation and the mapping ``math.inf`` ->
``maxsize=0`` (asyncio's spelling of "unbounded"); ``len(store)``; the
``items`` snapshots; the ``monitor=`` hook (D021, D023 v2); like asyncio's
primitives, a store is bound to the first event loop a task waits on it in
(``RuntimeError`` on another loop).
"""

import asyncio
import itertools
import math
from collections import deque
from collections.abc import Callable
from typing import Any

from desyncio._monitor import report_monitor_error


def _check_capacity(capacity: float) -> None:
    """Validate ``capacity`` (shared by all stores): int >= 1 or ``math.inf``."""
    want = "capacity must be an int >= 1 or math.inf"
    if isinstance(capacity, bool) or not isinstance(capacity, (int, float)):
        raise TypeError(f"{want}, got {type(capacity).__name__}")
    if capacity != math.inf and (not isinstance(capacity, int) or capacity < 1):
        raise ValueError(f"{want}, got {capacity!r}")


def _check_monitor(monitor: object) -> None:
    """Validate ``monitor`` (shared by all stores): callable or ``None``."""
    if monitor is not None and not callable(monitor):
        name = type(monitor).__name__
        raise TypeError(f"monitor must be callable or None, got {name}")


def _call_monitor(monitor: "Callable[[Any], None] | None", store: object) -> None:
    """Call ``monitor(store)``, routing its errors to the loop (D023 v2).

    This mirrors how asyncio runs a callback (``Handle._run``): an
    ``Exception`` from the monitor is handed to
    ``loop.call_exception_handler`` (which logs it by default) instead of
    propagating, so the ``put``/``get`` that triggered it still completes
    and the item is never lost. ``KeyboardInterrupt``/``SystemExit`` are
    ``BaseException`` and propagate, as in asyncio. ``CancelledError`` is
    also a ``BaseException``; the stores call this only after their
    operation has finished, with no ``await`` left, so there is no pending
    cancellation to mask. Outside a running loop (e.g. ``shutdown()`` before
    ``run``) the error is logged on the ``desyncio`` logger instead (D045).
    """
    if monitor is None:
        return
    try:
        monitor(store)
    except Exception as exc:
        report_monitor_error("Store monitor raised", exc, "store", store)


# Rebuild a waiter deque only once this many cancellations have piled up, so
# a handful of cancellations never costs a rebuild.
_COMPACT_MIN = 32


class _Waiters:
    """A FIFO of parked waiter futures with O(1) amortised cancellation (D075).

    This replaces the ``deque`` of futures inside ``asyncio.Queue`` and
    ``asyncio.Condition``. The main difference is in how a cancelled waiter
    leaves: asyncio calls ``deque.remove(future)``, a scan that makes
    cancelling n waiters O(n²); here the future is merely cancelled and left in
    place ("lazy deletion"). A cancelled future is ``done()``, and every wake-up
    already skips done futures, so a dead future can never be woken and is
    popped at most once. The other difference is ``Condition``'s: a wake-up
    here pops every future it meets, the woken ones included (as
    ``Queue._wakeup_next`` does), whereas ``Condition._notify`` leaves woken
    futures in its deque for their waiters to remove. To keep memory bounded,
    the deque is rebuilt without its dead futures once more than half of it
    may be dead and there have been at least ``_COMPACT_MIN`` (32)
    cancellations since the last rebuild.

    Futures are woken with ``set_result(None)``: the waiting task is scheduled
    with ``call_soon`` and runs in a later step, in wake-up order.
    """

    __slots__ = ("_futures", "_cancelled")

    def __init__(self) -> None:
        self._futures: deque[asyncio.Future[None]] = deque()
        # Cancellations since the last rebuild: an upper bound on the dead
        # futures still in the deque (one may already have been popped by a
        # wake-up before its task ran ``discard``).
        self._cancelled = 0

    def park(self, loop: asyncio.AbstractEventLoop) -> "asyncio.Future[None]":
        """Append a new pending future at the tail and return it."""
        future: asyncio.Future[None] = loop.create_future()
        self._futures.append(future)
        return future

    def discard(self, future: "asyncio.Future[None]") -> None:
        """Forget ``future``, whose waiter is leaving with an exception.

        This is the O(1) replacement of asyncio's ``deque.remove``. The
        future is cancelled if it is still pending ("just in case", as
        asyncio does), which marks it dead where it stands. A future that
        was already woken (it has a result) was popped by the wake-up, so
        there is nothing to forget.
        """
        future.cancel()
        if not future.cancelled():
            return
        self._cancelled += 1
        if self._cancelled >= _COMPACT_MIN and 2 * self._cancelled > len(self._futures):
            # O(len) at most once per len/2 cancellations: O(1) amortised.
            self._futures = deque(f for f in self._futures if not f.done())
            self._cancelled = 0

    # Adapted from CPython Lib/asyncio/queues.py, Queue._wakeup_next and the
    # waking loops of Queue.shutdown, and Lib/asyncio/locks.py,
    # Condition._notify (Python 3.13; unchanged in 3.14).
    # Copyright (c) 2001 Python Software Foundation. Licensed under the PSF
    # License Agreement v2; see LICENSES/PSF.txt.
    # Changes from the original (wake, and wake_all through it): one loop for
    # all three, waking up to n futures (_wakeup_next wakes 1, shutdown wakes
    # all); like _wakeup_next and shutdown, every future met is popped,
    # including the woken ones, whereas Condition._notify leaves its futures
    # in place and each waiter removes its own after waking.
    def wake(self, n: int = 1) -> None:
        """Wake the ``n`` longest-waiting live waiters (fewer if there are fewer).

        Dead futures met on the way are popped and dropped: this is
        ``Queue._wakeup_next`` (``n=1``) and ``Condition._notify``.
        """
        futures = self._futures
        while n > 0 and futures:
            future = futures.popleft()
            if not future.done():
                future.set_result(None)
                n -= 1

    def wake_all(self) -> None:
        """Wake every live waiter (``Condition.notify_all``, ``Queue.shutdown``)."""
        self.wake(len(self._futures))


class _LoopBinding:
    """Bind a store to the first loop a task waits on it in, as asyncio does.

    asyncio's primitives (``Queue``, ``Condition``, ``Lock``) raise
    ``RuntimeError`` when used from a second event loop. The stores used to
    get this from ``Queue``/``Condition``; now that they park their own
    futures, they keep the same rule here.
    """

    __slots__ = ("_loop",)

    def __init__(self) -> None:
        self._loop: asyncio.AbstractEventLoop | None = None

    # Adapted from CPython Lib/asyncio/mixins.py, _LoopBoundMixin._get_loop
    # (Python 3.13; unchanged in 3.14).
    # Copyright (c) 2001 Python Software Foundation. Licensed under the PSF
    # License Agreement v2; see LICENSES/PSF.txt.
    # Changes from the original: the bound loop lives in this helper, not in
    # a mixin of the store, and the owner to name in the error is passed in;
    # no global lock around the first binding (it only guarded threads, and
    # a store is used from its loop's thread).
    def get(self, owner: object) -> asyncio.AbstractEventLoop:
        """Return the running loop, binding to it on first use."""
        loop = asyncio.get_running_loop()
        if self._loop is None:
            self._loop = loop
        if loop is not self._loop:
            raise RuntimeError(f"{owner!r} is bound to a different event loop")
        return loop


class _WaitingQueue[E]:
    """An ``asyncio.Queue`` (or subclass) with our own blocking ``put``/``get``.

    The queue holds the entries and decides full/empty/shut down through its
    public ``put_nowait``/``get_nowait``. It never has waiters of its own:
    blocked callers park in our two :class:`_Waiters` instead (D075).
    """

    __slots__ = ("queue", "_getters", "_putters", "_binding")

    def __init__(self, queue: "asyncio.Queue[E]") -> None:
        self.queue = queue
        self._getters = _Waiters()
        self._putters = _Waiters()
        self._binding = _LoopBinding()

    # Adapted from CPython Lib/asyncio/queues.py, Queue.put and Queue.get
    # (Python 3.13; unchanged in 3.14).
    # Copyright (c) 2001 Python Software Foundation. Licensed under the PSF
    # License Agreement v2; see LICENSES/PSF.txt.
    # Changes from the original: the waiter deques are _Waiters, so a leaving
    # waiter calls discard() instead of deque.remove(); the full/empty/shut-down
    # checks are delegated to put_nowait/get_nowait (which raise QueueShutDown
    # before QueueFull, and QueueShutDown only when empty, exactly as the
    # original loops do); the wake-up of the other side moved here from
    # put_nowait/get_nowait.
    async def put(self, entry: E) -> None:
        """Add ``entry``, parking FIFO while the queue is full."""
        while True:
            try:
                self.queue.put_nowait(entry)
            except asyncio.QueueFull:
                putter = self._putters.park(self._binding.get(self.queue))
                try:
                    await putter
                except BaseException:
                    self._putters.discard(putter)
                    if not self.queue.full() and not putter.cancelled():
                        # Woken by a get, but leaving: wake the next in line.
                        self._putters.wake()
                    raise
                # Woken: loop and re-check (a same-instant put may have barged).
            else:
                self._getters.wake()
                return

    async def get(self) -> E:
        """Remove and return the next entry, parking FIFO while empty."""
        while True:
            try:
                entry = self.queue.get_nowait()
            except asyncio.QueueEmpty:
                getter = self._getters.park(self._binding.get(self.queue))
                try:
                    await getter
                except BaseException:
                    self._getters.discard(getter)
                    if not self.queue.empty() and not getter.cancelled():
                        # Woken by a put, but leaving: wake the next in line.
                        self._getters.wake()
                    raise
                # Woken: loop and re-check (a same-instant get may have barged).
            else:
                self._putters.wake()
                return entry

    def shutdown(self, immediate: bool) -> None:
        """``Queue.shutdown``, then wake every parked getter and putter.

        Woken waiters re-check and get ``QueueShutDown`` (getters only once the
        queue is empty), the same order and outcome as ``Queue.shutdown``.
        """
        self.queue.shutdown(immediate)
        self._getters.wake_all()
        self._putters.wake_all()


class Store[T]:
    """A FIFO buffer of items with optional capacity, for simulation processes.

    Usage::

        store = Store(capacity=2)
        await store.put(item)      # blocks (in virtual time) while full
        item = await store.get()   # blocks (in virtual time) while empty

    The items live in an ``asyncio.Queue``; blocked callers wait in FIFO
    order with ``asyncio.Queue``'s semantics, but in our own waiter queue so
    that cancelling many of them is linear (D075). See the module docstring
    for exactly what is delegated and what this class adds.

    Args:
        capacity: maximum number of items held; an ``int >= 1`` or
            ``math.inf`` (the default, unbounded). ``inf`` is passed to the
            queue as ``maxsize=0``, which asyncio treats as "no limit".
        monitor: optional callable, called as ``monitor(store)`` right after
            each completed ``put`` or ``get`` (D021). It runs synchronously
            in the task that completed the operation, at the current virtual
            time, once the store's state is settled (D023). An ``Exception``
            it raises does not reach that task: it is passed to
            ``loop.call_exception_handler`` and the operation completes
            normally (D023 v2). It is also called after
            ``shutdown(immediate=True)`` discards the queued items. It is not
            called for operations that were cancelled or refused by
            ``shutdown``. Monitors must not block, and must not acquire the
            store (no ``put``/``get`` from inside the monitor).

    Raises:
        TypeError: ``capacity`` is not an int or ``math.inf`` (``bool`` is
            rejected), or ``monitor`` is not callable.
        ValueError: ``capacity`` is below 1, ``nan``, or a non-infinite float.
    """

    def __init__(
        self,
        capacity: float = math.inf,
        *,
        monitor: "Callable[[Store[T]], None] | None" = None,
    ) -> None:
        _check_capacity(capacity)
        _check_monitor(monitor)
        self._capacity = capacity
        self._queue: _WaitingQueue[T] = _WaitingQueue(
            asyncio.Queue(maxsize=0 if capacity == math.inf else int(capacity))
        )
        # Parallel public record of the queued items, only so ``items`` does
        # not need ``Queue._queue`` (private). It holds no queueing logic: it
        # is appended/popped in the same synchronous step in which the queue
        # completes a put/get (there is no ``await`` between them), so it
        # always mirrors the queue's contents.
        self._items: deque[T] = deque()
        self._monitor = monitor

    @property
    def capacity(self) -> float:
        """Maximum number of items (read-only); ``math.inf`` if unbounded."""
        return self._capacity

    @property
    def items(self) -> tuple[T, ...]:
        """Snapshot of the queued items, oldest first (read-only, a copy)."""
        return tuple(self._items)

    def __len__(self) -> int:
        """Number of queued items (``Queue.qsize()``)."""
        return self._queue.queue.qsize()

    async def put(self, item: T) -> None:
        """Add ``item``, waiting while the store is full.

        Blocked putters are served FIFO. A putter cancelled after a slot was
        handed to it, but before it ran, passes the slot on to the next
        putter. Raises ``asyncio.QueueShutDown`` after ``shutdown()``.
        """
        await self._queue.put(item)
        self._items.append(item)
        self._notify()

    async def get(self) -> T:
        """Remove and return the oldest item, waiting while the store is empty.

        Blocked getters are served FIFO. An item is only removed in the step
        in which ``get`` returns it, so if the caller is cancelled after an
        item was handed to it but before it ran, the item stays queued and is
        passed on to the next getter: it is never lost or duplicated.
        Raises ``asyncio.QueueShutDown`` once shut down and empty.
        """
        item = await self._queue.get()
        self._items.popleft()
        self._notify()
        return item

    def _notify(self) -> None:
        """Call the monitor through the shared D023 v2 helper."""
        _call_monitor(self._monitor, self)

    def shutdown(self, immediate: bool = False) -> None:
        """Shut the store down (``Queue.shutdown``, then wake every waiter).

        Later ``put`` calls raise ``asyncio.QueueShutDown``; blocked waiters
        are woken with it. With ``immediate=False`` queued items can still be
        drained by ``get``; with ``immediate=True`` they are discarded and
        the monitor is called, since discarding items is a state change
        (D023).
        """
        self._queue.shutdown(immediate)
        if immediate:
            self._items.clear()  # keep the mirror in step with the queue
            self._notify()


class PriorityStore[T]:
    """A store that hands out the item with the lowest priority value first.

    The items live in an ``asyncio.PriorityQueue`` (principle 6, D022);
    blocked callers wait as in :class:`Store` (D075).

    Usage::

        store = PriorityStore(capacity=10)
        await store.put(job, priority=2)   # lower number = served earlier
        job = await store.get()            # best priority, ties in put order

    How the priority is supplied: ``put(item, priority=0)``. This follows
    SimPy's ``PriorityItem(priority, item)`` ergonomics without a wrapper
    class, and the item itself never has to carry or expose a priority. If the
    priority is a function of the item, call ``put(job, priority=key(job))``.
    ``priority`` values must be comparable with each other (numbers, tuples);
    items never are.

    Delegated to asyncio (``PriorityQueue``): the heap ordering, "full",
    "empty" and ``shutdown()``. Shared with :class:`Store`: blocking ``put``
    while full and ``get`` while empty, FIFO order of blocked putters and of
    blocked getters, and cancellation cleanup (a cancelled getter never loses
    or duplicates an item, also when cancelled at the instant an item
    arrives).

    Ours: ``capacity`` validation and the ``inf`` -> ``maxsize=0`` mapping;
    storing each item as ``(priority, sequence, item)`` where ``sequence``
    comes from ``itertools.count()``, so equal priorities come out in put
    order and the heap never compares two items; ``len()``; the ``items``
    snapshot; the ``monitor=`` hook.

    The sequence number is taken when ``put`` is called, so tied items leave
    in the order their ``put`` calls were made (blocked putters are served
    FIFO, which is the same order).

    Args:
        capacity: maximum number of items; an ``int >= 1`` or ``math.inf``.
        monitor: optional ``monitor(store)`` called synchronously after each
            completed ``put``/``get`` and after ``shutdown(immediate=True)``,
            with the same contract as :class:`Store` (D023 v2).
    """

    def __init__(
        self,
        capacity: float = math.inf,
        *,
        monitor: "Callable[[PriorityStore[T]], None] | None" = None,
    ) -> None:
        _check_capacity(capacity)
        _check_monitor(monitor)
        self._capacity = capacity
        self._queue: _WaitingQueue[tuple[float, int, T]] = _WaitingQueue(
            asyncio.PriorityQueue(maxsize=0 if capacity == math.inf else int(capacity))
        )
        self._seq = itertools.count()
        # Public-API mirror of the queued entries, keyed by sequence number,
        # so ``items`` does not need ``PriorityQueue._queue`` (private). Same
        # discipline as ``Store``: updated in the same synchronous step in
        # which the queue completes a put/get.
        self._entries: dict[int, tuple[float, int, T]] = {}
        self._monitor = monitor

    @property
    def capacity(self) -> float:
        """Maximum number of items (read-only); ``math.inf`` if unbounded."""
        return self._capacity

    @property
    def items(self) -> tuple[T, ...]:
        """Snapshot of the queued items in the order ``get`` will return them.

        Best priority first, ties in put order; read-only, a copy.
        """
        ordered = sorted(self._entries.values(), key=lambda e: (e[0], e[1]))
        return tuple(e[2] for e in ordered)

    def __len__(self) -> int:
        """Number of queued items (``Queue.qsize()``)."""
        return self._queue.queue.qsize()

    async def put(self, item: T, priority: float = 0) -> None:
        """Add ``item`` with ``priority`` (lower is served first).

        Waits while the store is full; blocked putters are served FIFO, as in
        :meth:`Store.put`. Raises ``asyncio.QueueShutDown`` after
        ``shutdown()``.
        """
        entry = (priority, next(self._seq), item)
        await self._queue.put(entry)
        self._entries[entry[1]] = entry
        _call_monitor(self._monitor, self)

    async def get(self) -> T:
        """Remove and return the best-priority item, waiting while empty.

        Blocked getters are served FIFO, as in :meth:`Store.get`: an item is
        only removed when ``get`` actually returns it, so a getter cancelled
        or interrupted at the instant an item arrives loses nothing. Raises
        ``asyncio.QueueShutDown`` once shut down and empty.
        """
        entry = await self._queue.get()
        del self._entries[entry[1]]
        _call_monitor(self._monitor, self)
        return entry[2]

    def shutdown(self, immediate: bool = False) -> None:
        """Shut the store down; same semantics as :meth:`Store.shutdown`."""
        self._queue.shutdown(immediate)
        if immediate:
            self._entries.clear()
            _call_monitor(self._monitor, self)


class FilterStore[T]:
    """A FIFO store whose ``get`` can ask for the first item matching a filter.

    asyncio has no equivalent. This is ``asyncio.Condition``'s pattern
    (``wait_for`` under a lock) with the ``Condition``'s waiter deque replaced
    by our :class:`_Waiters`, so that cancelling many parked tasks is linear
    (D075); the lock is a plain ``asyncio.Lock``.

    Usage::

        store = FilterStore()
        await store.put(part)
        part = await store.get(filter=lambda p: p.kind == "wheel")
        any_part = await store.get()          # no filter: plain FIFO

    Delegated to asyncio's ``Lock``: mutual exclusion and FIFO lock
    hand-over. Kept from ``Condition`` (adapted, see :meth:`_wait`):

    * ``wait_for(predicate)`` parks a task and re-runs the predicate each time
      the store changes, so ``get`` blocks until a match exists and ``put``
      blocks while full;
    * cancellation and ``Interrupt`` (D024): the wait re-acquires the lock
      even when cancelled and then re-raises, and a waiter leaving with an
      exception passes one wake-up on (as ``Condition.wait`` does). The item
      is removed only by the predicate, in the same synchronous step in which
      ``get`` returns, so a getter cancelled at the instant an item arrives
      loses and duplicates nothing.

    Ours: the item ``deque``, the matching scan, ``capacity`` validation,
    ``len()``, the ``items`` snapshot and the ``monitor=`` hook.

    Who gets an item. Every change wakes every parked task (``notify_all``);
    they are woken in the order they parked and the lock lets them in the
    same order, so blocked getters are tried longest-waiting first, and the
    first one whose filter matches takes the first matching item (FIFO order
    of items). A getter whose filter matches nothing just parks again, so it
    never blocks a later getter (no head-of-line blocking). Blocked putters
    are served in the same way, FIFO. Like ``asyncio.Queue``, a call arriving
    in the very instant of a change may use the item or the free slot before
    tasks that were already woken have run. Everything is deterministic: the
    same model and seed give the same order.

    A filter that raises: the exception is raised from ``get`` in the task
    whose filter it was, nothing is removed, the lock is released and other
    waiters are unaffected.

    No ``shutdown()``: it is omitted by decision D031, which was taken when
    the waiters were parked in an ``asyncio.Condition`` (waking them needed
    the lock, so an ``await``). With D075's waiter FIFO, waking them is
    synchronous (:meth:`_Waiters.wake_all`), so a synchronous ``shutdown``
    like ``Queue.shutdown`` would now be possible; adding it is a follow-up.

    Args:
        capacity: maximum number of items; an ``int >= 1`` or ``math.inf``.
        monitor: optional ``monitor(store)`` called synchronously after each
            completed ``put``/``get``, once the store is settled, with the
            same contract as :class:`Store` (D023 v2).
    """

    def __init__(
        self,
        capacity: float = math.inf,
        *,
        monitor: "Callable[[FilterStore[T]], None] | None" = None,
    ) -> None:
        _check_capacity(capacity)
        _check_monitor(monitor)
        self._capacity = capacity
        self._items: deque[T] = deque()
        # One lock and one waiter queue guard the items and both kinds of
        # waiters (as one Condition did).
        self._lock = asyncio.Lock()
        self._waiters = _Waiters()
        self._binding = _LoopBinding()
        self._monitor = monitor

    @property
    def capacity(self) -> float:
        """Maximum number of items (read-only); ``math.inf`` if unbounded."""
        return self._capacity

    @property
    def items(self) -> tuple[T, ...]:
        """Snapshot of the stored items, oldest first (read-only, a copy)."""
        return tuple(self._items)

    def __len__(self) -> int:
        """Number of stored items."""
        return len(self._items)

    async def put(self, item: T) -> None:
        """Add ``item``, waiting (FIFO among putters) while the store is full."""
        async with self._lock:
            await self._wait_for(lambda: len(self._items) < self._capacity)
            self._items.append(item)
            self._waiters.wake_all()  # a getter may now find a match
        # No await after the append, so no cancellation can land between the
        # state change and the monitor call.
        _call_monitor(self._monitor, self)

    async def get(self, filter: "Callable[[T], bool] | None" = None) -> T:  # noqa: A002
        """Remove and return the first item (FIFO) for which ``filter`` is true.

        Waits until there is one. Without a filter any item matches. Raises
        whatever ``filter`` raises, in this caller only.
        """
        if filter is not None and not callable(filter):
            name = type(filter).__name__
            raise TypeError(f"filter must be callable or None, got {name}")
        taken: list[T] = []

        def take_first_match() -> bool:
            for index, candidate in enumerate(self._items):
                if filter is None or filter(candidate):
                    del self._items[index]
                    taken.append(candidate)
                    return True
            return False

        async with self._lock:
            # ``_wait_for`` returns as soon as the predicate is true, with no
            # await in between, so the removal inside it is atomic with
            # ``get`` returning.
            await self._wait_for(take_first_match)
            self._waiters.wake_all()  # a blocked putter may now fit
        _call_monitor(self._monitor, self)
        return taken[0]

    async def _wait_for(self, predicate: Callable[[], bool]) -> None:
        """``Condition.wait_for``: wait (lock held) until ``predicate()`` is true."""
        while not predicate():
            await self._wait()

    # Adapted from CPython Lib/asyncio/locks.py, Condition.wait (Python 3.13;
    # unchanged in 3.14).
    # Copyright (c) 2001 Python Software Foundation. Licensed under the PSF
    # License Agreement v2; see LICENSES/PSF.txt.
    # Changes from the original: the waiter deque is _Waiters, so a leaving
    # waiter calls discard() instead of deque.remove(), and passing a
    # notification on (Condition._notify(1)) skips dead futures by popping
    # them; no "un-acquired lock" check (only called with the lock held); no
    # return value.
    async def _wait(self) -> None:
        """Release the lock, park until woken, then re-acquire the lock.

        The lock is re-acquired even if the wait is cancelled, before the
        ``CancelledError`` propagates, so ``async with self._lock`` always
        exits holding it. A waiter leaving with an exception wakes one other
        waiter, since it may have consumed a wake-up meant for "someone":
        that may cause a spurious wake-up, which ``_wait_for`` tolerates.
        """
        waiter = self._waiters.park(self._binding.get(self))
        self._lock.release()
        try:
            try:
                try:
                    await waiter
                except BaseException:
                    self._waiters.discard(waiter)
                    raise
            finally:
                # Must re-acquire the lock even if the wait is cancelled. Only
                # CancelledError is caught, so other (fatal) errors can't spin.
                err: asyncio.CancelledError | None = None
                while True:
                    try:
                        await self._lock.acquire()
                        break
                    except asyncio.CancelledError as e:
                        err = e
                if err is not None:
                    try:
                        raise err  # re-raise the most recent CancelledError
                    finally:
                        err = None  # break the reference cycle
        except BaseException:
            self._waiters.wake(1)
            raise
