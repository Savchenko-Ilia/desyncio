"""``Store``: a buffer of discrete items, a thin wrapper over ``asyncio.Queue``.

What asyncio does for us (principle 6, "reuse asyncio first")
-------------------------------------------------------------
``asyncio.Queue`` already *is* a SimPy-style store, and it only uses the
event loop's ``call_soon`` and futures, so it works unchanged in virtual time.
Everything below is delegated to it, not re-implemented:

* FIFO order of items (an internal ``deque``);
* FIFO order of blocked getters and of blocked putters (two ``deque`` of
  futures);
* blocking ``put`` while full and ``get`` while empty;
* cancellation cleanup: a cancelled waiter removes its own future from the
  waiter deque, and if it had already been woken it passes the wake-up on to
  the next waiter, so an item is never lost or duplicated;
* ``shutdown()``: refuses new puts, wakes every blocked waiter with
  ``asyncio.QueueShutDown``, and (``immediate=True``) discards queued items.

What is ours (small on purpose)
-------------------------------
* ``capacity`` validation and the mapping ``math.inf`` -> ``maxsize=0``
  (asyncio's spelling of "unbounded");
* ``len(store)`` (forwards to ``Queue.qsize()``);
* the ``items`` snapshot (see below);
* the ``monitor=`` hook decided in D021.
"""

import asyncio
import itertools
import math
from collections import deque
from collections.abc import Callable
from typing import Any


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
    cancellation to mask.
    """
    if monitor is None:
        return
    try:
        monitor(store)
    except Exception as exc:
        asyncio.get_running_loop().call_exception_handler(
            {"message": "Store monitor raised", "exception": exc, "store": store}
        )


class Store[T]:
    """A FIFO buffer of items with optional capacity, for simulation processes.

    Usage::

        store = Store(capacity=2)
        await store.put(item)      # blocks (in virtual time) while full
        item = await store.get()   # blocks (in virtual time) while empty

    Every blocking behaviour is ``asyncio.Queue``'s; see the module docstring
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
        self._queue: asyncio.Queue[T] = asyncio.Queue(
            maxsize=0 if capacity == math.inf else int(capacity)
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
        return self._queue.qsize()

    async def put(self, item: T) -> None:
        """Add ``item``, waiting while the store is full.

        Waiting, FIFO ordering of blocked putters and cancellation cleanup
        are ``asyncio.Queue.put``'s. Raises ``asyncio.QueueShutDown`` after
        ``shutdown()``.
        """
        await self._queue.put(item)
        self._items.append(item)
        self._notify()

    async def get(self) -> T:
        """Remove and return the oldest item, waiting while the store is empty.

        Waiting, FIFO ordering of blocked getters and cancellation cleanup
        are ``asyncio.Queue.get``'s. If the caller is cancelled after an item
        was handed to it but before it ran, asyncio leaves the item queued
        (or passes it to the next getter), so it is never lost or duplicated.
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
        """Shut the store down; a pure passthrough to ``Queue.shutdown``.

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

    A thin wrapper over ``asyncio.PriorityQueue`` (principle 6, D022).

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

    Delegated to asyncio (``PriorityQueue``): the heap ordering, blocking
    ``put`` while full and ``get`` while empty, FIFO order of blocked putters
    and of blocked getters, cancellation cleanup (a cancelled getter never
    loses or duplicates an item, also when cancelled at the instant an item
    arrives), and ``shutdown()``.

    Ours: ``capacity`` validation and the ``inf`` -> ``maxsize=0`` mapping;
    storing each item as ``(priority, sequence, item)`` where ``sequence``
    comes from ``itertools.count()``, so equal priorities come out in put
    order and the heap never compares two items; ``len()``; the ``items``
    snapshot; the ``monitor=`` hook.

    The sequence number is taken when ``put`` is called, so tied items leave
    in the order their ``put`` calls were made (blocked putters are served
    FIFO by asyncio, which is the same order).

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
        self._queue: asyncio.PriorityQueue[tuple[float, int, T]] = (
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
        return self._queue.qsize()

    async def put(self, item: T, priority: float = 0) -> None:
        """Add ``item`` with ``priority`` (lower is served first).

        Waits while the store is full. Waiting, FIFO order of blocked putters
        and cancellation cleanup are ``asyncio.PriorityQueue.put``'s. Raises
        ``asyncio.QueueShutDown`` after ``shutdown()``.
        """
        entry = (priority, next(self._seq), item)
        await self._queue.put(entry)
        self._entries[entry[1]] = entry
        _call_monitor(self._monitor, self)

    async def get(self) -> T:
        """Remove and return the best-priority item, waiting while empty.

        Waiting, FIFO order of blocked getters and cancellation cleanup are
        ``asyncio.PriorityQueue.get``'s: an item is only removed when ``get``
        actually returns it, so a getter cancelled or interrupted at the
        instant an item arrives loses nothing. Raises
        ``asyncio.QueueShutDown`` once shut down and empty.
        """
        entry = await self._queue.get()
        del self._entries[entry[1]]
        _call_monitor(self._monitor, self)
        return entry[2]

    def shutdown(self, immediate: bool = False) -> None:
        """Shut the store down; a passthrough to ``Queue.shutdown``.

        Same semantics as :meth:`Store.shutdown`.
        """
        self._queue.shutdown(immediate)
        if immediate:
            self._entries.clear()
            _call_monitor(self._monitor, self)


class FilterStore[T]:
    """A FIFO store whose ``get`` can ask for the first item matching a filter.

    asyncio has no equivalent, so this is built on ``asyncio.Condition``
    (principle 6, D022), the same way ``Container`` is.

    Usage::

        store = FilterStore()
        await store.put(part)
        part = await store.get(filter=lambda p: p.kind == "wheel")
        any_part = await store.get()          # no filter: plain FIFO

    Delegated to the ``Condition`` (and its ``Lock``):

    * ``wait_for(predicate)`` parks a task and re-runs the predicate each time
      the store changes, so ``get`` blocks until a match exists and ``put``
      blocks while full;
    * mutual exclusion and FIFO lock hand-over (a ``Lock`` is FIFO);
    * cancellation and ``Interrupt`` (D024): ``Condition.wait`` re-acquires the
      lock even when cancelled and then re-raises. The item is removed only by
      the predicate, in the same synchronous step in which ``get`` returns, so
      a getter cancelled at the instant an item arrives loses and duplicates
      nothing.

    Ours: the item ``deque``, the matching scan, ``capacity`` validation,
    ``len()``, the ``items`` snapshot and the ``monitor=`` hook.

    Who gets an item. Every change calls ``notify_all()``; ``Condition`` wakes
    parked tasks in the order they parked and the lock lets them in the same
    order, so blocked getters are tried longest-waiting first, and the first
    one whose filter matches takes the first matching item (FIFO order of
    items). A getter whose filter matches nothing just parks again, so it
    never blocks a later getter (no head-of-line blocking). Blocked putters
    are served in the same way, FIFO. Like ``asyncio.Queue``, a call arriving
    in the very instant of a change may use the item or the free slot before
    tasks that were already woken have run. Everything is deterministic: the
    same model and seed give the same order.

    A filter that raises: the exception is raised from ``get`` in the task
    whose filter it was, nothing is removed, the lock is released and other
    waiters are unaffected.

    No ``shutdown()``: ``Condition.notify_all`` may only be called with the
    lock held, which needs an ``await``, but ``shutdown`` is a synchronous
    call in ``asyncio.Queue``. It is omitted rather than half-implemented.

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
        # One Condition (and so one Lock) guards the items and both kinds of
        # waiters.
        self._cond = asyncio.Condition()
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
        async with self._cond:
            await self._cond.wait_for(lambda: len(self._items) < self._capacity)
            self._items.append(item)
            self._cond.notify_all()  # a getter may now find a match
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

        async with self._cond:
            # ``wait_for`` returns as soon as the predicate is true, with no
            # await in between, so the removal inside it is atomic with
            # ``get`` returning.
            await self._cond.wait_for(take_first_match)
            self._cond.notify_all()  # a blocked putter may now fit
        _call_monitor(self._monitor, self)
        return taken[0]
