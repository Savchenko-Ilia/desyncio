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
import math
from collections import deque
from collections.abc import Callable


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
        want = "capacity must be an int >= 1 or math.inf"
        if isinstance(capacity, bool) or not isinstance(capacity, (int, float)):
            raise TypeError(f"{want}, got {type(capacity).__name__}")
        if capacity != math.inf and (not isinstance(capacity, int) or capacity < 1):
            raise ValueError(f"{want}, got {capacity!r}")
        if monitor is not None and not callable(monitor):
            name = type(monitor).__name__
            raise TypeError(f"monitor must be callable or None, got {name}")
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
        """Call the monitor, routing its errors to the loop (D023 v2).

        This mirrors how asyncio runs a callback (``Handle._run``): an
        ``Exception`` from the monitor is handed to
        ``loop.call_exception_handler`` (which logs it by default) instead of
        propagating, so the ``put``/``get`` that triggered it still completes
        and the item is never lost. ``KeyboardInterrupt``/``SystemExit`` are
        ``BaseException`` and propagate, as in asyncio. ``CancelledError`` is
        also a ``BaseException``; this helper runs only after the queue
        operation has finished, so there is no pending cancellation to mask.
        """
        if self._monitor is None:
            return
        try:
            self._monitor(self)
        except Exception as exc:
            asyncio.get_running_loop().call_exception_handler(
                {"message": "Store monitor raised", "exception": exc, "store": self}
            )

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
