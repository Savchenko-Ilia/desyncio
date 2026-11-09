"""Container: a level of homogeneous stuff (fuel, water, money) with blocking put/get.

asyncio has no primitive for "wait until an amount is available", so ``Container``
is built on the closest stock piece, :class:`asyncio.Condition`.

What asyncio does for us
------------------------
* ``Condition.wait_for(predicate)``: parks the task on a future, re-checks the
  predicate after every wakeup, and re-acquires the lock before returning.
* ``Condition.notify_all()``: wakes every parked task so each re-checks its
  predicate.
* The ``Lock`` inside the ``Condition``: FIFO and mutually exclusive, so the
  level is never modified by two tasks at once, and tasks are admitted in call
  order.
* Cancellation: ``Condition.wait`` re-acquires the lock even when cancelled, so
  our ``finally`` block always runs holding the lock.
* Timers and virtual time: nothing here knows about time at all. Blocking is
  plain futures, which the desyncio loop drives in virtual time.

What is ours (the minimum on top)
---------------------------------
``Condition`` wakes waiters in FIFO order, but ``wait_for`` makes each of them
re-check its *own* predicate, so after a ``notify_all`` a small ``get(1)`` queued
behind a big ``get(10)`` would see ``level >= 1`` and overtake it. The spec
(D021) wants strict FIFO with head-of-line blocking. Condition cannot express
that by itself, so each direction keeps a deque of tickets (plain ``object()``
instances) and the predicate also requires "my ticket is at the front". That is
the only extra state: no hand-rolled futures, no wakeup logic.
When a waiter leaves (served or cancelled) its ticket is removed and
``notify_all`` lets the new head re-check, so a cancelled head cannot block the
line.
"""

import asyncio
import math
import numbers
from collections import deque
from collections.abc import Callable

__all__ = ["Container"]


def _check_real(name: str, value: object) -> float:
    # bool is an int subclass but "True units of fuel" is always a bug.
    if isinstance(value, bool) or not isinstance(value, numbers.Real):
        raise TypeError(f"{name} must be a real number, not {value!r}")
    return float(value)


class Container:
    """A bounded or unbounded level that processes can ``put`` into and ``get`` from.

    ``await c.put(n)`` blocks while ``level + n > capacity``; ``await c.get(n)``
    blocks while ``level < n``. Puts are served strictly FIFO among puts, and
    gets strictly FIFO among gets (head-of-line blocking): a large waiting
    ``get`` holds back smaller later ones until it is served. Puts and gets do
    not block each other's queues.

    ``monitor``, if given, is called as ``monitor(level)`` after every level
    change (not on blocked or cancelled requests).
    """

    def __init__(
        self,
        capacity: float = math.inf,
        init: float = 0,
        *,
        monitor: Callable[[float], object] | None = None,
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
        # One Condition (and so one Lock) guards the level and both queues.
        self._cond = asyncio.Condition()
        self._putters: deque[object] = deque()
        self._getters: deque[object] = deque()

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
        self, queue: deque[object], delta: float, fits: Callable[[], bool]
    ) -> None:
        ticket = object()
        # ``async with cond`` takes the Condition's Lock; ``wait_for`` releases it
        # while parked and re-takes it before each predicate check.
        async with self._cond:
            queue.append(ticket)
            try:
                await self._cond.wait_for(lambda: queue[0] is ticket and fits())
                self._level += delta
                if self._monitor is not None:
                    self._monitor(self._level)
            finally:
                # Runs on success and on cancellation (the lock is held either
                # way). Dropping the ticket promotes the next waiter; notify_all
                # makes it, and anyone the level change now satisfies, re-check.
                queue.remove(ticket)
                self._cond.notify_all()
