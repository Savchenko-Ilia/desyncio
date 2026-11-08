"""Deterministic ordering for timers: ``(time, priority, sequence)``.

Why stock asyncio is not deterministic enough
---------------------------------------------
``BaseEventLoop`` keeps its timers in ``loop._scheduled``, a plain Python list
managed as a *binary min-heap* by the ``heapq`` module. ``heappush`` and
``heappop`` only promise that ``heap[0]`` is a smallest element, according to
``<``. For ``asyncio.TimerHandle``, ``<`` compares ``_when`` and nothing else.

So when two timers have the same ``_when``, neither is "smaller", and the
heap is free to pop them in whatever order its internal swaps happened to
leave them. ``heapq`` is not *stable* (it does not keep insertion order for
equal elements). With a handful of timers you may not notice; with a thousand
tasks all sleeping until t=5 the order comes out shuffled. A simulation whose
same-time events run in an arbitrary order cannot give the same trace twice.

The fix: make ties impossible
-----------------------------
Give every timer a sort key that is *unique*: ``(when, priority, sequence)``.

* ``when``: the simulation time. Earlier fires first.
* ``priority``: an int, default 0. At the same time, lower fires first.
* ``sequence``: a per-loop counter, stamped when the timer is scheduled. Two
  timers can never share a sequence number, so no two keys are ever equal and
  the heap's arbitrary choices no longer matter: there is exactly one valid
  pop order, and it is the sorted order. Equal ``(when, priority)`` therefore
  means FIFO in scheduling order.

:class:`PriorityTimerHandle` is a ``TimerHandle`` that compares by that key.
The heap code in CPython is untouched; it simply calls our ``__lt__``.
"""

import itertools
from asyncio import AbstractEventLoop, TimerHandle
from collections.abc import Callable
from contextvars import Context
from typing import Any

DEFAULT_PRIORITY = 0
"""Priority of every timer made through stock asyncio APIs (``sleep``,
``call_later``, ``call_at``, ``asyncio.timeout``)."""


class SequenceCounter:
    """A per-loop, strictly increasing counter used as the final tie-break.

    Each loop owns its own instance, so a fresh loop always starts again at 0
    and two runs of the same model stamp identical sequence numbers.
    """

    def __init__(self) -> None:
        self._counter = itertools.count()

    def next(self) -> int:
        """Return the next number: 0, 1, 2, ... never repeated."""
        return next(self._counter)


class PriorityTimerHandle(TimerHandle):
    """A ``TimerHandle`` ordered by ``(when, priority, sequence)``.

    ``heapq`` only ever uses ``<`` (``__lt__``) to order items, so that is the
    method that matters. The other rich comparisons are overridden to agree
    with it so the class is not self-contradictory. ``__eq__`` and
    ``__hash__`` are inherited unchanged from ``TimerHandle``; the loop never
    relies on them for ordering.
    """

    __slots__ = ("_priority", "_sequence")

    def __init__(
        self,
        when: float,
        priority: int,
        sequence: int,
        callback: Callable[..., object],
        args: tuple[Any, ...],
        loop: AbstractEventLoop,
        context: Context | None = None,
    ) -> None:
        super().__init__(when, callback, args, loop, context)
        # In debug mode Handle.__init__ records the creation stack. TimerHandle
        # drops its own frame; drop ours too so the traceback ends at the
        # user's call site like stock asyncio's.
        self.drop_creation_frame()
        self._priority = priority
        self._sequence = sequence

    @property
    def priority(self) -> int:
        """Tie-break rank at equal time; lower fires first."""
        return self._priority

    @property
    def sequence(self) -> int:
        """Per-loop scheduling number; lower was scheduled earlier."""
        return self._sequence

    def sort_key(self) -> tuple[float, int, int]:
        """The ``(when, priority, sequence)`` triple that orders this timer."""
        # ``_when`` is a private TimerHandle attribute missing from typeshed.
        when: float = self._when  # type: ignore[attr-defined]
        return (when, self._priority, self._sequence)

    # Adapted from CPython (Python 3.13) Lib/asyncio/base_events.py,
    # BaseEventLoop.call_at, and Lib/asyncio/events.py, TimerHandle.__init__
    # (the ``if ..._source_traceback: del ..._source_traceback[-1]`` trim).
    # Copyright (c) 2001 Python Software Foundation. Licensed under the PSF
    # License Agreement v2; see LICENSES/PSF.txt. Changed: moved into a method.
    def drop_creation_frame(self) -> None:
        """Remove the innermost frame of the debug-mode creation traceback.

        Each helper layer between the user and ``Handle.__init__`` calls this
        once so the recorded stack ends at the user's call. No-op unless the
        loop is in debug mode.
        """
        tb = self._source_traceback  # type: ignore[attr-defined]
        if tb:
            del tb[-1]

    def mark_scheduled(self) -> None:
        """Tell the handle it now sits in the loop's heap.

        ``cancel()`` consults this flag so the loop can count cancelled timers
        still in the heap and decide when to rebuild it.
        """
        self._scheduled = True

    def __lt__(self, other: object) -> bool:
        if isinstance(other, PriorityTimerHandle):
            return self.sort_key() < other.sort_key()
        return NotImplemented

    def __le__(self, other: object) -> bool:
        if isinstance(other, PriorityTimerHandle):
            return self.sort_key() <= other.sort_key()
        return NotImplemented

    def __gt__(self, other: object) -> bool:
        if isinstance(other, PriorityTimerHandle):
            return self.sort_key() > other.sort_key()
        return NotImplemented

    def __ge__(self, other: object) -> bool:
        if isinstance(other, PriorityTimerHandle):
            return self.sort_key() >= other.sort_key()
        return NotImplemented
