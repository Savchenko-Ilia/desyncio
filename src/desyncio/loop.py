"""Virtual-time event loop.

How an asyncio loop spends its time
-----------------------------------
Every iteration of ``BaseEventLoop._run_once`` does roughly this:

1. Drop cancelled timers from the front of the ``_scheduled`` heap.
2. Pick a *timeout* for the selector: ``0`` if callbacks are ready
   (``_ready``), else "time until the earliest timer" (``_scheduled[0]._when
   - self.time()``), else ``None`` (block until I/O).
3. Call ``self._selector.select(timeout)``. This is the **only** place a stock
   loop really waits (``epoll_wait`` and friends).
4. Move every timer whose ``_when < self.time() + clock_resolution`` from
   ``_scheduled`` to ``_ready``.
5. Run everything in ``_ready``.

A simulation must never wait in step 3. :class:`VirtualTimeLoop` therefore
(a) answers ``time()`` from a :class:`~desyncio.clock.VirtualClock`, and
(b) swaps the selector for :class:`_JumpingSelector`, whose ``select`` moves
the clock to the earliest timer instead of sleeping. Step 4 then sees that
timer as due and the loop carries on, having skipped the wait entirely.

Same-time ordering and the resolution window
--------------------------------------------
Timers are ordered by ``(when, priority, sequence)``; see
:mod:`desyncio.scheduling`. Step 4 above has a subtlety: it moves every timer
with ``_when < time() + _clock_resolution`` into ``_ready`` *in one go*, and
``time()`` stays frozen while they run. With the real-clock resolution
(~1e-9) a timer at ``1.0 + 5e-10`` would run while ``loop.time() == 1.0``. We
shrink the window to the width of one float (see
``VirtualTimeLoop._clock_resolution``) so that only timers at exactly the same
time share an instant.

Private-API dependency
----------------------
``VirtualTimeLoop._schedule`` is adapted from CPython's
``BaseEventLoop.call_at`` (``Lib/asyncio/base_events.py``, Python 3.13) and
is under the PSF License Agreement; see ``LICENSES/PSF.txt``. The rest of
this module only subclasses and overrides. The selector trick and
``_schedule`` read private attributes of ``BaseEventLoop``: ``_scheduled`` (a
heap of ``TimerHandle``), ``TimerHandle._when``, ``_selector`` and
``_clock_resolution``. They are stable across 3.13 but not public; if a
future Python changes them, the tests in ``tests/test_loop.py`` will fail
loudly.
"""

import asyncio
import heapq
import math
import selectors
from collections.abc import Callable, Mapping
from contextvars import Context
from typing import Any

from desyncio.clock import VirtualClock
from desyncio.guards import RealIOGuardMixin
from desyncio.scheduling import (
    DEFAULT_PRIORITY,
    PriorityTimerHandle,
    SequenceCounter,
)


class SimulationDeadlock(RuntimeError):
    """Nothing is ready, no timer is scheduled, and no I/O event is pending.

    Every task is waiting on something that can never happen, so the
    simulation can make no further progress.
    """


class _JumpingSelector(selectors.BaseSelector):
    """Wraps a real selector; ``select`` advances virtual time, never sleeps.

    The real selector is still used for registration and is still polled
    (with timeout 0) on every call. That matters because the loop's
    self-pipe, which ``call_soon_threadsafe`` writes to, is a real file
    descriptor. Polling it means a ``call_soon_threadsafe`` made from the
    loop's own thread (the byte is already in the pipe) is seen on the next
    iteration.

    This does **not** support wake-ups from other threads. The poll never
    waits, so if the loop is idle it jumps the clock or raises
    :class:`SimulationDeadlock` before a real thread can write its byte
    (D006). Real threads are unsupported; ``run_in_executor`` and
    ``to_thread`` are refused by :mod:`desyncio.guards`.
    """

    def __init__(self, real: selectors.BaseSelector, loop: "VirtualTimeLoop") -> None:
        self._real = real
        self._loop = loop

    def register(
        self, fileobj: Any, events: int, data: Any = None
    ) -> selectors.SelectorKey:
        return self._real.register(fileobj, events, data)

    def unregister(self, fileobj: Any) -> selectors.SelectorKey:
        return self._real.unregister(fileobj)

    def modify(
        self, fileobj: Any, events: int, data: Any = None
    ) -> selectors.SelectorKey:
        return self._real.modify(fileobj, events, data)

    def get_key(self, fileobj: Any) -> selectors.SelectorKey:
        return self._real.get_key(fileobj)

    def get_map(self) -> Mapping[Any, selectors.SelectorKey]:
        return self._real.get_map()

    def close(self) -> None:
        self._real.close()

    def select(
        self, timeout: float | None = None
    ) -> list[tuple[selectors.SelectorKey, int]]:
        """Poll real fds without blocking; if idle, jump to the next timer.

        ``_run_once`` passes ``timeout == 0`` when callbacks are ready (or the
        loop is stopping): then we only poll. Otherwise it is positive or
        ``None`` and it would block; we jump the clock instead.
        """
        events = self._real.select(0)
        if events or timeout == 0:
            return events

        # _run_once already popped cancelled timers off the head of the heap
        # before computing the timeout, so [0] is a live timer. Cancelled
        # timers therefore never move the clock.
        scheduled = self._loop._scheduled  # type: ignore[attr-defined]  # noqa: SLF001
        if scheduled:
            when: float = scheduled[0]._when
            # No "never go backwards" guard is needed. If a timer were
            # overdue (``when <= now``), ``_run_once`` would have computed a
            # timeout of 0 and we would have returned above. Reaching this
            # line therefore means ``when > now``. ``advance_to`` still
            # raises if that were ever false, which is the loud failure we
            # want rather than silently hiding a bug.
            self._loop.clock.advance_to(when)
            return events

        raise SimulationDeadlock(
            "no ready callbacks, no scheduled timers and no pending I/O: "
            "every task is waiting on something that will never happen"
        )


class VirtualTimeLoop(RealIOGuardMixin, asyncio.SelectorEventLoop):
    """A ``SelectorEventLoop`` whose ``time()`` is a virtual clock.

    Use it with ``asyncio.Runner(loop_factory=VirtualTimeLoop)``. Timers
    (``sleep``, ``call_later``, ``call_at``, ``timeout``) are measured on the
    virtual clock, and whenever the loop would otherwise wait it jumps the
    clock straight to the earliest pending timer.

    Real I/O (sockets, subprocesses, executors, signals) is refused with
    :class:`~desyncio.guards.RealIOError`; see :mod:`desyncio.guards` for
    which entry points and why. :class:`~desyncio.guards.RealIOGuardMixin`
    comes first in the bases so its overrides win.
    """

    def __init__(self, start: float = 0.0, clock: VirtualClock | None = None) -> None:
        """Create the loop.

        Args:
            start: initial simulation time; ignored if ``clock`` is given.
            clock: an existing clock to drive instead of a fresh one.
        """
        # Set before super().__init__: the base constructor may call time().
        self.clock = clock if clock is not None else VirtualClock(start)
        super().__init__()
        # The base constructor created the real selector and registered the
        # self-pipe on it. Wrap it so the registrations carry over.
        self._selector = _JumpingSelector(self._selector, self)  # type: ignore[has-type]
        self._sequence = SequenceCounter()

    @property
    def _clock_resolution(self) -> float:
        """Width of the "due" window used by ``_run_once``: one float step.

        ``_run_once`` treats a timer as due if ``_when < time() + resolution``.
        Everything due is run in the same iteration while ``time()`` is frozen,
        so a window of ~1e-9 would let a timer at ``1.0 + 5e-10`` observe
        ``loop.time() == 1.0``. We want each timer to see its own time.

        Zero is not an option: the test is ``_when >= end_time`` -> stop, so
        with resolution 0 a timer at exactly ``now`` would never be due and
        the loop would spin. Instead use the gap from ``now`` to the next
        representable float, so ``now + resolution`` is the very next float
        and only timers at exactly ``now`` (or earlier) are due. The window
        therefore depends on ``now`` and is recomputed on every read.
        """
        now = self.clock.now
        return math.nextafter(now, math.inf) - now

    @_clock_resolution.setter
    def _clock_resolution(self, value: float) -> None:
        # BaseEventLoop.__init__ assigns the real clock's resolution. Ignore
        # it: the virtual clock defines its own (see the getter).
        del value

    def call_at[*Ts](
        self,
        when: float,
        callback: Callable[[*Ts], object],
        *args: *Ts,
        context: Context | None = None,
    ) -> asyncio.TimerHandle:
        """Schedule ``callback`` at absolute time ``when`` with default priority.

        ``call_later``, ``asyncio.sleep`` and ``asyncio.timeout`` all end up
        here (``call_later`` calls ``self.call_at``; ``sleep`` calls
        ``call_later``; ``timeout`` calls ``call_at``), so every stock timer
        gets priority 0 and the next sequence number.
        """
        timer = self._schedule(
            when, DEFAULT_PRIORITY, callback, args, context, "call_at"
        )
        # Drop this frame from the debug-mode creation traceback, as stock
        # asyncio does, so it ends at the caller.
        timer.drop_creation_frame()
        return timer

    def call_at_priority[*Ts](
        self,
        when: float,
        priority: int,
        callback: Callable[[*Ts], object],
        *args: *Ts,
        context: Context | None = None,
    ) -> asyncio.TimerHandle:
        """Like ``call_at`` but with an explicit tie-break ``priority``.

        Timers fire in ``(when, priority, sequence)`` order: first by time,
        then lower ``priority`` first, then in the order they were scheduled.
        ``priority`` must be an ``int`` (``bool`` is rejected); plain
        ``call_at`` uses 0.
        """
        if not isinstance(priority, int) or isinstance(priority, bool):
            raise TypeError(
                f"priority must be an int, got {type(priority).__name__}"
            )
        timer = self._schedule(
            when, priority, callback, args, context, "call_at_priority"
        )
        timer.drop_creation_frame()
        return timer

    # Adapted from CPython Lib/asyncio/base_events.py, BaseEventLoop.call_at
    # (Python 3.13). Copyright (c) 2001 Python Software Foundation. Licensed
    # under the PSF License Agreement v2; see LICENSES/PSF.txt.
    # Changes from the original: builds a PriorityTimerHandle (keyed by
    # when, priority, sequence) instead of events.TimerHandle; takes a
    # priority and assigns the next per-loop sequence number; takes the
    # caller's api_name for _check_callback instead of the literal 'call_at';
    # takes args as a tuple; uses drop_creation_frame()/mark_scheduled()
    # helpers for the traceback trim and the _scheduled flag.
    def _schedule(
        self,
        when: float,
        priority: int,
        callback: Callable[..., object],
        args: tuple[Any, ...],
        context: Context | None,
        api_name: str,
    ) -> PriorityTimerHandle:
        """Create a keyed timer and push it onto the loop's timer heap.

        ``heappush`` sifts the new timer up using ``PriorityTimerHandle.__lt__``,
        i.e. the ``(when, priority, sequence)`` key. The sequence number is
        taken here, at scheduling time, so it reflects call order.
        """
        if when is None:
            raise TypeError("when cannot be None")
        self._check_closed()  # type: ignore[attr-defined]
        if self._debug:  # type: ignore[attr-defined]
            self._check_thread()  # type: ignore[attr-defined]
            self._check_callback(callback, api_name)  # type: ignore[attr-defined]
        timer = PriorityTimerHandle(
            when, priority, self._sequence.next(), callback, args, self, context
        )
        timer.drop_creation_frame()
        heapq.heappush(self._scheduled, timer)  # type: ignore[attr-defined]
        timer.mark_scheduled()
        return timer

    def time(self) -> float:
        """Return the current simulation time instead of ``time.monotonic()``."""
        return self.clock.now
