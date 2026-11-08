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

Private-API dependency
----------------------
No CPython code is copied (so no PSF notice is needed), but the selector
trick reads three private attributes of ``BaseEventLoop``: ``_scheduled`` (a
heap of ``TimerHandle``), ``TimerHandle._when`` and ``_selector``. They are
stable across 3.13 but not public; if a future Python changes them, the tests
in ``tests/test_loop.py`` will fail loudly.
"""

import asyncio
import selectors
from collections.abc import Mapping
from typing import Any

from desyncio.clock import VirtualClock


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
    descriptor.
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
            # Never go backwards: a timer already overdue leaves time as is.
            self._loop.clock.advance_to(max(when, self._loop.clock.now))
            return events

        raise SimulationDeadlock(
            "no ready callbacks, no scheduled timers and no pending I/O: "
            "every task is waiting on something that will never happen"
        )


class VirtualTimeLoop(asyncio.SelectorEventLoop):
    """A ``SelectorEventLoop`` whose ``time()`` is a virtual clock.

    Use it with ``asyncio.Runner(loop_factory=VirtualTimeLoop)``. Timers
    (``sleep``, ``call_later``, ``call_at``, ``timeout``) are measured on the
    virtual clock, and whenever the loop would otherwise wait it jumps the
    clock straight to the earliest pending timer.
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

    def time(self) -> float:
        """Return the current simulation time instead of ``time.monotonic()``."""
        return self.clock.now
