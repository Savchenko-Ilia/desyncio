"""Scaled real-time mode: pace the virtual clock against the wall clock.

The idea
--------
In fast mode ``_JumpingSelector.select`` never waits: when the loop is idle it
jumps the virtual clock to the next timer. In real-time mode it *waits first*,
for ``real_dt = virtual_dt / speed`` seconds of wall time, and only then
advances the clock. Same mechanism, one extra step (AGENTS.md principle 3).

How the waiting works
---------------------
A stock loop waits in exactly one place: ``selector.select(timeout)``, which
blocks in ``epoll_wait`` until the timeout passes **or a file descriptor
becomes readable**. The loop owns one such descriptor, the self-pipe;
``loop.call_soon_threadsafe`` writes a byte into it. So if our wait is a real
``select(real_timeout)`` on the real selector, any thread can cut the wait
short by writing that byte. That is how speed changes, pause and resume take
effect immediately instead of after the current (possibly hours-long) wait.

When a wait is cut short after ``elapsed`` real seconds, the virtual clock
advances by ``elapsed * speed`` (never past the timer), so virtual time stays
consistent with the real time that actually passed. The loop then runs the
woken callback, and the next idle ``select`` re-reads speed and pause state.

The wall clock
--------------
:class:`WallClock` is the minimal seam tests need: ``monotonic()`` and
``wait(timeout, selector)``. ``wait`` blocks for up to ``timeout`` wall
seconds (``None`` means until woken) **or until the selector reports events**,
and returns those events. :class:`SystemWallClock` does exactly
``selector.select(timeout)``. A test fake just adds ``timeout`` to a fake time
and returns, so no test sleeps.

Thread safety
-------------
:class:`RealTimePacer` state is guarded by a lock, and every change is
followed by ``loop.call_soon_threadsafe(noop)`` to wake the loop. Calls are
supported from (a) code running inside the simulation, e.g. a coroutine or
timer callback, and (b) any other thread, e.g. a UI. In case (a) the wake-up is
simply an extra, harmless callback. Note that a *pause issued from inside the
simulation* freezes virtual time, so only another thread (or a wall-clock
hook) can resume it.

Determinism
-----------
The virtual trace does not depend on speed or on pacing: the same events run
in the same order at the same virtual times as in fast mode. Only *when* (in
wall time) they happen differs. Changes made from another thread are
inherently timing-dependent; that is the point of a UI, and they affect only
pacing, never the order of events. Callback run time is not subtracted from
waits, so real time can lag the ideal schedule slightly (drift).

Deadlock rule
-------------
With no timers and nothing ready, a running (not paused) simulation raises
``SimulationDeadlock`` exactly as in fast mode (D006): real-time mode does not
wait for threads. While *paused* the loop waits for resume, since pausing is
an explicit request to stop.
"""

import asyncio
import math
import selectors
import threading
import time
from typing import Protocol

Events = list[tuple[selectors.SelectorKey, int]]

#: Longest single real wait, in seconds (one day). ``select`` raises
#: ``OverflowError`` for timeouts above ~9.2e9 s, which a slow speed easily
#: reaches (``1 / 1e-12``). asyncio caps its own waits the same way
#: (``MAXIMUM_SELECT_TIMEOUT`` in ``base_events.py``). A longer wait is done
#: in chunks; see ``_JumpingSelector._select_paced``.
MAX_REAL_WAIT = 86400.0


class WallClock(Protocol):
    """What the pacer needs from a wall clock (so tests can fake it)."""

    def monotonic(self) -> float:
        """Current wall time in seconds; only differences matter."""
        ...

    def wait(self, timeout: float | None, selector: selectors.BaseSelector) -> Events:
        """Block up to ``timeout`` seconds or until ``selector`` has events.

        ``timeout=None`` means no limit (used while paused). Returns the
        selector's events (empty if the full time passed).
        """
        ...


class SystemWallClock:
    """The real wall clock: ``time.monotonic`` and a real ``select``."""

    def monotonic(self) -> float:
        return time.monotonic()

    def wait(self, timeout: float | None, selector: selectors.BaseSelector) -> Events:
        # The real wait. The self-pipe is registered on this selector, so
        # call_soon_threadsafe from another thread ends the wait early.
        return selector.select(timeout)


def _check_speed(value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"speed must be a real number, got {type(value).__name__}")
    speed = float(value)
    if math.isnan(speed) or speed <= 0.0:
        raise ValueError(f"speed must be > 0 (or math.inf), got {value!r}")
    return speed


def _noop() -> None:
    """Callback whose only purpose is to make the loop wake up and re-check."""


class RealTimePacer:
    """Controls how fast virtual time runs relative to the wall clock.

    ``speed=1`` is real time, ``2`` twice as fast, ``0.5`` half speed and
    ``math.inf`` is fast mode (no waiting). Pass it to ``desyncio.run(...,
    pacer=...)`` or ``VirtualTimeLoop(pacer=...)``; one pacer serves one loop
    at a time.
    """

    def __init__(self, speed: float = 1.0, wall_clock: WallClock | None = None) -> None:
        self._speed = _check_speed(speed)
        self._paused = False
        self._lock = threading.Lock()
        self._loop: asyncio.AbstractEventLoop | None = None
        self.wall_clock: WallClock = (
            wall_clock if wall_clock is not None else SystemWallClock()
        )

    @property
    def speed(self) -> float:
        """Virtual seconds per real second. Settable from any thread."""
        with self._lock:
            return self._speed

    @speed.setter
    def speed(self, value: float) -> None:
        checked = _check_speed(value)
        with self._lock:
            self._speed = checked
        self._wake()

    @property
    def paused(self) -> bool:
        """Whether virtual time is frozen."""
        with self._lock:
            return self._paused

    def pause(self) -> None:
        """Freeze virtual time. The loop keeps waiting (interruptibly)."""
        with self._lock:
            self._paused = True
        self._wake()

    def resume(self) -> None:
        """Let virtual time flow again."""
        with self._lock:
            self._paused = False
        self._wake()

    def snapshot(self) -> tuple[float, bool]:
        """Return ``(speed, paused)`` read together (used by the loop)."""
        with self._lock:
            return self._speed, self._paused

    def bind(self, loop: asyncio.AbstractEventLoop) -> None:
        """Attach to ``loop`` (called by ``VirtualTimeLoop``)."""
        with self._lock:
            if self._loop is not None and not self._loop.is_closed():
                raise ValueError("this pacer is already bound to a running loop")
            self._loop = loop

    def _wake(self) -> None:
        """Write to the loop's self-pipe so a blocked ``select`` returns."""
        loop = self._loop
        if loop is None:
            return
        try:
            loop.call_soon_threadsafe(_noop)
        except RuntimeError:
            pass  # loop closed: nothing is waiting any more
