"""The ``run`` entry point: run one coroutine on a fresh virtual-time loop.

This module is named ``runners`` (like ``asyncio.runners``, which holds
``asyncio.run``) so that no submodule called ``run`` shadows the function
``desyncio.run``.

``desyncio.run(main(), until=None, seed=0, speed=None, pacer=None, trace=None)`` is the
simulation counterpart of ``asyncio.run``. Three things are worth
understanding here (real-time pacing is covered in :mod:`desyncio.realtime`).

Task lifetimes and shutdown
---------------------------
``run`` wraps ``main`` in a task and drives the loop until that task is done
(or until virtual time ``until`` is reached). Any *other* task that ``main``
spawned with ``asyncio.create_task`` is a separate task with its own lifetime:
finishing ``main`` does **not** wait for it. Those tasks are simply left
unfinished, exactly like ``asyncio.run``.

When the ``asyncio.Runner`` closes, it does what ``asyncio.run`` does:

1. ``cancel()`` every task that is still pending, then run the loop until all
   of them have processed the ``CancelledError`` (their ``except`` / ``finally``
   blocks run now);
2. shut down async generators and the default executor;
3. close the loop.

Step 1 is the danger for a simulation. A task's cancellation handler may
``await asyncio.sleep(5)``, which would normally push the clock forward and
make "nothing runs after ``until``" false. We therefore **freeze the clock**
before closing (see :class:`_BoundedClock`): cleanup code may run, but only
code that needs no virtual time to pass (``await asyncio.sleep(0)``, releasing
locks, closing resources...). A handler that waits for a timer later than the
frozen clock makes ``run`` raise :class:`SimulationCleanupError`; the loop is
still closed.

How ``until`` stops the loop
----------------------------
The loop only advances the clock when it is idle (nothing ready to run): it
jumps to the earliest timer. :class:`_BoundedClock` is a ``VirtualClock`` that
refuses to jump past a limit. When the loop asks for a later time, the clock
moves to exactly the limit and raises a private exception that unwinds the
loop. Because jumps happen only when idle, every callback and timer due at
``t <= until`` has already run by then. No ``loop.py`` change is needed.

Seeds
-----
``run`` builds an :class:`~desyncio.rng.RngStreams` and stores it in a
``contextvars.ContextVar``. The variable is set inside a fresh
``contextvars.Context`` passed to ``Runner.run(main, context=...)`` (the
``context=`` keyword of ``Runner(...)`` itself only exists from Python 3.14). Every task
*copies the context that is current when it is created*, so the main task, and
all tasks it spawns (directly or indirectly), see the same ``RngStreams``
object. Setting the variable inside a private ``Context`` (rather than in the
caller's) also means it never leaks into, or out of, the caller's code.
Read it with :func:`current_rng_streams`.
"""

import asyncio
import contextvars
import math
from collections.abc import Coroutine
from typing import Any, overload

from desyncio.clock import VirtualClock
from desyncio.loop import VirtualTimeLoop
from desyncio.realtime import RealTimePacer
from desyncio.rng import RngStreams
from desyncio.trace import TraceSink

_rng_streams: contextvars.ContextVar[RngStreams] = contextvars.ContextVar(
    "desyncio_rng_streams"
)


class SimulationCleanupError(RuntimeError):
    """A task's cancellation handler needed virtual time to pass during close."""


class _LimitReached(Exception):
    """Internal: the loop tried to move the clock past its limit."""


class _BoundedClock(VirtualClock):
    """A clock that never moves past ``limit``; see the module docstring."""

    def __init__(self, limit: float) -> None:
        super().__init__(0.0)
        self.limit = limit

    def advance_to(self, t: float) -> None:
        if t > self.limit:
            super().advance_to(self.limit)  # clock reads exactly the limit
            raise _LimitReached
        super().advance_to(t)


def current_rng_streams() -> RngStreams:
    """Return the :class:`RngStreams` of the simulation being run.

    Works in ``main`` and in any task (transitively) created by it.

    Raises:
        RuntimeError: when called outside :func:`run`.
    """
    try:
        return _rng_streams.get()
    except LookupError:
        raise RuntimeError(
            "no simulation is running: current_rng_streams() only works "
            "inside desyncio.run()"
        ) from None


def _check_until(until: object) -> float:
    """Validate ``until`` (a finite real number >= the start time 0.0)."""
    if isinstance(until, bool) or not isinstance(until, (int, float)):
        raise TypeError(
            f"until must be a real number or None, got {type(until).__name__}"
        )
    try:
        value = float(until)
    except OverflowError:
        raise ValueError(
            "until must be finite, got an int too large for a float"
        ) from None
    if not math.isfinite(value):
        raise ValueError(f"until must be finite, got {value!r}")
    if value < 0.0:
        raise ValueError(f"until must be >= the start time 0.0, got {value!r}")
    return value


@overload
def run[T](
    main: Coroutine[Any, Any, T],
    *,
    until: None = None,
    seed: int = 0,
    speed: float | None = None,
    pacer: RealTimePacer | None = None,
    trace: TraceSink | None = None,
) -> T: ...
@overload
def run[T](
    main: Coroutine[Any, Any, T],
    *,
    until: float,
    seed: int = 0,
    speed: float | None = None,
    pacer: RealTimePacer | None = None,
    trace: TraceSink | None = None,
) -> T | None: ...
def run[T](
    main: Coroutine[Any, Any, T],
    *,
    until: float | None = None,
    seed: int = 0,
    speed: float | None = None,
    pacer: RealTimePacer | None = None,
    trace: TraceSink | None = None,
) -> T | None:
    """Run ``main`` on a fresh virtual-time loop and return its result.

    Args:
        main: a coroutine object (``main()``, not ``main``).
        until: stop at this virtual time. Timers due at ``t <= until`` run;
            nothing runs after. ``None`` means run until ``main`` finishes.
        seed: master seed for :func:`current_rng_streams`.
        speed: run in scaled real time at this speed (virtual seconds per
            real second); shorthand for ``pacer=RealTimePacer(speed)``.
            ``None`` (default) is fast mode; ``math.inf`` is the same.
        pacer: a :class:`~desyncio.realtime.RealTimePacer`, for changing
            speed or pausing mid-run, or for an injected wall clock. Give
            ``speed`` or ``pacer``, not both.
        trace: a callable taking one :class:`~desyncio.trace.TraceRecord`, called
            for every task start, resume, finish and cancel (see
            :mod:`desyncio.trace`). Off by default (``None``); when off no
            tracing code runs. If it raises, the error propagates.

    Returns:
        ``main``'s result, or ``None`` if ``until`` was reached first (the
        clock then reads exactly ``until`` and the other tasks, including
        ``main``, are cancelled during close; see the module docstring).

    Raises:
        TypeError: ``main`` is not a coroutine, ``until`` / ``seed`` /
            ``speed`` / ``pacer`` has the wrong type, or both ``speed`` and
            ``pacer`` are given.
        ValueError: ``until`` is NaN, infinite or negative; ``seed`` is
            negative; ``speed`` is not > 0.
        RuntimeError: called from inside a running event loop.
        SimulationDeadlock: every task is blocked and no timer is pending.
        SimulationCleanupError: a cancellation handler needed time to pass.
        Exception: whatever ``main`` raises. The loop is closed first.
    """
    if not asyncio.iscoroutine(main):
        raise TypeError(
            f"run() needs a coroutine object, got {type(main).__name__} "
            "(did you forget to call main()?)"
        )
    try:
        # Validate everything before creating a loop; close the coroutine on
        # failure so Python does not warn "coroutine was never awaited".
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            pass
        else:
            raise RuntimeError("run() cannot be called from a running event loop")
        limit = math.inf if until is None else _check_until(until)
        streams = RngStreams(seed)  # validates seed
        if speed is not None and pacer is not None:
            raise TypeError("give either speed= or pacer=, not both")
        if speed is not None:
            pacer = RealTimePacer(speed)  # validates speed
        elif pacer is not None and not isinstance(pacer, RealTimePacer):
            raise TypeError(
                f"pacer must be a RealTimePacer, got {type(pacer).__name__}"
            )
        clock = _BoundedClock(limit)
        # Build the loop here, not lazily inside Runner.run: binding the pacer
        # can fail (it is bound to another live loop), and Runner.run would
        # then raise without closing ``main``.
        loop = VirtualTimeLoop(clock=clock, pacer=pacer, trace=trace)
        loop.name_next_task("main")
    except BaseException:
        main.close()
        raise

    # A fresh Context: the ContextVar set below is visible to main and to all
    # tasks it creates (each task copies the current context), and nowhere else.
    context = contextvars.Context()
    context.run(_rng_streams.set, streams)

    runner = asyncio.Runner(loop_factory=lambda: loop)
    try:
        try:
            return runner.run(main, context=context)
        except _LimitReached:
            return None  # `until` reached; clock is exactly `until`
    finally:
        # Freeze time where it is (== until if we stopped early, or when main
        # finished otherwise) before runner.close() cancels leftover tasks.
        clock.limit = clock.now
        loop.stop_tracing_new_tasks()
        try:
            runner.close()  # always closes the loop, even if this raises
        except _LimitReached:
            raise SimulationCleanupError(
                "a task's cancellation handler waited for virtual time to pass "
                f"beyond {clock.now!r} (the end of the simulation, `until` or "
                "when main finished); cleanup code may not advance the clock"
            ) from None
        finally:
            # Runner.close() is a no-op if the runner never used the loop.
            if not loop.is_closed():
                loop.close()
