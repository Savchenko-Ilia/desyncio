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

A simulation must not wait for the real world in step 3. :class:`VirtualTimeLoop`
therefore (a) answers ``time()`` from a :class:`~desyncio.clock.VirtualClock`, and
(b) swaps the selector for :class:`_JumpingSelector`, whose ``select`` moves
the clock to the earliest timer. In fast mode it does so at once, skipping the
wait entirely. In real-time mode (a :class:`~desyncio.realtime.RealTimePacer`)
it first waits ``virtual_dt / speed`` of wall time, interruptibly. Step 4 then
sees the timer as due and the loop carries on.

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
import itertools
import math
import selectors
from collections.abc import Callable, Coroutine, Mapping
from contextvars import Context
from typing import Any

from desyncio.clock import VirtualClock
from desyncio.guards import RealIOGuardMixin
from desyncio.realtime import MAX_REAL_WAIT, RealTimePacer
from desyncio.scheduling import (
    DEFAULT_PRIORITY,
    PriorityTimerHandle,
    SequenceCounter,
)
from desyncio.trace import Tracer, TraceSink, _TracedCoroutine


class SimulationDeadlock(RuntimeError):
    """Nothing is ready, no timer is scheduled, and no I/O event is pending.

    Every task is waiting on something that can never happen, so the
    simulation can make no further progress.
    """


class _JumpingSelector(selectors.BaseSelector):
    """Wraps a real selector; ``select`` advances virtual time.

    The real selector is still used for registration and is still polled
    (with timeout 0) on every call. That matters because the loop's
    self-pipe, which ``call_soon_threadsafe`` writes to, is a real file
    descriptor. Polling it means a ``call_soon_threadsafe`` made from the
    loop's own thread (the byte is already in the pipe) is seen on the next
    iteration.

    **Fast mode** (no pacer, or speed ``inf``): the poll never waits. If the
    loop is idle it jumps the clock or raises :class:`SimulationDeadlock`
    before a real thread can write its byte, so wake-ups from other threads
    are not waited for (D006). ``run_in_executor`` and ``to_thread`` are
    refused by :mod:`desyncio.guards`.

    **Real-time mode** (a pacer with finite speed): an idle ``select`` first
    waits on the real selector for ``(when - now) / speed`` seconds, so a
    byte in the self-pipe (speed change, pause, resume, or a user thread's
    ``call_soon_threadsafe``) ends the wait early. The deadlock rule is
    unchanged unless the pacer is paused.
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

    def _select_paced(
        self, pacer: RealTimePacer
    ) -> list[tuple[selectors.SelectorKey, int]] | None:
        """Real-time mode's idle step. ``None`` means "use the fast path".

        Called only when idle (nothing ready, no events). Steps:

        1. Paused: wait with no timeout for a wake-up; virtual time stays put.
        2. Else wait ``(min(when, limit) - now) / speed`` real seconds. The
           limit is a ``_BoundedClock``'s ``until``, so we never wait for
           time beyond it.
        3. Full wait: ``advance_to(when)`` (a bounded clock raises there if
           ``when`` is past ``until``). Interrupted wait: advance only by
           ``elapsed * speed``, capped at the target, and return the events.

        A real wait longer than ``MAX_REAL_WAIT`` (one day) is cut to one
        chunk. A chunk that ends with no events is *not* a full wait: the
        clock advances by ``elapsed * speed`` (capped at the target), we
        return ``[]`` and ``_run_once`` comes back for the next chunk.
        """
        loop = self._loop
        clock = loop.clock
        now = clock.now
        # ``limit`` exists on runners._BoundedClock. When the runner freezes
        # it at ``now`` for cleanup, real-time pacing must not apply: cleanup
        # either needs no time to pass or fails exactly as in fast mode.
        limit: float = getattr(clock, "limit", math.inf)
        if limit <= now:
            return None
        speed, paused = pacer.snapshot()
        wall = pacer.wall_clock
        if paused:
            return wall.wait(None, self._real)
        scheduled = loop._scheduled  # type: ignore[attr-defined]  # noqa: SLF001
        if speed == math.inf or not scheduled:
            return None  # fast mode, or deadlock (raised by the fast path)
        when: float = scheduled[0]._when
        target = min(when, limit)
        real_timeout = (target - now) / speed
        # select() overflows above ~9.2e9 s, so wait at most one chunk.
        chunked = real_timeout > MAX_REAL_WAIT
        if chunked:
            real_timeout = MAX_REAL_WAIT
        started = wall.monotonic()
        events = wall.wait(real_timeout, self._real) if real_timeout > 0 else []
        if not events and not chunked:
            clock.advance_to(when)  # waited the full time
            return events
        elapsed = wall.monotonic() - started
        advanced = now + elapsed * speed
        if chunked and not events:
            # A whole chunk passed but the timer is still ahead. If
            # ``elapsed * speed`` is below half a float step of ``now`` the
            # sum rounds back to ``now`` and the loop would never progress;
            # move at least one float step instead.
            advanced = max(advanced, math.nextafter(now, math.inf))
        # Returning [] (chunk) or the events makes _run_once iterate again;
        # the next idle select waits the next chunk (or the rest).
        clock.advance_to(min(target, advanced))
        return events

    def select(
        self, timeout: float | None = None
    ) -> list[tuple[selectors.SelectorKey, int]]:
        """Poll real fds; if idle, advance to the next timer.

        ``_run_once`` passes ``timeout == 0`` when callbacks are ready (or the
        loop is stopping): then we only poll. Otherwise it is positive or
        ``None`` and it would block; we jump the clock instead.
        """
        events = self._real.select(0)
        if events or timeout == 0:
            return events

        pacer = self._loop.pacer
        if pacer is not None:
            paced = self._select_paced(pacer)
            if paced is not None:
                return paced
            # None: fast behaviour applies (speed inf, frozen, or no timers).

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
    clock straight to the earliest pending timer (or, with a ``pacer``, waits
    the scaled real time first).

    Real I/O (sockets, subprocesses, executors, signals) is refused with
    :class:`~desyncio.guards.RealIOError`; see :mod:`desyncio.guards` for
    which entry points and why. :class:`~desyncio.guards.RealIOGuardMixin`
    comes first in the bases so its overrides win.
    """

    def __init__(
        self,
        start: float = 0.0,
        clock: VirtualClock | None = None,
        pacer: RealTimePacer | None = None,
        trace: TraceSink | None = None,
    ) -> None:
        """Create the loop.

        Args:
            start: initial simulation time; ignored if ``clock`` is given.
            clock: an existing clock to drive instead of a fresh one.
            pacer: if given, run in scaled real time under this pacer
                (see :mod:`desyncio.realtime`); ``None`` is fast mode.
            trace: a callable taking one :class:`~desyncio.trace.TraceRecord`;
                it receives every task start, resume, finish and cancel (see
                :mod:`desyncio.trace`). ``None`` (default) means no tracing
                and no tracing code on any path.
        """
        # Set before super().__init__: the base constructor may call time().
        self.clock = clock if clock is not None else VirtualClock(start)
        super().__init__()
        if trace is not None and not callable(trace):
            self.close()  # the base constructor opened a selector and sockets
            raise TypeError(
                f"trace must be callable or None, got {type(trace).__name__}"
            )
        # The base constructor created the real selector and registered the
        # self-pipe on it. Wrap it so the registrations carry over.
        self._selector = _JumpingSelector(self._selector, self)  # type: ignore[has-type]
        self._sequence = SequenceCounter()
        # Task naming (D015): one counter per loop, so names do not depend on
        # how many tasks other loops (or plain asyncio) created before.
        self._task_name_counter = itertools.count(1)
        self._next_task_name: str | None = None
        self._tracer = Tracer(trace, self.time) if trace is not None else None
        self.pacer = pacer
        if pacer is not None:
            try:
                pacer.bind(self)
            except BaseException:
                # The base constructor already opened a selector and the
                # self-pipe sockets; close them or they leak (ResourceWarning
                # "unclosed event loop" when this half-built loop is freed).
                self.pacer = None
                self.close()
                raise

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

    def name_next_task(self, name: str) -> None:
        """Give the next task created without an explicit name this ``name``.

        One-shot. ``desyncio.run`` uses it to call the main task ``main``,
        because ``asyncio.Runner`` creates that task itself and cannot pass a
        name.
        """
        self._next_task_name = name

    def stop_tracing_new_tasks(self) -> None:
        """Stop wrapping tasks created from now on (existing ones keep tracing).

        ``desyncio.run`` calls this before ``Runner.close()``: closing makes
        the runner create its own housekeeping tasks (async-generator and
        executor shutdown), which are not part of the model and would only
        add noise to the trace. Tasks that are still pending keep their
        wrapper, so their cleanup ``cancel`` records are still emitted.
        """
        self._tracer = None

    def create_task[T](
        self,
        coro: Coroutine[Any, Any, T],
        *,
        name: object = None,
        context: Context | None = None,
        eager_start: bool | None = None,
    ) -> asyncio.Task[T]:
        """Create a task with a deterministic default name; trace it if enabled.

        Every route that makes a task ends here: ``asyncio.create_task``,
        ``TaskGroup.create_task``, ``gather``/``ensure_future`` on a coroutine,
        ``sim.spawn`` and ``asyncio.Runner.run`` all call
        ``loop.create_task``. (A ``Task(...)`` built by hand bypasses it.)

        *Naming.* Without an explicit ``name`` the task is called ``proc-N``,
        N counting from 1 on this loop (stock asyncio uses a process-global
        ``Task-N``, which differs between runs in one process).

        *Tracing.* If the loop has a trace sink, the coroutine is wrapped in a
        :class:`~desyncio.trace._TracedCoroutine` (see :mod:`desyncio.trace`).
        Otherwise the user's coroutine goes to the Task untouched.
        """
        self._check_closed()  # type: ignore[attr-defined]
        if name is None:
            if self._next_task_name is not None:
                name, self._next_task_name = self._next_task_name, None
            else:
                name = f"proc-{next(self._task_name_counter)}"
        if self._tracer is not None and asyncio.iscoroutine(coro):
            coro = _TracedCoroutine(coro, str(name), self._tracer)
        if eager_start is None:  # 3.13's create_task has no such keyword
            return super().create_task(coro, name=name, context=context)
        return super().create_task(
            coro, name=name, context=context, eager_start=eager_start
        )

    def time(self) -> float:
        """Return the current simulation time instead of ``time.monotonic()``."""
        return self.clock.now
