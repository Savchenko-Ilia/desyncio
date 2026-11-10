"""The ``sim`` facade: the everyday API for writing simulation processes.

Usage::

    from desyncio import sim

    async def customer():
        await sim.timeout(5)          # wait 5 units of virtual time
        print(sim.now)                # 5.0

    async def main():
        task = sim.spawn(customer())  # start a process
        await task                    # wait for it, get its result

    desyncio.run(main())

``sim`` is a module-level singleton. Nothing is passed into ``main``;
instead every attribute looks up the *running* loop each time it is used,
the way ``asyncio.sleep`` and ``asyncio.create_task`` do. Outside
``desyncio.run(...)`` (or on a plain asyncio loop) it raises ``RuntimeError``.
"""

import asyncio
from collections.abc import Coroutine
from random import Random
from typing import TYPE_CHECKING, Any

from desyncio._validate import to_float
from desyncio.bus import Handler, Subscription
from desyncio.interrupt import Interruptible, interrupt_task
from desyncio.loop import VirtualTimeLoop
from desyncio.runners import current_random_streams

if TYPE_CHECKING:
    import numpy as np

    from desyncio.events import Event

_NOT_RUNNING = (
    "desyncio.sim can only be used inside desyncio.run(...), on a "
    "VirtualTimeLoop; there is no such loop running here"
)


def _loop() -> VirtualTimeLoop:
    """Return the running VirtualTimeLoop or raise RuntimeError."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        raise RuntimeError(_NOT_RUNNING) from None
    if not isinstance(loop, VirtualTimeLoop):
        raise RuntimeError(_NOT_RUNNING)
    return loop


def _forbid_in_handler(loop: VirtualTimeLoop, call: str) -> None:
    """Observer guard (D059): raise if an event handler makes ``call``.

    Callers test ``loop._debug`` first, so outside debug mode the guard
    costs one attribute load.
    """
    bus = loop.bus
    if bus._dispatching:
        raise bus.forbid(call)


def _check_delay(dt: object) -> float:
    """Validate ``dt``: a finite real number >= 0 (bool is rejected)."""
    value = to_float(
        dt,
        type_error="dt must be a real number, got {type}",
        overflow_error="dt must be finite, got an int too large",
        finite_error="dt must be finite, got {value!r}",
    )
    if value < 0.0:
        raise ValueError(f"dt must be >= 0, got {value!r}")
    return value


class Sim:
    """Facade over the running simulation; use the singleton ``desyncio.sim``."""

    @property
    def now(self) -> float:
        """The current virtual time (``loop.time()``).

        Raises:
            RuntimeError: outside ``desyncio.run(...)``.
        """
        return _loop().time()

    async def timeout(self, dt: float, value: object = None) -> Any:
        """Wait ``dt`` units of virtual time, then return ``value`` (default None).

        ``value`` mirrors SimPy's ``timeout(delay, value)``. ``dt == 0`` yields
        to the loop once (other ready callbacks run first), like
        ``asyncio.sleep(0)``. Under the hood this is ``asyncio.sleep``, which
        schedules a timer on the loop, so cancelling the awaiting task cancels
        the timer.

        Raises:
            TypeError: ``dt`` is not a real number (``bool`` is rejected).
            ValueError: ``dt`` is NaN, infinite or negative.
            RuntimeError: outside ``desyncio.run(...)``.
        """
        # Checks run when the coroutine is awaited, not when it is created.
        _loop()
        return await asyncio.sleep(_check_delay(dt), value)

    def spawn[T](
        self, coro: Coroutine[Any, Any, T], name: str | None = None
    ) -> asyncio.Task[T]:
        """Start ``coro`` as a new task (process) and return the Task.

        The task is scheduled on the running loop and starts at the next loop
        iteration; ``spawn`` itself does not wait. The Task is awaitable:
        ``await task`` returns the coroutine's result, or re-raises the
        exception it ended with. If nobody awaits a failed task, asyncio logs
        "Task exception was never retrieved" when it is garbage collected, so
        await your tasks (or use ``asyncio.TaskGroup``).

        The loop keeps only a weak reference to tasks: keep the returned Task
        in a variable if the process must not be garbage collected.

        Args:
            coro: a coroutine object (``worker()``, not ``worker``).
            name: the task's name (``task.get_name()``). Default
                ``"proc-N"`` from the loop's one per-run counter, shared with
                ``asyncio.create_task`` and ``TaskGroup`` (see
                ``VirtualTimeLoop.create_task``), so names are identical in
                every run of the same model.

        Raises:
            TypeError: ``coro`` is not a coroutine object.
            RuntimeError: outside ``desyncio.run(...)``.
            ObserverError: called from an event handler in debug mode (D059;
                checked by ``loop.create_task``).
        """
        try:
            loop = _loop()
            if not asyncio.iscoroutine(coro):
                raise TypeError(
                    f"spawn() needs a coroutine object, got {type(coro).__name__} "
                    "(did you forget to call it?)"
                )
        except BaseException:
            # Close it so Python does not warn "coroutine was never awaited".
            if asyncio.iscoroutine(coro):
                coro.close()
            raise
        return loop.create_task(coro, name=name)

    def random(self, name: str) -> Random:
        """Return the named random stream of this run (same object each call).

        Streams derive from the ``seed`` given to ``desyncio.run``: the same
        seed gives the same sequence, a different seed a different one.

        Raises:
            RuntimeError: outside ``desyncio.run(...)``, or in a task created
                with a fresh ``contextvars.Context`` (it lacks the run's streams).
            ObserverError: called from an event handler in debug mode (a
                draw there would change the model's next value; D059).
        """
        loop = _loop()
        if loop._debug:  # type: ignore[attr-defined]
            _forbid_in_handler(loop, "sim.random")
        try:
            streams = current_random_streams()
        except RuntimeError:
            # _loop() passed, so a run is active: the variable is missing from
            # this task's context only if it was created with a fresh Context.
            raise RuntimeError(
                "the simulation is running, but this task's context lacks the "
                "run's random streams: the task was created with a fresh "
                "contextvars.Context (loop.create_task(..., context=...)); "
                "create it without context= to use sim.random()"
            ) from None
        return streams.stream(name)

    def np_random(self, name: str) -> "np.random.Generator":
        """Return the named numpy ``Generator`` of this run (same object each call).

        Seeded from the run seed and ``name``, independent of ``random(name)``.
        Call numpy's own distributions on it (``rng.poisson(...)``) and scipy's
        (``scipy.stats.truncnorm.rvs(..., random_state=rng)``).

        Raises:
            RuntimeError: outside ``desyncio.run(...)``, or in a task created
                with a fresh ``contextvars.Context`` (it lacks the run's streams).
            ObserverError: called from an event handler in debug mode (D059).
        """
        loop = _loop()
        if loop._debug:  # type: ignore[attr-defined]
            _forbid_in_handler(loop, "sim.np_random")
        try:
            streams = current_random_streams()
        except RuntimeError:
            raise RuntimeError(
                "the simulation is running, but this task's context lacks the "
                "run's random streams: the task was created with a fresh "
                "contextvars.Context (loop.create_task(..., context=...)); "
                "create it without context= to use sim.np_random()"
            ) from None
        return streams.np_stream(name)

    def interruptible(self) -> Interruptible:
        """Return an async context manager marking code that may be interrupted.

        Use it like ``asyncio.timeout()``::

            try:
                async with sim.interruptible() as scope:
                    await sim.timeout(10)
            except Interrupt as exc:
                print(exc.cause, scope.interrupted)

        Another process calls ``sim.interrupt(task, cause)``; the block then
        exits with ``desyncio.interrupt.Interrupt``. See that module's
        docstring for the full analogy and mechanism.

        Raises:
            RuntimeError: outside ``desyncio.run(...)``.
        """
        _loop()
        return Interruptible()

    def interrupt(self, task: "asyncio.Task[Any]", cause: object = None) -> None:
        """Interrupt ``task``'s innermost active ``interruptible()`` block.

        The interrupt is delivered at the current virtual time, at the task's
        current ``await`` (or the next one, if its wait already completed this
        instant): the block exits with ``Interrupt(cause)``. Like a timeout
        firing, it is ``task.cancel()`` underneath, and the block translates
        the cancellation in ``__aexit__``. A real ``task.cancel()`` always
        wins. A second ``interrupt`` to a block that is already interrupted
        is ignored (the first cause is kept); once the block has exited a new
        ``interruptible()`` block is needed.

        Raises:
            RuntimeError: ``task`` is finished, is the calling task itself, or
                is not inside an ``interruptible()`` block; or outside
                ``desyncio.run(...)``.
            ObserverError: called from an event handler in debug mode (D059).
        """
        loop = _loop()
        if loop._debug:  # type: ignore[attr-defined]
            _forbid_in_handler(loop, "sim.interrupt")
        interrupt_task(task, cause)

    def subscribe(
        self, handler: Handler, *, kinds: "tuple[type[Event], ...] | None" = None
    ) -> Subscription:
        """Call ``handler(event)`` for every event of this run from now on.

        ``handler`` is a plain function (an ``async def`` is refused); it is
        called synchronously at each emit point, in subscription order (see
        :mod:`desyncio.bus` for why). ``kinds`` restricts it to events that
        are instances of one of these ``Event`` subclasses; ``None`` (the
        default) means every event. Subscribing (or closing) while an event
        is being delivered takes effect from the next event.

        If ``handler`` raises, the exception never reaches the emitting
        task: delivery stops for good, the loop stops after the current
        iteration, and ``desyncio.run`` raises that exception (D058).

        A handler observes and must not change the run. In asyncio debug
        mode (``desyncio.run(..., debug=True)``) creating tasks, scheduling
        callbacks or timers, ``sim.interrupt``, ``sim.random``,
        ``sim.np_random`` and ``sim.emit`` inside a handler end the run with
        ``ObserverError`` (raised at the call, except ``loop.call_soon`` and
        a primitive's own event, such as ``ResourceChanged``, emitted during
        dispatch: those only record it, so the state change completes, D068;
        see :mod:`desyncio.bus`); ``subscribe`` and ``Subscription.close``
        stay allowed. Garbage collection during a handler can close a leaked
        task that held a resource; its release then records an
        ``ObserverError`` that blames the handler (D068 addendum). The
        guard cannot see direct mutation that schedules nothing, such as
        ``store.shutdown()`` or ``task.cancel()`` with no waiter to wake;
        see :mod:`desyncio.bus`.

        To see ``RunStarted``, subscribe with ``desyncio.run(...,
        subscribers=[...])`` instead.

        Returns:
            a :class:`~desyncio.events.Subscription`; ``close()`` it, or use
            it as a context manager, to stop receiving events.

        Raises:
            TypeError: ``handler`` is not callable or is an ``async def``
                function (best effort), or ``kinds`` is not a tuple of
                ``Event`` subclasses.
            ValueError: ``kinds`` is an empty tuple.
            RuntimeError: outside ``desyncio.run(...)``.
        """
        return _loop().bus.subscribe(handler, kinds)

    def emit(self, kind: "type[Event]", /, **fields: Any) -> None:
        """Emit a custom event: ``sim.emit(OrderShipped, order_id=7)``.

        The sim fills in ``t`` (``sim.now``) and ``seq``. ``seq`` advances on
        every call, whether or not anyone listens; the event object is built
        only if a subscriber wants ``kind``. Field names are therefore
        checked (by the dataclass constructor) only when it is built.

        Raises:
            TypeError: ``kind`` is not an ``Event`` subclass; or, when the
                event is built, ``fields`` do not match its fields.
            RuntimeError: outside ``desyncio.run(...)``.
            ObserverError: called from an event handler in debug mode (D059).
        """
        _loop().bus.emit(kind, fields)


sim = Sim()
