"""The per-run event bus behind ``sim.subscribe`` and ``sim.emit`` (EVT-02).

Every :class:`~desyncio.loop.VirtualTimeLoop` owns one :class:`Bus`, kept on
the loop's ``_state`` (D017, D060) rather than in a context variable, so a
task created with a fresh ``contextvars.Context`` still reaches it.

Subscribing
-----------
``sim.subscribe(handler, kinds=None)`` returns a :class:`Subscription`.
``handler`` is a plain (not ``async``) callable taking one event. ``kinds``
is ``None`` (every event) or a non-empty tuple of :class:`~desyncio.events.Event`
subclasses, matched like ``isinstance(event, kinds)``, so subclasses of a
listed kind match too. Handlers run in subscription order. A subscription
ends with ``close()`` (idempotent) or by leaving its ``with`` block.

``desyncio.run(..., subscribers=[...])`` subscribes before the run starts,
which is the only way to see ``RunStarted``.

Emitting
--------
``sim.emit(Kind, **fields)`` stamps the event with ``t`` (``sim.now``) and
``seq`` and hands it to every subscriber that wants ``Kind``. Two rules make
it cheap and keep the model independent of who listens (D060):

* ``seq`` ticks on **every** emit, also when nobody subscribed: the numbers
  an event gets never depend on the subscriptions.
* The event object is built only if some subscriber wants that kind. The bus
  keeps, per event class, the tuple of handlers that want it; the tuple is
  computed on the first emit of the class and dropped whenever somebody
  subscribes or closes. With no interested subscriber an emit is a dict
  lookup, an empty-tuple check and one integer add.

A consequence: a wrong field name (or ``t=``/``seq=`` given by hand) is
reported by the dataclass constructor only when the event is actually built,
i.e. only when someone subscribed to that kind.

Why delivery is inline
----------------------
Handlers are called synchronously, inside ``emit``, in the emitting task's
step. They therefore see the model exactly as it is at the emit point, and
nothing is added to the loop: scheduling the delivery with ``call_soon``
would put extra callbacks into ``_ready`` and change what runs when, so
merely subscribing could alter the model (AGENTS principle 4).

Handler errors (D058, modelled on trace sinks, D016)
----------------------------------------------------
An exception escaping a handler must not reach the emitting task: it would
replace that task's own exception, be swallowed by the model's
``try/except``, or abort a resource's grant half-way and leak a unit. So the
bus catches the first exception from any handler (any ``BaseException``,
like the tracer), then:

1. stops delivering: the remaining handlers do not get this event, and no
   handler gets any later event (the bus is closed, so there is no
   ``RunFinished`` either);
2. tells the loop, which calls ``loop.stop()``. ``stop()`` only sets a flag:
   the current loop iteration finishes (callbacks already in ``_ready`` run,
   as they would have), then ``run_forever`` returns and raises the handler's
   exception, the same object. Stopping at an iteration boundary is the only
   place the loop can be left without raising into some task.

``desyncio.run`` then tears down as usual and the exception propagates out of
``run``. ``KeyboardInterrupt`` and ``SystemExit`` are handled the same way
(contained, then re-raised by ``run`` once the loop has stopped), as for
trace sinks. If a trace sink also fails in the same run, the first of the two
errors is the one ``run`` raises (see ``VirtualTimeLoop._on_observer_error``).

Run lifecycle (D057)
--------------------
``desyncio.run`` emits ``RunStarted`` as the first event (``seq`` 0, before
any task's first step) and ``RunFinished`` as the last, then closes the bus
**before** tearing the loop down. Teardown cancels leftover tasks in
``asyncio.all_tasks()`` order, a set ordered by object address, so anything
emitted during teardown would differ between identical runs. After closing,
``emit`` is a silent no-op (it still validates the kind), and ``subscribe``
returns an already closed subscription, so teardown code never fails on them.

The observer guard (debug mode, D059)
-------------------------------------
Handlers observe; they must not change the run. In asyncio debug mode
(``desyncio.run(..., debug=True)``, ``PYTHONASYNCIODEBUG=1`` or
``loop.set_debug(True)``) the calls through which a handler would change it
raise :class:`ObserverError` while a handler runs: creating a task
(``sim.spawn``, ``asyncio.create_task``, ``loop.create_task``), scheduling a
callback or timer (``loop.call_soon``, ``call_later``, ``call_at``,
``call_at_priority``; ``call_soon`` is also reached indirectly, e.g. by a
future's ``set_result`` when a task waits on it), ``sim.interrupt``,
``sim.random``, ``sim.np_random`` and emitting (``sim.emit`` and primitives' events).
``sim.subscribe`` and ``Subscription.close`` stay allowed.

How it knows "a handler runs": :meth:`Bus._deliver` sets ``_dispatching``
for exactly the duration of the handler calls (saved and restored, so
nested emits and a raising handler leave it right). Delivery is synchronous,
so anything that happens while the flag is set was called, directly or
indirectly, by handler code: the loop's own scheduling never runs inside a
handler. ``RunStarted`` is delivered *from* a ``call_soon`` callback and
``RunFinished`` after the loop has stopped, but in both cases the flag is
only set inside the handler call itself. ``call_soon_threadsafe`` is not
guarded: other threads (a real-time pacer's ``resume``) and asyncio's own
async-generator finalizer (which garbage collection can run in the middle
of a handler) use it, and they are not handler code.

The guard records the error with the bus (as if the handler had raised it)
before raising, so a handler that catches ``ObserverError`` still ends the
run (D058).

``loop.call_soon`` is the exception to "raise": it records the error but
does not raise, and the callback is scheduled as usual. asyncio's own code
calls it on the handler's behalf in the middle of a state change: a
handler's ``fut.set_result(...)`` (or ``event.set()``, ``queue.put_nowait``,
``task.cancel()``) marks the future done and clears its callback list, then
schedules the waiters' wake-ups with ``call_soon``. Raising there would lose
those wake-ups, and a task whose wake-up is lost can no longer even be
cancelled, so teardown would deadlock and ``run`` would raise
``SimulationDeadlock`` instead (probe-verified). So the call completes, the
loop stops after the current iteration, and ``run`` raises the
``ObserverError``, whose notes show where the handler made the call. Every
other guarded call raises before it changes anything.

Outside debug mode nothing is checked and a violation's effect is
undefined. The flag itself is always maintained (two attribute stores
per delivered event). The callback checks sit in asyncio's existing
``if self._debug`` branches: ``VirtualTimeLoop._check_callback`` is called
by asyncio (and by our ``_schedule``) only in debug mode, so it adds
nothing outside it. The other checks are new but cheap: ``create_task``,
``sim.random``, ``sim.np_random`` and ``sim.interrupt`` each test
``loop._debug`` first (one attribute load when off), and ``emit`` tests
``_dispatching`` (one attribute load when no handler runs).

What the guard cannot see: mutation of model objects that schedules
nothing, because it goes through none of the calls above. Examples:
``task.cancel()`` of a task that is already in the ready queue (a task
waiting on a future *is* caught: cancelling it cancels the future, which
schedules the wake-up with ``call_soon``), ``store.shutdown()`` with no
getter or putter waiting, changing a resource's attributes, or drawing
from a ``random.Random`` stream that the handler got before the dispatch
(or from ``current_random_streams()``).

Helpers for primitives
----------------------
:func:`emit_event` and :func:`register_resource_name` are for desyncio's own
primitives, which also run on plain asyncio loops: there they do nothing.
"""

from __future__ import annotations

import asyncio
import inspect
import traceback
from collections.abc import Callable, Iterable
from types import TracebackType
from typing import TYPE_CHECKING, Any

from desyncio.loop import VirtualTimeLoop

if TYPE_CHECKING:
    from desyncio.events import Event

__all__ = [
    "Bus",
    "ObserverError",
    "Subscription",
    "emit_event",
    "register_resource_name",
]


class ObserverError(RuntimeError):
    """An event handler tried to change the run (debug mode only, D059).

    Raised in asyncio debug mode by the calls listed in the module docstring
    when an event handler makes them. A ``RuntimeError``, like asyncio's own
    "wrong context" errors (``call_soon`` from another thread in debug mode).
    It is a handler error: delivery stops and ``desyncio.run`` raises it.
    """


Handler = Callable[[Any], object]
"""A plain callable taking one event. Typed with ``Any`` so that a handler
annotated with the specific subclass it subscribes to type-checks."""

Kinds = tuple[type["Event"], ...]


def check_subscription(handler: object, kinds: object) -> Kinds | None:
    """Validate a ``(handler, kinds)`` pair; return ``kinds``.

    Raises:
        TypeError: ``handler`` is not callable, is an ``async def`` function
            (best effort: ``inspect.iscoroutinefunction``, so e.g. a lambda
            that returns a coroutine is not caught), or ``kinds`` is not
            ``None`` or a tuple of ``Event`` subclasses.
        ValueError: ``kinds`` is an empty tuple (it would match nothing;
            use ``None`` for every event).
    """
    from desyncio.events import Event  # lazy: events.py imports this module

    if not callable(handler):
        raise TypeError(f"handler must be callable, got {type(handler).__name__}")
    if inspect.iscoroutinefunction(handler):
        raise TypeError(
            "handler must be a plain function, not an async def: events are "
            "delivered synchronously and a coroutine would never run"
        )
    if kinds is None:
        return None
    if not isinstance(kinds, tuple):
        raise TypeError(
            f"kinds must be a tuple of Event subclasses or None, got "
            f"{type(kinds).__name__}"
        )
    if not kinds:
        raise ValueError("kinds must not be empty; use None for every event")
    for kind in kinds:
        if not (isinstance(kind, type) and issubclass(kind, Event)):
            raise TypeError(f"kinds must hold Event subclasses, got {kind!r}")
    return kinds


Subscriber = Handler | tuple[Handler, Kinds | None]
"""An item of ``desyncio.run(..., subscribers=[...])``: a handler, or a
``(handler, kinds)`` pair."""


def check_subscribers(subscribers: object) -> list[tuple[Handler, Kinds | None]]:
    """Validate ``run``'s ``subscribers=``; return ``(handler, kinds)`` pairs.

    ``None`` is no subscribers. Each item is a handler or a ``(handler,
    kinds)`` tuple, checked like :func:`check_subscription`.

    Raises:
        TypeError: not iterable, an item is a tuple that is not a pair, or as
            :func:`check_subscription`.
        ValueError: as :func:`check_subscription`.
    """
    if subscribers is None:
        return []
    if not isinstance(subscribers, Iterable):
        raise TypeError(
            "subscribers must be an iterable of handlers or (handler, kinds) "
            f"pairs, got {type(subscribers).__name__}"
        )
    pairs: list[tuple[Handler, Kinds | None]] = []
    for item in subscribers:
        if isinstance(item, tuple):
            if len(item) != 2:
                raise TypeError(
                    "a subscribers item must be a handler or a (handler, kinds) "
                    f"pair, got a tuple of length {len(item)}"
                )
            handler, kinds = item
        else:
            handler, kinds = item, None
        pairs.append((handler, check_subscription(handler, kinds)))
    return pairs


class Subscription:
    """One handler's subscription to a run's events.

    Returned by ``sim.subscribe``. ``close()`` ends it (calling it again does
    nothing); used as a context manager it closes on exit. Closing during a
    dispatch takes effect from the next event.
    """

    __slots__ = ("_active", "_bus", "_handler", "_kinds")

    def __init__(self, bus: Bus, handler: Handler, kinds: Kinds | None) -> None:
        self._bus = bus
        self._handler = handler
        self._kinds = kinds
        self._active = False

    @property
    def handler(self) -> Handler:
        """The subscribed callable."""
        return self._handler

    @property
    def kinds(self) -> Kinds | None:
        """The event classes it receives, or ``None`` for all."""
        return self._kinds

    @property
    def closed(self) -> bool:
        """True once closed, or once the run's bus has closed."""
        return not self._active

    def close(self) -> None:
        """Stop receiving events. Idempotent."""
        if self._active:
            self._active = False
            self._bus._remove(self)

    def __enter__(self) -> Subscription:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()


class Bus:
    """A loop's subscriptions, sequence counter and resource names.

    Args:
        time: returns the current virtual time (stamps ``t``).
        on_error: called with the exception a handler raised. Normally
            once; again only if a handler raises after a nested emit inside
            it already failed (or after catching an ``ObserverError``), so
            ``on_error`` must keep the first.
        debug: returns whether the observer guard is on (the loop's
            ``get_debug``); ``None`` means never.
    """

    __slots__ = (
        "_closed",
        "_debug",
        "_dispatching",
        "_names",
        "_on_error",
        "_seq",
        "_subs",
        "_taken",
        "_time",
        "_unnamed",
        "_wanted",
    )

    def __init__(
        self,
        time: Callable[[], float],
        on_error: Callable[[BaseException], object],
        debug: Callable[[], bool] | None = None,
    ) -> None:
        self._time = time
        self._on_error = on_error
        self._debug = debug
        # True while handlers are being called; see "The observer guard".
        self._dispatching = False
        self._seq = 0
        # Copy-on-write: a new tuple on every (un)subscribe, so a dispatch in
        # progress keeps iterating the snapshot it started with.
        self._subs: tuple[Subscription, ...] = ()
        # Event class -> handlers that want it; cleared on (un)subscribe.
        self._wanted: dict[object, tuple[Handler, ...]] = {}
        self._closed = False
        self._names: dict[object, str] = {}  # resource -> its name in this run
        self._taken: set[str] = set()  # membership tests only, never iterated
        self._unnamed = 0

    @property
    def closed(self) -> bool:
        """True after :meth:`close` or after a handler error."""
        return self._closed

    @property
    def guarding(self) -> bool:
        """True while a handler runs and the observer guard is on."""
        return self._dispatching and self._debug is not None and self._debug()

    def forbid(self, call: str, *, raised: bool = True) -> ObserverError:
        """Record and return the ``ObserverError`` for ``call`` made by a handler.

        With ``raised`` the caller raises it; recording it first (as a handler
        error) means a handler that catches it still ends the run. Without,
        the call goes on (see ``loop.call_soon`` in the module docstring) and
        the current stack is attached as a note, since the error is raised
        later, by ``run``, with no traceback into the handler.
        """
        error = ObserverError(
            f"{call} was called from an event handler: observers must not "
            "change the run (no new tasks, callbacks, timers, interrupts, "
            "random draws or events); the guard is on because the loop is in "
            "asyncio debug mode"
        )
        if not raised:
            # Drop forbid, _check_callback and call_soon themselves.
            stack = traceback.format_stack()[:-3]
            error.add_note("called from:\n" + "".join(stack).rstrip())
        self._fail(error)
        return error

    def subscribe(self, handler: Handler, kinds: Kinds | None = None) -> Subscription:
        """Add a subscription at the end of the delivery order.

        On a closed bus the subscription is returned already closed.
        """
        kinds = check_subscription(handler, kinds)
        sub = Subscription(self, handler, kinds)
        if not self._closed:
            sub._active = True
            self._subs = (*self._subs, sub)
            self._wanted.clear()
        return sub

    def _remove(self, sub: Subscription) -> None:
        self._subs = tuple(s for s in self._subs if s is not sub)
        self._wanted.clear()

    def emit(self, kind: type[Event], fields: dict[str, Any]) -> None:
        """Tick ``seq``; build ``kind(t=..., seq=..., **fields)`` and deliver it
        only if a subscriber wants ``kind``.

        Raises:
            TypeError: ``kind`` is not an ``Event`` subclass (always checked),
                or the constructor rejects ``fields`` (only when built).
            ObserverError: called from a handler in debug mode.
        """
        if self._dispatching and self.guarding:
            raise self.forbid("emit (sim.emit or a primitive's event)")
        try:
            handlers = self._wanted[kind]
        except (KeyError, TypeError):  # TypeError: unhashable, so not a class
            handlers = self._resolve(kind)
        seq = self._seq
        self._seq = seq + 1
        if handlers:
            self._deliver(kind(t=self._time(), seq=seq, **fields), handlers)

    def _resolve(self, kind: object) -> tuple[Handler, ...]:
        """Validate ``kind`` and cache the handlers that want it."""
        from desyncio.events import Event  # lazy: events.py imports this module

        if not (isinstance(kind, type) and issubclass(kind, Event)):
            raise TypeError(f"emit() needs an Event subclass, got {kind!r}")
        handlers = tuple(
            s._handler
            for s in self._subs
            if s._kinds is None or issubclass(kind, s._kinds)
        )
        self._wanted[kind] = handlers
        return handlers

    def _deliver(self, event: Event, handlers: tuple[Handler, ...]) -> None:
        # Saved and restored, not just set and cleared: a handler may emit
        # (outside debug mode), and the inner dispatch must leave it True.
        outer = self._dispatching
        self._dispatching = True
        try:
            for handler in handlers:
                # Closed by a failure, possibly in a nested emit from a handler.
                if self._closed:
                    return
                handler(event)
        except BaseException as exc:
            # BaseException on purpose, as in Tracer.emit: nothing a handler
            # raises (CancelledError included) may reach the task. The flag is
            # cleared first: _fail is the bus's own code, not the handler's.
            self._dispatching = outer
            self._fail(exc)
        finally:
            self._dispatching = outer

    def _fail(self, exc: BaseException) -> None:
        # A handler can raise after a nested emit (from that handler) has
        # already failed, so this may run twice. The loop keeps the first
        # error (VirtualTimeLoop._on_observer_error); it is the one record.
        self.close()
        self._on_error(exc)

    def close(self) -> None:
        """End delivery for good: close every subscription. Idempotent."""
        if self._closed:
            return
        self._closed = True
        subs, self._subs = self._subs, ()
        for sub in subs:
            sub._active = False
        self._wanted.clear()

    def register_resource(self, resource: object, name: str | None) -> str:
        """Return ``resource``'s name in this run, registering it on first call.

        Names, explicit or automatic, are unique per run (D064). The first
        call gives an unnamed resource ``resource-N``, with N the next number
        (from 1, in registration order) whose name is still free: an
        automatic name skips a name another resource took explicitly, so an
        unnamed resource never raises. Later calls for the same object return
        the same name and ignore ``name``. A failed call changes nothing.

        Raises:
            ValueError: the explicit ``name`` is already used by another
                resource in this run, explicitly or as an automatic name
                (``"resource-1"`` given after an unnamed resource got it).
        """
        known = self._names.get(resource)
        if known is not None:
            return known
        if name is None:
            n = self._unnamed + 1
            while f"resource-{n}" in self._taken:
                n += 1
            self._unnamed = n
            name = f"resource-{n}"
        elif name in self._taken:
            raise ValueError(f"resource name {name!r} is already used in this run")
        self._taken.add(name)
        self._names[resource] = name
        return name


def _running_bus() -> Bus | None:
    # asyncio._get_running_loop returns None instead of raising, which keeps
    # the "no loop" case cheap.
    loop = asyncio._get_running_loop()
    if isinstance(loop, VirtualTimeLoop):
        return loop.bus
    return None


def emit_event(kind: type[Event], /, **fields: Any) -> None:
    """Emit from the running loop; do nothing without a running ``VirtualTimeLoop``.

    For primitives, which also work on plain asyncio (D060). Cost when nobody
    wants ``kind``: the call itself (building ``fields``), the running-loop
    lookup and ``isinstance``, then the bus's dict lookup and integer add.
    """
    bus = _running_bus()
    if bus is not None:
        bus.emit(kind, fields)


def register_resource_name(resource: object, name: str | None) -> str | None:
    """Register ``resource`` with the running run; see :meth:`Bus.register_resource`.

    Returns ``name`` unchanged (possibly ``None``) when no ``VirtualTimeLoop``
    is running. Call it before the resource's first state change in a run.
    """
    bus = _running_bus()
    if bus is None:
        return name
    return bus.register_resource(resource, name)
