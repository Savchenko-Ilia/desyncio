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

Helpers for primitives
----------------------
:func:`emit_event` and :func:`register_resource_name` are for desyncio's own
primitives, which also run on plain asyncio loops: there they do nothing.
"""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Callable, Iterable
from types import TracebackType
from typing import TYPE_CHECKING, Any

from desyncio.loop import VirtualTimeLoop

if TYPE_CHECKING:
    from desyncio.events import Event

__all__ = ["Bus", "Subscription", "emit_event", "register_resource_name"]

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
        on_error: called once with the first exception a handler raises.
    """

    __slots__ = (
        "_closed",
        "_names",
        "_on_error",
        "_seq",
        "_subs",
        "_taken",
        "_time",
        "_unnamed",
        "_wanted",
        "error",
    )

    def __init__(
        self, time: Callable[[], float], on_error: Callable[[BaseException], object]
    ) -> None:
        self._time = time
        self._on_error = on_error
        self._seq = 0
        # Copy-on-write: a new tuple on every (un)subscribe, so a dispatch in
        # progress keeps iterating the snapshot it started with.
        self._subs: tuple[Subscription, ...] = ()
        # Event class -> handlers that want it; cleared on (un)subscribe.
        self._wanted: dict[object, tuple[Handler, ...]] = {}
        self._closed = False
        self.error: BaseException | None = None
        """The first exception a handler raised, or ``None``."""
        self._names: dict[object, str] = {}  # resource -> its name in this run
        self._taken: set[str] = set()  # membership tests only, never iterated
        self._unnamed = 0

    @property
    def closed(self) -> bool:
        """True after :meth:`close` or after a handler error."""
        return self._closed

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
        """
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
        for handler in handlers:
            # Closed by a failure, possibly in a nested emit from a handler.
            if self._closed:
                return
            try:
                handler(event)
            except BaseException as exc:
                # BaseException on purpose, as in Tracer.emit: nothing a
                # handler raises (CancelledError included) may reach the task.
                self._fail(exc)
                return

    def _fail(self, exc: BaseException) -> None:
        if self.error is not None:
            # A handler raised after a nested emit (from that handler) had
            # already failed: the first error stands.
            return
        self.error = exc
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
