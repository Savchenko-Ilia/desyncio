"""Typed events that a simulation run produces (schema version 1).

An event is a small immutable record: ``t`` (virtual time) and ``seq`` (a
per-run counter that gives a total order, also among events at the same ``t``)
plus the fields of its kind. ``to_dict()`` turns it into a plain dict for JSON,
logs or a database; ``"kind"`` in that dict is the class name.

Built-in events: :class:`RunStarted`, :class:`RunFinished` and
:class:`ResourceChanged`. desyncio has no entity or block events (D056); a
platform built on desyncio defines its own as subclasses of :class:`Event`.

Subclassing :class:`Event` (supported public contract)
-----------------------------------------------------
Define your own kind like this::

    @dataclass(frozen=True, slots=True, kw_only=True)
    class OrderShipped(Event):
        order_id: int
        carrier: str

Rules:

* Use exactly the decorator above.

  - ``frozen=True`` makes events immutable, so one subscriber cannot change
    what the next one sees.
  - ``slots=True`` removes the per-instance ``__dict__`` (smaller, and no
    stray attributes). It works together with ``frozen=True``; each subclass
    must repeat it, otherwise that subclass gets a ``__dict__`` again.
  - ``kw_only=True`` makes every field keyword-only. A dataclass normally
    refuses a required field after a field with a default; keyword-only
    fields are exempt from that ordering rule, so a subclass can add required
    fields whatever the base class declares. It also matches how events are
    built: ``sim.emit(Kind, **fields)`` passes names, never positions.
* Fields are primitives only: ``int``, ``float``, ``str``, ``bool`` or
  ``None`` (also as ``Literal[...]`` or a union of these). That keeps every
  event trivially serialisable and comparable.
* ``kind`` is the class name, not a qualified name. Two classes with the same
  name collide in ``to_dict()["kind"]``; pick distinct names.
* Do not fill in ``t`` and ``seq`` yourself. The intended use is
  ``sim.emit(OrderShipped, order_id=7, carrier="x")``: the sim supplies ``t``
  and ``seq`` and builds the event only if somebody subscribed to that kind.

:class:`Subscription` (what ``sim.subscribe`` returns) is defined in
:mod:`desyncio.bus` and re-exported here.
"""

from __future__ import annotations

from dataclasses import dataclass, fields
from typing import Any, Literal

# bus.py imports this module only inside functions, so there is no cycle.
from desyncio.bus import Subscription

__all__ = [
    "EVENT_SCHEMA_VERSION",
    "Event",
    "ResourceChanged",
    "RunFinished",
    "RunStarted",
    "Subscription",
]

#: Bumped on any change to the fields of a built-in event.
EVENT_SCHEMA_VERSION = 1


@dataclass(frozen=True, slots=True, kw_only=True)
class Event:
    """Base class of all events: virtual time ``t`` and per-run sequence ``seq``."""

    t: float
    seq: int

    def to_dict(self) -> dict[str, Any]:
        """All fields as a plain dict, plus ``"kind"`` (the class name).

        Uses :func:`dataclasses.fields` on the instance, so it also covers
        the fields of user subclasses. Values are primitives, so this
        shallow copy is a safe snapshot.
        """
        out: dict[str, Any] = {f.name: getattr(self, f.name) for f in fields(self)}
        out["kind"] = type(self).__name__
        return out


@dataclass(frozen=True, slots=True, kw_only=True)
class RunStarted(Event):
    """First event of a run. ``until`` is the run's time limit, or ``None``."""

    seed: int
    until: float | None


@dataclass(frozen=True, slots=True, kw_only=True)
class RunFinished(Event):
    """Last event of a run.

    ``reason``: ``"main_returned"`` (the main coroutine finished), ``"until"``
    (the time limit was reached) or ``"error"`` (including a deadlock). It is
    only typed as a ``Literal``, not validated at construction: the sim is the
    only producer, and mypy checks the values.
    """

    reason: Literal["main_returned", "until", "error"]


@dataclass(frozen=True, slots=True, kw_only=True)
class ResourceChanged(Event):
    """A resource's state changed; the fields are its state after the change."""

    resource: str
    in_use: int
    capacity: int
    queue_len: int
