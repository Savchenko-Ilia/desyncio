"""desyncio: async-native discrete-event simulation on a virtual-time asyncio loop.

Public API:

- Running: ``run``, ``sim``, ``RealTimePacer``
- Primitives: ``Resource``, ``PriorityResource``, ``PreemptiveResource``, ``Preempted``,
  ``Container``, ``Store``, ``FilterStore``, ``PriorityStore``
- Interrupts: ``Interrupt``
- Statistics: ``TimeSeries``, ``Tally``, ``replicate``, ``confidence_interval``
- Calendar: ``Calendar``, ``Shift``, ``Schedule``, ``ScheduledResource``, ``ShiftEnded``
- Events: ``Subscription`` (returned by ``sim.subscribe``), ``ObserverError``
  (the debug-mode observer guard)
- Errors: ``RealIOError``, ``SimulationDeadlock``

The event types (``Event``, ``RunStarted``, ``RunFinished``, ``ResourceChanged``,
``EVENT_SCHEMA_VERSION``) stay in the :mod:`desyncio.events` submodule, which
``import desyncio`` makes available as ``desyncio.events``: write
``from desyncio.events import Event`` or ``desyncio.events.Event``. See
``docs/observing.md``.
"""

# The submodule itself is public (EVT-08); the "as" marks it as a re-export.
from desyncio import events as events
from desyncio.container import Container
from desyncio.dates import Calendar
from desyncio.events import ObserverError, Subscription
from desyncio.experiment import confidence_interval, replicate
from desyncio.guards import RealIOError
from desyncio.interrupt import Interrupt
from desyncio.loop import SimulationDeadlock
from desyncio.monitor import TimeSeries
from desyncio.realtime import RealTimePacer
from desyncio.resource import Preempted, PreemptiveResource, PriorityResource, Resource
from desyncio.runners import run
from desyncio.schedule import Schedule, Shift
from desyncio.scheduled import ScheduledResource, ShiftEnded
from desyncio.sim import sim
from desyncio.store import FilterStore, PriorityStore, Store
from desyncio.tally import Tally

__version__ = "0.1.0"

__all__ = [
    "Calendar",
    "Container",
    "FilterStore",
    "Interrupt",
    "ObserverError",
    "Preempted",
    "PreemptiveResource",
    "PriorityResource",
    "PriorityStore",
    "RealIOError",
    "RealTimePacer",
    "Resource",
    "Schedule",
    "ScheduledResource",
    "Shift",
    "ShiftEnded",
    "SimulationDeadlock",
    "Store",
    "Subscription",
    "Tally",
    "TimeSeries",
    "__version__",
    "confidence_interval",
    "replicate",
    "run",
    "sim",
]
