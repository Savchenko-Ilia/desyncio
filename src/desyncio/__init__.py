"""desyncio: async-native discrete-event simulation on a virtual-time asyncio loop.

Public API:

- Running: ``run``, ``sim``, ``RealTimePacer``
- Primitives: ``Resource``, ``PriorityResource``, ``PreemptiveResource``, ``Preempted``,
  ``Container``, ``Store``, ``FilterStore``, ``PriorityStore``
- Interrupts: ``Interrupt``
- Monitoring: ``TimeSeries``
- Errors: ``RealIOError``, ``SimulationDeadlock``
"""

from desyncio.container import Container
from desyncio.guards import RealIOError
from desyncio.interrupt import Interrupt
from desyncio.loop import SimulationDeadlock
from desyncio.monitor import TimeSeries
from desyncio.realtime import RealTimePacer
from desyncio.resource import Preempted, PreemptiveResource, PriorityResource, Resource
from desyncio.runners import run
from desyncio.sim import sim
from desyncio.store import FilterStore, PriorityStore, Store

__version__ = "0.1.0"

__all__ = [
    "Container",
    "FilterStore",
    "Interrupt",
    "Preempted",
    "PreemptiveResource",
    "PriorityResource",
    "PriorityStore",
    "RealIOError",
    "RealTimePacer",
    "Resource",
    "SimulationDeadlock",
    "Store",
    "TimeSeries",
    "__version__",
    "run",
    "sim",
]
