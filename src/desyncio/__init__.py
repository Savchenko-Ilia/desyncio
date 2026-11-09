"""desyncio: async-native discrete-event simulation on a virtual-time asyncio loop.

Public API:

- Running: ``run``, ``sim``, ``RealTimePacer``
- Primitives: ``Resource``, ``PriorityResource``, ``PreemptiveResource``, ``Preempted``,
  ``Container``, ``Store``, ``FilterStore``, ``PriorityStore``
- Interrupts: ``Interrupt``
- Statistics: ``TimeSeries``, ``Tally``, ``replicate``, ``confidence_interval``
- Errors: ``RealIOError``, ``SimulationDeadlock``
"""

from desyncio.container import Container
from desyncio.experiment import confidence_interval, replicate
from desyncio.guards import RealIOError
from desyncio.interrupt import Interrupt
from desyncio.loop import SimulationDeadlock
from desyncio.monitor import TimeSeries
from desyncio.realtime import RealTimePacer
from desyncio.resource import Preempted, PreemptiveResource, PriorityResource, Resource
from desyncio.runners import run
from desyncio.sim import sim
from desyncio.store import FilterStore, PriorityStore, Store
from desyncio.tally import Tally

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
    "Tally",
    "TimeSeries",
    "__version__",
    "confidence_interval",
    "replicate",
    "run",
    "sim",
]
