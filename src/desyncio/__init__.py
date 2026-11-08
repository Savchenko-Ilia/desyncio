"""desyncio: async-native discrete-event simulation on a virtual-time asyncio loop."""

from desyncio.runners import run
from desyncio.sim import sim

__version__ = "0.1.0"

__all__ = ["__version__", "run", "sim"]
