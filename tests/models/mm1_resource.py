"""A reference M/M/1 queue built on ``desyncio.Resource``, public API only.

Same queue as ``mm1.py`` (exponential gaps at ``arrival_rate``, exponential
service at ``service_rate``, one server, first-in first-out), but the server is
a named ``Resource`` instead of an ``asyncio.Lock``. That makes the run emit
``ResourceChanged`` events (a request queueing, a grant, a release), which is
what the event-determinism tests look at.

* every customer is a task made with ``sim.spawn``;
* the streams ``"arrivals"`` and ``"service"`` are named and seeded from the
  master seed, so changing one cannot shift the other;
* nothing reads the wall clock, ``hash()`` or the global ``random`` module.

``mm1_resource`` returns a few results (mean wait, served count) so tests can
check that observing the run does not change the model.
"""

import asyncio
from dataclasses import dataclass

from desyncio import Resource, sim


@dataclass(frozen=True)
class Mm1Results:
    """What one run of the model measured (virtual time)."""

    served: int
    mean_wait: float
    last_departure: float


async def mm1_resource(
    n_customers: int = 300,
    arrival_rate: float = 0.9,
    service_rate: float = 1.0,
) -> Mm1Results:
    """Simulate ``n_customers`` through an M/M/1 queue; run inside ``desyncio.run``.

    With ``until=`` the run may be cut short while customers still hold or wait
    for the server; then ``desyncio.run`` returns ``None`` and this never
    returns.
    """
    arrivals = sim.random("arrivals")
    service = sim.random("service")
    server = Resource(1, name="server")
    waits: list[float] = []

    async def customer() -> None:
        arrival = sim.now
        async with server:
            waits.append(sim.now - arrival)
            await sim.timeout(service.expovariate(service_rate))

    tasks: list[asyncio.Task[None]] = []
    for _ in range(n_customers):
        await sim.timeout(arrivals.expovariate(arrival_rate))
        tasks.append(sim.spawn(customer()))  # keep a reference: the loop is weak

    await asyncio.gather(*tasks)
    return Mm1Results(
        served=len(waits),
        mean_wait=sum(waits) / len(waits),
        last_departure=sim.now,
    )
