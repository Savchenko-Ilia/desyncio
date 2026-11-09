"""A reference M/M/1 queue, written with desyncio's public API only.

M/M/1 is the "hello world" of queueing theory:

* **M**: arrivals are Markovian (a Poisson process), so the gaps between
  customers are exponentially distributed with rate ``arrival_rate`` (lambda);
* **M**: service times are exponentially distributed with rate
  ``service_rate`` (mu);
* **1**: a single server, and customers are served first-in first-out.

Theory says that with lambda < mu the mean time a customer spends in the system
(waiting plus being served) is ``1 / (mu - lambda)``.

How it maps onto asyncio
------------------------
* Every customer is its own task (``sim.spawn``), a "process" in DES terms.
* The single server is an ``asyncio.Lock``. ``Lock.acquire`` keeps its waiters
  in a FIFO deque, so the customer who has waited longest gets the server
  next. That is the queue, and we did not have to write one.
* ``sim.timeout(dt)`` is "hold for dt of *virtual* time". The clock jumps
  straight to the next event, so 10_000 customers take well under a second
  of real time.

Why the results are reproducible
--------------------------------
The two random streams are named (``"arrivals"`` and ``"service"``) and seeded
from the master seed given to ``desyncio.run``. Keeping them separate means
that changing how service times are drawn cannot shift the arrival times.
Nothing here reads the wall clock, ``hash()`` or the global ``random`` module.
"""

import asyncio
from dataclasses import dataclass

from desyncio import sim


@dataclass(frozen=True)
class CustomerRecord:
    """Timestamps (virtual time) of one customer's visit."""

    arrival: float
    service_start: float
    departure: float

    @property
    def time_in_system(self) -> float:
        return self.departure - self.arrival


async def mm1(
    n_customers: int = 10_000,
    arrival_rate: float = 0.9,
    service_rate: float = 1.0,
) -> list[CustomerRecord]:
    """Simulate ``n_customers`` through an M/M/1 queue.

    Must run inside ``desyncio.run(...)``. Returns one record per customer, in
    arrival order.
    """
    arrivals = sim.random("arrivals")  # a random.Random: its own seeded stream
    service = sim.random("service")
    server = asyncio.Lock()
    records: list[CustomerRecord | None] = [None] * n_customers

    async def customer(index: int) -> None:
        arrival = sim.now
        # The queue: waiting here is what "queueing" means. Waiters wake in
        # the order they arrived (FIFO).
        async with server:
            service_start = sim.now
            await sim.timeout(service.expovariate(service_rate))
        records[index] = CustomerRecord(arrival, service_start, sim.now)

    tasks: list[asyncio.Task[None]] = []
    for i in range(n_customers):
        await sim.timeout(arrivals.expovariate(arrival_rate))
        # Keep a reference: the loop holds tasks only weakly.
        tasks.append(sim.spawn(customer(i)))

    await asyncio.gather(*tasks)
    return [r for r in records if r is not None]


def mean_time_in_system(records: list[CustomerRecord]) -> float:
    """Average of departure - arrival over all customers."""
    return sum(r.time_in_system for r in records) / len(records)
