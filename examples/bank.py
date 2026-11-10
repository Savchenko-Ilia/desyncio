"""A one-teller bank: the classic M/M/1 queue.

What it models: customers arrive at random (on average 0.8 per time unit) at a bank
with a single teller, and each needs a random service time (on average 1.0). Queueing
theory gives the exact long-run answers, so you can check the simulation against them.

desyncio features shown: `Resource`, `sim.random` (named, seeded streams), `Tally`
(statistics per customer), `TimeSeries` (statistics of a value over time, with
`time_fractions`), and `replicate` + `confidence_interval` (an error bar from many
runs).

How to run it: uv run python examples/bank.py   (takes about a second or two)
"""

import asyncio

from desyncio import (
    Resource,
    Tally,
    TimeSeries,
    confidence_interval,
    replicate,
    run,
    sim,
)

ARRIVAL_RATE = 0.8  # lambda: customers arriving per time unit
SERVICE_RATE = 1.0  # mu: customers the teller can serve per time unit


async def bank(
    n_customers: int, warmup: float = 0.0
) -> tuple[Tally, TimeSeries, float]:
    """Simulate `n_customers` arrivals; return waits, head count and the end time."""
    teller = Resource(capacity=1)
    waits = Tally(warmup=warmup)  # drops waits recorded before sim time `warmup`
    in_system = TimeSeries()
    in_system.record(0)  # a TimeSeries needs its starting value recorded by hand
    arrivals, service = sim.random("arrivals"), sim.random("service")

    async def customer() -> None:
        arrived = sim.now
        in_system.record(in_system.last() + 1)
        async with teller:  # suspends, in virtual time, until the teller is free
            waits.record(sim.now - arrived)
            await sim.timeout(service.expovariate(SERVICE_RATE))
        in_system.record(in_system.last() - 1)

    # Awaiting sim.timeout() in the loop moves the virtual clock to the next arrival;
    # while main is suspended there, the customer tasks run. The TaskGroup waits for
    # every customer task when its `async with` block ends, so the queue is empty
    # (everyone served) after the loop.
    async with asyncio.TaskGroup() as tg:
        for _ in range(n_customers):
            await sim.timeout(arrivals.expovariate(ARRIVAL_RATE))
            tg.create_task(customer())
    return waits, in_system, sim.now


async def mean_wait() -> float:
    """One replication: a short run that starts empty, minus a warm-up period."""
    waits, _, _ = await bank(500, warmup=50)
    return waits.mean()


def main() -> None:
    # Theory for M/M/1 with utilisation rho = lambda / mu.
    rho = ARRIVAL_RATE / SERVICE_RATE
    theory_wq = rho / (SERVICE_RATE - ARRIVAL_RATE)  # mean wait in the queue
    theory_l = rho / (1 - rho)  # mean number in the system

    # 1. One run. run() starts a fresh virtual-time loop and returns the result.
    # A single run is one noisy sample, so expect it to be near the theory, not on it.
    waits, in_system, end = run(bank(3000), seed=1)
    p0 = in_system.time_fractions(until=end)[0]  # share of time with 0 in the system
    print(f"customers served: {len(waits)}")
    print(f"mean wait:      {waits.mean():.2f}   (theory {theory_wq:.2f})")
    print(f"mean in system: {in_system.mean(until=end):.2f}   (theory {theory_l:.2f})")
    print(f"time with 0 in system: {p0:.3f}   (theory {1 - rho:.3f})")

    # 2. Twenty short replications, seeds 100, 101, ..., and a 95% interval.
    means = replicate(mean_wait, 20, seed=100)
    mean, half_width = confidence_interval(means, level=0.95)
    print(f"20 replications: mean wait {mean:.2f} +- {half_width:.2f} (95% interval)")
    print(f"theoretical long-run mean wait: {theory_wq:.2f}")

    # Honest caveat: the interval is for the mean wait of THIS short experiment (500
    # customers, queue starting empty, waits before time 50 dropped). An empty start
    # makes early waits too short, and the warm-up removes most of that bias but not
    # all, so the interval can sit a little below the theory. More replications make
    # it narrower around that short-run mean; only longer runs or a longer warm-up
    # move the mean itself toward the theory (see docs/statistics.md).


if __name__ == "__main__":
    main()
