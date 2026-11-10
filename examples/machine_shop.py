"""A machine shop: random breakdowns, urgent repairs and routine maintenance.

What it models: five machines break down at random. Two repairmen fix them. Each
machine also gets routine maintenance from time to time, which needs a repairman too
but is less important. When both repairmen are busy and a machine breaks down, the
urgent repair takes a repairman away from a maintenance job (preemption); the
maintenance job goes back in the queue and later finishes its remaining work.

desyncio features shown: `PreemptiveResource` (priority requests that preempt),
`Interrupt` and `Preempted` (how a preempted process finds out), `TimeSeries` (fed by
a resource's `monitor=` hook), `sim.random` (one named stream per machine) and
`sim.spawn`.

How to run it: uv run python examples/machine_shop.py   (takes well under a second)
"""

from collections import Counter
from dataclasses import dataclass, field

from desyncio import (
    Interrupt,
    Preempted,
    PreemptiveResource,
    TimeSeries,
    run,
    sim,
)

END = 2000.0  # simulated time units
UPTIME, REPAIR = 25.0, 5.0  # mean time to failure, mean repair time
MAINT_GAP, MAINT_WORK = 40.0, 10.0  # mean time between maintenance jobs, its length
URGENT, ROUTINE = 0, 5  # priorities: a lower number wins


@dataclass
class Machine:
    name: str
    up: TimeSeries = field(default_factory=TimeSeries)  # 1 = working, 0 = broken
    breakdowns: int = 0


async def work(
    repairmen: PreemptiveResource, priority: int, amount: float, tally: Counter[str]
) -> None:
    """Do `amount` of work on a repairman, resuming after each preemption."""
    remaining = amount
    while remaining > 0:
        try:
            async with repairmen.request(priority=priority):
                await sim.timeout(remaining)
                remaining = 0
        except Interrupt as exc:
            # The Interrupt is raised by the `async with` itself, so it is caught
            # OUTSIDE it: by the time we get here the repairman is already gone.
            # Interrupt is not a CancelledError, so it does not end the task; a
            # real cancel (e.g. at the end of the run) still passes through.
            if not isinstance(exc.cause, Preempted):
                raise  # some other interrupt: not ours to handle
            remaining -= sim.now - exc.cause.usage_since  # work done before the loss
            # Counted per preemption, not per job: one maintenance job can be
            # preempted several times, so `preemptions` can exceed `maintenance jobs
            # finished`. (Repairs are never preempted: only a strictly better
            # priority preempts, and URGENT is already the best.)
            tally["preemptions"] += 1
            # The loop goes round and queues for a repairman again.


async def breakdowns(
    machine: Machine, repairmen: PreemptiveResource, tally: Counter[str]
) -> None:
    rng = sim.random(f"failures-{machine.name}")  # own stream per machine
    machine.up.record(1)
    while True:
        await sim.timeout(rng.expovariate(1 / UPTIME))
        machine.up.record(0)
        machine.breakdowns += 1
        await work(repairmen, URGENT, rng.expovariate(1 / REPAIR), tally)
        machine.up.record(1)


async def maintenance(
    machine: Machine, repairmen: PreemptiveResource, tally: Counter[str]
) -> None:
    # Simplification: the machine keeps running during maintenance; the job only
    # competes with repairs for the repairmen.
    rng = sim.random(f"maintenance-{machine.name}")
    while True:
        await sim.timeout(rng.expovariate(1 / MAINT_GAP))
        await work(repairmen, ROUTINE, MAINT_WORK, tally)
        tally["maintenance jobs finished"] += 1


async def main() -> None:
    busy = TimeSeries()
    busy.record(0)  # the monitor only fires on a change, so record the start
    repairmen = PreemptiveResource(capacity=2, monitor=lambda r: busy.record(r.count))
    tally: Counter[str] = Counter()
    machines = [Machine(f"M{i}") for i in range(1, 6)]
    for m in machines:
        # sim.spawn starts a coroutine as a task, like asyncio.create_task. These
        # loop forever; they are cancelled when main returns and the run ends.
        sim.spawn(breakdowns(m, repairmen, tally))
        sim.spawn(maintenance(m, repairmen, tally))
    await sim.timeout(END)

    for m in machines:
        up = m.up.mean(until=END)  # time-weighted: fraction of the time it was 1
        print(f"{m.name}: up {up:.1%} of the time, {m.breakdowns} breakdowns")
    print(f"repairmen utilisation: {busy.mean(until=END) / repairmen.capacity:.1%}")
    print(f"preemptions: {tally['preemptions']}")
    print(f"maintenance jobs finished: {tally['maintenance jobs finished']}")


if __name__ == "__main__":
    run(main(), seed=4)
