# desyncio

Discrete-event simulation that runs on asyncio itself. `desyncio` replaces the clock
under the event loop with a virtual one, so `asyncio.sleep`, `Queue`, `Semaphore`,
`TaskGroup` and `asyncio.timeout()` work in simulated time. Simulations run as fast
as your CPU allows, or scaled to real time. The same seed always gives the same run.

> **Status:** early development (version 0.1.0). The virtual-time loop,
> the DES primitives (resources, containers, stores, interrupts) and the monitoring and
> random-stream helpers below are implemented and tested; the example below runs as written.
> The API may still change.

```python
import asyncio
import desyncio


async def ship(berth: asyncio.Semaphore, name: str) -> None:
    async with berth:
        await asyncio.sleep(12)  # 12 virtual hours, returns instantly
        print(f"{name} leaves at t={asyncio.get_running_loop().time()}")


async def main() -> None:
    berth = asyncio.Semaphore(1)
    async with asyncio.TaskGroup() as tg:
        for i in range(3):
            tg.create_task(ship(berth, f"ship-{i}"))


desyncio.run(main())  # ship-0 at 12, ship-1 at 24, ship-2 at 36
```

## Goals

- Plain `async def` processes on a real asyncio event loop
- Deterministic: seeded random streams and explicit ordering of simultaneous events
- Fast-as-possible and scaled real-time modes
- Classic DES primitives: resources, containers, stores, interrupts

## Public API

Everything below is importable from the top level: `from desyncio import Resource, run, sim`.

- `run`: run a coroutine on a fresh virtual-time loop (`seed=`, `until=`, `speed=`, `trace=`)
- `sim`: the facade used inside a run (`sim.now`, `sim.timeout`, `sim.spawn`, `sim.random`,
  `sim.np_random`, `sim.interruptible`, `sim.interrupt`)
- `Resource`: counted resource, first come first served
- `PriorityResource`: served by `(priority, request order)`
- `PreemptiveResource`: a priority resource whose holders can be preempted
- `Preempted`: the `Interrupt.cause` a preempted holder receives
- `Container`: a level of homogeneous stuff with blocking `put`/`get`
- `Store`: FIFO buffer of items
- `FilterStore`: store whose `get` takes the first item matching a filter
- `PriorityStore`: store that hands out the lowest priority value first
- `Interrupt`: raised by `async with sim.interruptible():` when another process interrupts it
- `TimeSeries`: time-weighted statistics, fed from a primitive's `monitor=` hook
- `Tally`: statistics of individual observations (mean, quantiles, histogram, `warmup=`)
- `replicate`: run a model `n` times with consecutive seeds
- `confidence_interval`: Student t interval for the mean of the replications
- `RealTimePacer`: pacing for scaled real-time runs
- `RealIOError`: raised when a simulation tries real I/O
- `SimulationDeadlock`: raised when nothing can ever run again
- `__version__`: the package version

## Documentation

- [Primitives guide](docs/primitives.md): every primitive with a runnable example, what is
  delegated to asyncio and what desyncio adds, and the ordering and cancellation guarantees.
  The examples are run by the test suite.

- [How the loop works](docs/how-the-loop-works.md): how asyncio's `_run_once`, timer
  heap and selector are turned into a virtual-time loop, with a traced worked example.

- [Statistics guide](docs/statistics.md): `Tally`, `TimeSeries`, warm-up, replications with
  confidence intervals, and input distributions with `sim.np_random` (numpy and scipy do the maths).
  The examples are run by the test suite.

## License

Apache-2.0
