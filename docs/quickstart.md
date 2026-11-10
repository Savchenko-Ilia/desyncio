# Quick start

This page installs desyncio and walks you through a first model: customers queue for one
server, and you measure how long they wait. It takes a few minutes.

## 1. Install

desyncio is not on PyPI yet, so install it from GitHub. With [uv](https://docs.astral.sh/uv/):

```text
uv add git+https://github.com/Savchenko-Ilia/desyncio
```

or with pip:

```text
pip install git+https://github.com/Savchenko-Ilia/desyncio
```

You need Python 3.13 or newer. numpy and scipy are installed with it (the statistics use
them), so there is nothing else to add.

## 2. A first model

A small shop has one counter. A customer arrives on average every 5 time units, and serving
one takes 4 on average. Both times are random. How long do customers wait before they are
served?

Save this as `shop.py` and run `python shop.py`. (The block below is also run by the test
suite, so the output shown is exactly what it prints.)

```python
import asyncio

import desyncio
from desyncio import Resource, Tally, sim


async def customer(counter: Resource, waits: Tally) -> None:
    arrived = sim.now
    async with counter:
        waits.record(sim.now - arrived)
        await sim.timeout(sim.random("service").expovariate(1 / 4))


async def main() -> None:
    waits = Tally()
    counter = Resource(1)
    arrivals = sim.random("arrivals")
    async with asyncio.TaskGroup() as tg:
        for _ in range(200):
            await sim.timeout(arrivals.expovariate(1 / 5))
            tg.create_task(customer(counter, waits))
    print(f"customers: {len(waits)}")
    print(f"finished at t={sim.now:.1f}")
    print(f"mean wait: {waits.mean():.2f}")
    print(f"p90 wait: {waits.quantile(0.9):.2f}")


desyncio.run(main(), seed=1)
```

```text
customers: 200
finished at t=823.5
mean wait: 12.61
p90 wait: 35.39
```

### What each part does

- **`desyncio.run(main(), seed=1)`** runs the coroutine `main()` on an asyncio event loop
  whose clock is virtual. It returns when `main()` returns.
- **`sim`** is the handle you use inside a run. `sim.now` is the current virtual time. Time is
  a plain number; by convention one unit is a second, but read it as minutes if you like.
- **`sim.random("arrivals")`** gives you a random number generator (a `random.Random`) named
  `"arrivals"`. It is seeded from the `seed` you passed to `run`. Using one name per purpose
  (`"arrivals"`, `"service"`) keeps the streams independent of each other.
  `expovariate(1 / 5)` draws a random time with an average of 5.
- **`main()`** is the arrival process. It loops 200 times: wait for the next arrival, then
  start a `customer` task for it. `asyncio.TaskGroup` is plain asyncio; leaving the
  `async with` waits until every customer is done, so the prints come after the last customer
  has left.
- **`Resource(1)`** is a counter with room for one customer at a time. `async with counter:`
  waits in line (first come, first served) until the counter is free, and gives it back when
  the block ends.
- **`customer()`** notes the time it arrived, queues for the counter, and the moment it gets
  the counter records `sim.now - arrived`, its wait, in a `Tally`. Then it holds the counter
  for a random service time.
- **`Tally`** collects individual observations. `mean()` and `quantile(0.9)` summarise them;
  the 0.9 quantile (p90) is the wait that 90% of the customers did not exceed. See the
  [statistics guide](statistics.md) for the rest.

### Why `await sim.timeout(...)` returns instantly

`await sim.timeout(5)` does not make your program wait 5 seconds. It tells the loop "wake this
task at virtual time `now + 5`". When every task is waiting like that and nothing is ready to
run, the loop does not sleep: it moves the virtual clock straight to the earliest wake-up time
and carries on. So the 823 time units above pass in a blink of real time, and a long model can
run in seconds. [How the loop works](how-the-loop-works.md) shows this step by step.

### Why the same seed gives the same output

All the randomness in this model comes from the named streams, and the streams come from the
seed. Events at the same virtual instant are processed in a fixed order. So running the file
again with `seed=1` prints the same four lines, and another seed gives a different run.

One run is only one sample, and a short one. The mean wait here is noisy, and because the shop
starts empty it is also biased a little low: with these rates the long-run mean wait is 16, so
12.61 underestimates it. To average over many seeds and get a confidence interval, see
`replicate` in the
[statistics guide](statistics.md#replications-and-confidence-intervals); it also shows how a
warm-up reduces the empty-start bias.

## 3. Where next

- [Primitives guide](primitives.md): `Resource`, `Container`, `Store`, priorities, preemption
  and interrupts, each with a runnable example.
- [Statistics guide](statistics.md): `Tally`, `TimeSeries`, warm-up, replications with
  confidence intervals, and input distributions.
- [Calendar guide](calendar.md): real dates, daylight-saving days, and shifts that change a
  resource's capacity.
- [How the loop works](how-the-loop-works.md): how asyncio's event loop becomes a
  virtual-time loop.
- [`examples/`](../examples/): complete example models.
