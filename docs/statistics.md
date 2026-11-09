# Statistics guide

A simulation run produces numbers, and a single number from a single run tells you little.
This guide shows how to collect observations, summarise them, remove the start-up bias,
repeat the run with different seeds and put an error bar on the answer, and how to draw
random inputs for your model. Everything is built around one small model, an M/M/1 queue:
one server, customers arriving at random (on average 0.8 per time unit) and needing a random
service time (on average 1.0). Queueing theory gives the exact long-run answers, so you can
check the output: the server is busy 80% of the time, the mean number in the system is 4,
and the mean wait in the queue is 4.

Every `python` block below is executed by `tests/test_docs_examples.py`, and the `text` block
under it is compared with what the example really prints (the same convention as the
[primitives guide](primitives.md)). All seeds are fixed, so the output is the same on every
run. All examples use top-level imports (`from desyncio import ...`).

**The maths is numpy's and scipy's** (D048). desyncio only collects the numbers and passes them
on: each section names the numpy or scipy function that does the work, and that function's
documentation is the place to look for details. The exceptions are `TimeSeries.mean` and
`TimeSeries.time_fractions`, two plain sums that desyncio computes itself (see that section).
numpy and scipy are imported only when you call a statistics method that uses them, so a
simulation that uses none of this pays nothing for them.

Contents:

1. [Tally: statistics of observations](#tally-statistics-of-observations)
2. [TimeSeries: statistics of a value over time](#timeseries-statistics-of-a-value-over-time)
3. [Warm-up](#warm-up)
4. [Replications and confidence intervals](#replications-and-confidence-intervals)
5. [Input distributions: sim.np_random](#input-distributions-simnp_random)

## Tally: statistics of observations

`Tally()` collects individual observations that are not tied to a stretch of time, such as the
waiting time of each customer. Use it when you want "the average over customers" (or over jobs,
parts, calls). `tally.record(x)` adds an observation. Then:

- `mean()`, `variance()`, `stdev()`, `min()`, `max()` (`variance` and `stdev` are the sample
  versions, dividing by n-1, and need two observations): `numpy.mean`, `numpy.var`, ...
- `quantile(p)`: `numpy.quantile`. The 0.5 quantile is the median, 0.9 the value that 90% of
  the observations stay below. Pass a list to get several at once.
- `histogram(bins)`: `numpy.histogram`, returned as a pair `(counts, edges)`: a tuple of the
  counts per bin and a tuple of the bin edges (one more edge than counts).
- `len(tally)` and `tally.samples` give the raw observations.

The model: customers arrive, wait for the single server (a `Resource`), and record how long
they waited. The `service` and `arrivals` streams come from `sim.random(name)`: named,
seeded streams, so the run is reproducible.

```python
import asyncio

from desyncio import Resource, Tally, run, sim


async def main() -> Tally:
    server = Resource(1)
    waits = Tally()
    arrivals, service = sim.random("arrivals"), sim.random("service")

    async def customer() -> None:
        arrived = sim.now
        async with server:
            waits.record(sim.now - arrived)  # time spent in the queue
            await sim.timeout(service.expovariate(1.0))

    async with asyncio.TaskGroup() as tg:
        for _ in range(1000):
            await sim.timeout(arrivals.expovariate(0.8))
            tg.create_task(customer())
    return waits


waits = run(main(), seed=1)
p50, p90, p99 = waits.quantile([0.5, 0.9, 0.99])
print("customers:", len(waits))
print("mean wait:", round(waits.mean(), 1))
print("p50, p90, p99:", round(p50, 1), round(p90, 1), round(p99, 1))
counts, edges = waits.histogram([0, 2, 5, 10, 20, 100])
print("histogram:", counts)
```

```text
customers: 1000
mean wait: 3.8
p50, p90, p99: 2.3 9.7 20.1
histogram: (468, 247, 192, 81, 12)
```

The mean wait is the average, but the tail is what customers complain about: the p99 is several
times the mean. The histogram uses explicit bin edges (`[0, 2)`, `[2, 5)`, ... the last bin
includes its right edge); give a number instead (`histogram(10)`) for equal-width bins.
A short run is noisy: the true long-run mean wait is 4, and this run is one sample of it.
The rest of the guide is about how far to trust such a number.

## TimeSeries: statistics of a value over time

Some quantities hold a value for a while, then change: the number of customers in the system,
a tank level, the number of busy machines. Averaging the values at the moments they change
would be wrong (a value that lasted 100 time units counts the same as one that lasted 1), so
`TimeSeries` weights each value by how long it held. `series.record(v)` stamps `v` with
`sim.now`; the value holds until the next record. Record the starting value yourself, and
pass `until=` when you read results after the run has ended. (A primitive's `monitor=` hook
can feed a `TimeSeries` for you, see the [primitives guide](primitives.md#timeseries).)

- `mean(until=None)`: the time-weighted mean, area under the curve divided by the length of
  the span. This one is desyncio's own: `record` adds each finished stretch's area to a running
  total, so `mean` costs the same however many records there are, and it uses no numpy.
- `time_fractions(until=None)`: the share of time spent at each value, a dict that sums to 1.
  Also desyncio's own: it adds up the durations per value with `math.fsum`, no numpy.
- `quantile(p, until=None)`: `numpy.quantile(..., weights=durations, method="inverted_cdf")`.
- `variance(until=None)`: `numpy.average` of the squared deviations, durations as weights.
  This is the population (time-weighted) variance, dividing by the total time; `Tally.variance`
  is the sample variance, dividing by n-1.
- `min()`, `max()`, `last()`, `samples`.

For M/M/1 the share of time with k customers in the system is (1-0.8) * 0.8**k, which is
0.200, 0.160, 0.128 for k = 0, 1, 2. The measured shares below are within about 0.04 of these
here; a longer run gets closer.

```python
import asyncio

from desyncio import Resource, TimeSeries, run, sim


async def main() -> tuple[TimeSeries, float]:
    server = Resource(1)
    in_system = TimeSeries()
    in_system.record(0)  # the starting value
    arrivals, service = sim.random("arrivals"), sim.random("service")

    async def customer() -> None:
        in_system.record(in_system.last() + 1)
        async with server:
            await sim.timeout(service.expovariate(1.0))
        in_system.record(in_system.last() - 1)

    async with asyncio.TaskGroup() as tg:
        for _ in range(2500):
            await sim.timeout(arrivals.expovariate(0.8))
            tg.create_task(customer())
    return in_system, sim.now


in_system, end = run(main(), seed=1)
fractions = in_system.time_fractions(until=end)
print("time with 0, 1, 2 in system:", [round(fractions[k], 2) for k in (0, 1, 2)])
print("mean in system:", round(in_system.mean(until=end), 1))
print("median in system:", in_system.quantile(0.5, until=end))
```

```text
time with 0, 1, 2 in system: [0.16, 0.14, 0.12]
mean in system: 4.4
median in system: 3.0
```

`time_fractions` gives the whole distribution, `mean` its centre, `quantile` any percentile of
it. The long-run mean is 4; the p50 is a whole number because the value is a count.

## Warm-up

A simulation usually starts empty, but a model may start in a state that is not typical, for
example with a backlog. Then the first part of the run is not representative and drags the
averages away from the long-run values. That is the start-up bias. The usual remedy is a
warm-up period: run, but ignore everything before time `t`.

`Tally(warmup=t)` drops an observation recorded before `sim.now == t`.
`TimeSeries(warmup=t)` keeps recording, but the statistics only look at the window that
starts at `t` (with the value that held at that moment); `samples` still has everything.
With `warmup > 0` the `record` methods read `sim.now`, so they need a run.

Below, the system starts with 50 customers already waiting. Two series are fed the same
records: one without warm-up, one with `warmup=500`. The true long-run mean is 4.

```python
import asyncio

from desyncio import Resource, TimeSeries, run, sim


async def main() -> tuple[TimeSeries, TimeSeries, float]:
    server = Resource(1)
    raw, warm = TimeSeries(), TimeSeries(warmup=500)
    arrivals, service = sim.random("arrivals"), sim.random("service")
    count = 0

    def change(delta: int) -> None:
        nonlocal count
        count += delta
        raw.record(count)
        warm.record(count)

    async def customer() -> None:
        change(+1)
        async with server:
            await sim.timeout(service.expovariate(1.0))
        change(-1)

    async with asyncio.TaskGroup() as tg:
        for _ in range(50):  # the backlog, all present at t=0
            tg.create_task(customer())
        for _ in range(3000):
            await sim.timeout(arrivals.expovariate(0.8))
            tg.create_task(customer())
    return raw, warm, sim.now


raw, warm, end = run(main(), seed=1)
print("without warm-up:", round(raw.mean(until=end), 1))
print("with warm-up=500:", round(warm.mean(until=end), 1))
```

```text
without warm-up: 6.3
with warm-up=500: 4.6
```

How long a warm-up should be depends on the model: look at a plot of the series over time, or
try a few values and see where the mean stops changing. Too short leaves some bias, too long
throws away data. An empty start is a start-up bias too: an empty queue is not typical of a
busy server, so the first customers wait less than in the long run. That bias is usually
smaller than a backlog's, but it is there, and the replications example below uses `warmup`
for that reason.

## Replications and confidence intervals

One run is one sample. Two runs with different seeds give different answers, so a single
number has no error bar. The remedy is to repeat the run with independent random streams and
look at the spread of the results.

- `replicate(model, n, seed=0)`: calls `desyncio.run(model(), seed=seed + i)` for i = 0 to
  n-1 and returns the list of results. `model` is a function that takes no arguments and
  returns a new coroutine (a fresh one for each run); it returns whatever you want to
  compare, here the mean wait. The same arguments always give the same list.
- `confidence_interval(values, level=0.95)`: returns `(mean, half_width)`, so the interval is
  `mean +- half_width`. It uses the Student t distribution with n-1 degrees of freedom
  (`scipy.stats.t.interval`, with `scipy.stats.sem` for the standard error), the right choice
  when the number of replications is small. It needs at least 2 values.

Reading the result: "a 95% confidence interval" means that if you repeated the whole
experiment many times, about 95% of the intervals built this way would contain the true mean
of the quantity each replication returns. Be careful what that quantity is. Here it is the mean
wait of the customers who reach the server after time 50 (`warmup=50` is a time: about the
first 40 customers are dropped) in a run of 500 customers that starts empty. The long-run
value is 4, but this short run is slightly biased low (the warm-up removes most of the
empty-start bias, not all of it), so the interval estimates the short-run mean, a little
under 4. More replications make the interval narrower around that short-run mean; longer runs
and a longer warm-up move the short-run mean itself closer to 4.

```python
import asyncio

from desyncio import Resource, Tally, confidence_interval, replicate, sim


async def model() -> float:
    server = Resource(1)
    waits = Tally(warmup=50)
    arrivals, service = sim.random("arrivals"), sim.random("service")

    async def customer() -> None:
        arrived = sim.now
        async with server:
            waits.record(sim.now - arrived)
            await sim.timeout(service.expovariate(1.0))

    async with asyncio.TaskGroup() as tg:
        for _ in range(500):
            await sim.timeout(arrivals.expovariate(0.8))
            tg.create_task(customer())
    return waits.mean()


means = replicate(model, 20, seed=100)
mean, half_width = confidence_interval(means, level=0.95)
print("replications:", len(means))
print(f"mean wait: {mean:.1f} +- {half_width:.1f} (95%)")
print(f"interval: [{mean - half_width:.1f}, {mean + half_width:.1f}]")
```

```text
replications: 20
mean wait: 3.6 +- 0.7 (95%)
interval: [2.9, 4.2]
```

Note `model` is passed without parentheses: `replicate` calls it once per run. Use the
interval to decide whether a difference between two designs is real: if their intervals
do not overlap, it very likely is.

## Input distributions: sim.np_random

`sim.random(name)` gives a `random.Random` with the usual distributions (`expovariate`,
`gauss`, ...). For anything else, `sim.np_random(name)` gives a numpy `Generator`, a named,
seeded stream like `sim.random`: the same run seed and name give the same numbers, and
different names are independent streams. Call numpy's own distributions on it, and scipy's
too: scipy's samplers take it as `random_state=`. desyncio has no distributions of its own.
Like all `sim` calls, it works only inside a run.

- `rng.poisson(lam)`: the number of events in an interval when they occur at random, e.g.
  how many items are in an order. (`rng.exponential`, `rng.gamma`, `rng.triangular`, ... are
  there too.)
- **Empirical distribution**: you have measured data and want to sample "like the data".
  `numpy.quantile(data, rng.random())` turns a uniform number in [0, 1) into a value from the
  data's distribution (it interpolates between observations).
- `scipy.stats.truncnorm.rvs(a, b, loc=, scale=, random_state=rng)`: a normal distribution
  cut off at limits. Note `a` and `b` are the limits measured in standard deviations from
  `loc`, i.e. `(limit - loc) / scale`.

```python
import numpy as np
from scipy import stats

from desyncio import run, sim


async def main() -> None:
    rng = sim.np_random("inputs")

    orders = rng.poisson(3, size=5)
    print("items per order:", [int(x) for x in orders])

    measured = [4.1, 4.8, 5.0, 5.2, 6.0, 7.5, 9.0]  # e.g. observed service times
    sample = [float(np.quantile(measured, rng.random())) for _ in range(3)]
    print("empirical service times:", [round(x, 2) for x in sample])

    # Normal(mean 5, sd 2) but never below 1: a = (1 - 5) / 2, b = infinity.
    times = stats.truncnorm.rvs(-2.0, np.inf, loc=5, scale=2, size=5, random_state=rng)
    print("truncated normal:", [round(float(x), 2) for x in times])
    print("same stream again:", sim.np_random("inputs") is rng)


run(main(), seed=7)
```

```text
items per order: [0, 3, 3, 1, 1]
empirical service times: [4.87, 5.07, 4.87]
truncated normal: [5.85, 4.47, 3.36, 2.01, 2.78]
same stream again: True
```

Because the generator is seeded from the run seed and the name, the output above is the same on
every run, and `replicate` gives each run its own seed, so replications are independent. Give
each source of randomness its own name (`"arrivals"`, `"service"`, ...): then changing one
part of the model does not shift the random numbers of another.
