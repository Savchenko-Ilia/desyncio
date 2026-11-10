# Event API overhead benchmark (EVT-07)

`events_bench.py` times one model, an M/M/1 queue built on `desyncio.Resource`, in several event
configurations, to show what the event API costs. It is **informative, never a gate** (D060): the numbers
depend on the machine and are noisy, and nothing in the test suite or `scripts/gate.sh` runs it.
It uses no libraries beyond desyncio and the standard library. Comparison with SimPy and other libraries
is not done here: that is LOOP-12, deferred by the owner (D018).

## Run it

```
uv run python bench/events_bench.py                          # defaults: 20000 customers, 7 reps (~30 s)
uv run python bench/events_bench.py --customers 5000 --reps 3
uv run python bench/events_bench.py --customers 500 --reps 1 --check-fail   # must print FAIL, exit 1
UV_PROJECT_ENVIRONMENT=.venv-3.14 uv run --locked --python 3.14 python bench/events_bench.py
```

`--customers` and `--reps` must be at least 1 (argparse error otherwise).

## The model

Arrivals at rate 0.9, one server with service rate 1.0 (utilisation 0.9), `--customers` customers, seed
12345. Each customer is a task that waits for the server with `async with server:`, records its wait, then
sleeps an exponential service time. Random draws come from two named streams, so every configuration
makes the same draws in the same order. The script asserts that all configurations return the same
(served, total wait, final clock) and exits with `FAIL:` (status 1) otherwise: subscribing must not change
the run. `--check-fail` proves that this assertion is reached: it runs configuration 3 with seed 12346
instead of 12345, and the script then prints
`FAIL: 3. one no-op subscriber, all changed the model: (...) != (...)` and exits with status 1.

## Method

- **Warm-up:** every configuration runs once untimed (with `min(customers, 2000)` customers) before any
  timing, so none is timed cold.
- **Interleaving and rotation:** each repetition is one round over all configurations, and the order is
  rotated by one each round, so no configuration always runs first or always right after the
  allocation-heavy recording subscriber. Drift in machine load hits all configurations alike.
- **Garbage collection:** `gc.collect()` runs before each timed run, so garbage left by the previous run
  (for example the 58184 events the recording subscriber kept) is not collected on the next run's clock.
  The collector stays **enabled** during timing: collections triggered by a configuration's own
  allocations (building and keeping event objects) are part of what that configuration costs.
- **Statistics:** the script prints the best and the median of the repetitions. The `vs 1` and `vs sem`
  columns, and every percentage quoted below, compare **medians**; the `best vs 1` column compares best
  times and is printed for reference only. `us/cust` and `Mev/s` also use the median.

## Configurations

| Configuration | What it measures |
|---|---|
| semaphore baseline | The same model with `asyncio.Semaphore(1)` instead of `Resource`. |
| 1. no subscriber | `Resource` with the event API present but nobody listening: the cost of the emit points alone. |
| 2. subscriber, unrelated kind | One subscriber for a custom kind the model never emits: the cost of the emit points when a bus has subscribers but none wants `ResourceChanged`. |
| 3. one no-op subscriber, all | `run(subscribers=[lambda e: None])`: events are built and delivered, the handler does nothing. |
| 4. recording subscriber | `list.append` as the handler: the realistic case (every event is kept). |

**What the semaphore baseline does and does not measure.** The ideal baseline is this model on `integration`
before EVT-04, but that is not available at run time. `asyncio.Semaphore` has no events, so "vs sem"
shows the cost of events *plus* every other difference between `Resource` and a bare semaphore
(`Resource` has a monitor hook, names, the `async with` wrapper, stricter FIFO and release handling).
It is therefore an upper bound on the pure event overhead, not a measure of it. The gap between
configuration 1 and the baseline is likewise an upper bound on the cost of the emit points alone: it
includes all those other differences too, and this benchmark cannot separate them. For event cost alone,
use "vs 1" for the subscriber configurations.

## Results

Machine: 4-core x86_64 Linux container, shared (so noisy). Default settings (20000 customers, 7
repetitions); each run emits 58184 events (`RunStarted`, `RunFinished` and 58182 `ResourceChanged`,
about 2.9 per customer). All three 3.13 runs and the 3.14 run reported identical model results
(served 20000, total wait 175025.598, final clock 22092.521).

**Noise.** Within one run, a configuration's median was 3% to 32% above its best time. Across the three
3.13 runs, configuration 1's median ranged from 0.631 s to 0.726 s (15%). Differences of about ±10–15%
between configurations are therefore within the noise.

Python 3.13.18, three runs, median seconds and median "vs 1":

| Configuration | run A median s | run B median s | run C median s | vs 1 (A / B / C) | vs semaphore (A / B / C) |
|---|---|---|---|---|---|
| semaphore baseline | 0.673 | 0.630 | 0.601 | -7% / -6% / -5% | 0 |
| 1. no subscriber | 0.726 | 0.671 | 0.631 | 0 | +8% / +7% / +5% |
| 2. unrelated kind | 0.655 | 0.712 | 0.653 | -10% / +6% / +4% | -3% / +13% / +9% |
| 3. one no-op subscriber | 0.872 | 0.819 | 0.799 | +20% / +22% / +27% | +30% / +30% / +33% |
| 4. recording subscriber | 0.855 | 0.846 | 0.843 | +18% / +26% / +33% | +27% / +34% / +40% |

For reference, the best-of comparison of configuration 3 against 1 gave +21%, +17% and +12% in the same
three runs: best-of times are noisier here and are not quoted elsewhere.

Python 3.14.6, one run (same machine). Read it with care: here the baseline was 20% faster than
configuration 1, against 5% to 7% in the 3.13 runs, and the medians were 4% to 28% above the best times:

| Configuration | best s | median s | us/customer | vs 1 | vs semaphore |
|---|---|---|---|---|---|
| semaphore baseline | 0.484 | 0.502 | 25.1 | -20% | 0 |
| 1. no subscriber | 0.551 | 0.628 | 31.4 | 0 | +25% |
| 2. unrelated kind | 0.470 | 0.602 | 30.1 | -4% | +20% |
| 3. one no-op subscriber | 0.581 | 0.671 | 33.6 | +7% | +34% |
| 4. recording subscriber | 0.568 | 0.670 | 33.5 | +7% | +34% |

One 3.14 run is not enough to say 3.14 is cheaper per event; run it several times before quoting it.

Summary of the 3.13 runs (medians):

- **No subscriber vs semaphore baseline:** configuration 1 was +5% to +8% slower than the baseline. That is
  within the noise, and it is an upper bound on the emit points' cost (see above), not a measure of it.
- **Unrelated subscriber vs no subscriber:** -10% to +6%, i.e. within the noise; no visible cost.
- **One no-op subscriber vs no subscriber:** +20% to +27%.
- **Recording subscriber vs no subscriber:** +18% to +33%.

The "events/s" column the script prints is emitted events divided by the whole run's median time, so it
is the model's throughput, not the bus's own capacity.

## How this compares with the draft's 20% target

The platform's draft v0.1 wanted at most 20% overhead for one no-op subscriber. It was informative only
(D060). On this M/M/1, one no-op subscriber cost +20% to +27% over no subscriber (medians, three 3.13 runs),
so it is at or above the draft's figure. The M/M/1
is close to a worst case for this ratio: the model does almost nothing besides the resource, and
each customer produces about three events. A model with real work between resource operations
pays proportionally less.

## Where the cost comes from

- **No subscriber:** each state change still runs the emit point (D060: one check and one integer add,
  no event object). The EVT-02/EVT-04 notes measured about +0.4 µs per state change on a tight uncontended
  loop: `emit_event` ~170 ns, the running-bus lookup ~110 ns and name registration ~160 ns per acquire.
  At about 2.9 state changes per customer that is about 1 µs of the ~30 µs per customer, i.e. a few
  percent. The measured +5% to +8% over the semaphore baseline is consistent with that, but it also
  includes the other `Resource`-vs-`Semaphore` differences and is within this benchmark's noise.
- **Unrelated subscriber:** the emit point also consults the per-class cache of interested handlers
  (a dict lookup); measured -10% to +6% against configuration 1, i.e. not visible above the noise.
- **A subscriber that wants the kind:** per event, a frozen slotted `ResourceChanged` is built through
  the dataclass constructor (keyword arguments, `t`, `seq`) and the handler is called inline. EVT-02
  measured `sim.emit` at 0.55 s vs 1.24 s per 1e6 calls with no subscriber vs one no-op subscriber, i.e.
  about 0.7 µs more per delivered event; times 2.9 events per customer that is about 2 µs per customer.
  The gap measured here (configuration 3 minus 1, median µs per customer) is about 7 to 8 µs, larger
  than that estimate; this benchmark does not show where the rest goes. A recording handler adds the
  list append and the memory of keeping 58184 event objects.
