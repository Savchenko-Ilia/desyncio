# Event API overhead benchmark (EVT-07)

`events_bench.py` times one model, an M/M/1 queue built on `desyncio.Resource`, in several event
configurations, to show what the event API costs. It is **informative, never a gate** (D060): the numbers
depend on the machine and are noisy, and nothing in the test suite or `scripts/gate.sh` runs it.
It uses no libraries beyond desyncio and the standard library. Comparison with SimPy and other libraries
is not done here: that is LOOP-12, deferred by the owner (D018).

## Run it

```
uv run python bench/events_bench.py                          # defaults: 20000 customers, 7 reps (~25 s)
uv run python bench/events_bench.py --customers 5000 --reps 3
UV_PROJECT_ENVIRONMENT=.venv-3.14 uv run --locked --python 3.14 python bench/events_bench.py
```

## The model

Arrivals at rate 0.9, one server with service rate 1.0 (utilisation 0.9), `--customers` customers, seed
12345. Each customer is a task that waits for the server with `async with server:`, records its wait, then
sleeps an exponential service time. Random draws come from two named streams, so every configuration
makes the same draws in the same order. The script asserts that all configurations return the same
(served, total wait, final clock) and exits with `FAIL` otherwise: subscribing must not change the run.
(Checked by making one configuration's handler draw from a model stream: the script then fails.)

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
(`Resource` has a monitor hook, names, the `async with` wrapper, stricter FIFO and release handling). It is therefore an
upper bound on the pure event overhead, and not a measure of it: the honest number for event cost
alone is "vs 1" for the subscriber configurations, and the (small, noisy) gap between 1 and the baseline
for the emit points. Configuration 1 and the baseline are within the noise of each other in the runs below,
in both directions.

## Results

Machine: 4-core x86_64 Linux container, shared (so noisy). Best and median of 9 repetitions, 20000
customers; each run emits 58184 events (`RunStarted`, `RunFinished` and 58182 `ResourceChanged`,
about 2.9 per customer). Percentages are of the best times, rounded; the repetition-to-repetition
spread of one configuration was up to about 10%, so differences under ~10% mean "no visible difference".

Python 3.13.18:

| Configuration | best s | median s | us/customer | events/s (M) | vs 1 | vs semaphore |
|---|---|---|---|---|---|---|
| semaphore baseline | 0.50 | 0.57 | 24.9 | - | -10% | 0 |
| 1. no subscriber | 0.55 | 0.63 | 27.6 | 0.11 | 0 | +11% |
| 2. unrelated kind | 0.52 | 0.60 | 25.8 | 0.11 | -7% | +4% |
| 3. one no-op subscriber | 0.67 | 0.76 | 33.4 | 0.09 | +21% | +34% |
| 4. recording subscriber | 0.70 | 0.76 | 35.0 | 0.08 | +27% | +40% |

Python 3.14 (same machine):

| Configuration | best s | median s | us/customer | events/s (M) | vs 1 | vs semaphore |
|---|---|---|---|---|---|---|
| semaphore baseline | 0.43 | 0.50 | 21.6 | - | -9% | 0 |
| 1. no subscriber | 0.48 | 0.56 | 23.8 | 0.12 | 0 | +10% |
| 2. unrelated kind | 0.44 | 0.52 | 22.1 | 0.13 | -7% | +3% |
| 3. one no-op subscriber | 0.56 | 0.66 | 28.1 | 0.10 | +18% | +30% |
| 4. recording subscriber | 0.60 | 0.70 | 29.8 | 0.10 | +25% | +38% |

An earlier 7-repetition run on 3.13 gave +37% for configuration 3 against configuration 1: the best
time of configuration 1 was 0.49 s there and 0.55 s in the table above, while configuration 3 stayed
near 0.67 s. Expect roughly +20 to +35% depending on the run, and run it on your own machine before
quoting a number. The "events/s"
column is emitted events divided by the whole run's time, so it is the model's throughput, not the
bus's own capacity.

## How this compares with the draft's 20% target

The platform's draft v0.1 wanted at most 20% overhead for one no-op subscriber. It was informative only
(D060). On this M/M/1, one no-op subscriber costs about +18 to +21% over no subscriber in the table runs,
up to about +37% in a noisier run, so it is around the draft's figure, on its edge, not clearly below it. The M/M/1
is close to a worst case for this ratio: the model does almost nothing besides the resource, and
each customer produces about three events. A model with real work between resource operations
pays proportionally less.

## Where the cost comes from

- **No subscriber:** each state change still runs the emit point (D060: one check and one integer add,
  no event object). The EVT-02/EVT-04 notes measured about +0.4 µs per state change on a tight uncontended
  loop: `emit_event` ~170 ns, the running-bus lookup ~110 ns and name registration ~160 ns per acquire.
  At about 2.9 state changes per customer that is about 1 µs of the ~25 µs per customer, i.e. a few
  percent, which is below this benchmark's noise. That is why configuration 1 and the baseline cannot be told apart here.
- **Unrelated subscriber:** the emit point also consults the per-class cache of interested handlers
  (a dict lookup); indistinguishable from configuration 1 here.
- **A subscriber that wants the kind:** per event, a frozen slotted `ResourceChanged` is built through
  the dataclass constructor (keyword arguments, `t`, `seq`) and the handler is called inline. EVT-02
  measured `sim.emit` at 0.55 s vs 1.24 s per 1e6 calls with no subscriber vs one no-op subscriber, i.e.
  about 0.7 µs more per delivered event; times 2.9 events per customer that is the ~6 to 8 µs per
  customer seen above. A recording handler adds the list append and the memory of keeping 58184 event objects.
