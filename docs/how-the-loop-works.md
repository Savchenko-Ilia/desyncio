# How the loop works

This explains how desyncio turns a stock asyncio event loop into a virtual-time one,
and which asyncio internals that depends on. It is written for someone learning
asyncio internals. Every claim is checked against the code on `integration` and
against CPython 3.13.

CPython sources referenced (Python 3.13 tag; look up functions by name, since line
numbers drift):

- [`Lib/asyncio/base_events.py`](https://github.com/python/cpython/blob/3.13/Lib/asyncio/base_events.py): `BaseEventLoop._run_once`, `call_later`, `call_at`
- [`Lib/asyncio/events.py`](https://github.com/python/cpython/blob/3.13/Lib/asyncio/events.py): `TimerHandle`
- [`Lib/asyncio/tasks.py`](https://github.com/python/cpython/blob/3.13/Lib/asyncio/tasks.py): `sleep`
- [`Lib/asyncio/futures.py`](https://github.com/python/cpython/blob/3.13/Lib/asyncio/futures.py): `_set_result_unless_cancelled`

desyncio sources: [`clock.py`](../src/desyncio/clock.py), [`loop.py`](../src/desyncio/loop.py),
[`scheduling.py`](../src/desyncio/scheduling.py), [`guards.py`](../src/desyncio/guards.py),
and the runner behind `desyncio.run(...)`.

## 1. The idea in one paragraph

An asyncio loop does not know about wall-clock time except through two things: it
asks `self.time()` what time it is, and it waits in exactly one place, the selector's
`select(timeout)`. desyncio replaces `time()` with a virtual clock and replaces the
selector with one whose `select` moves the clock forward instead of waiting. Everything
else (tasks, futures, `sleep`, `Queue`, `TaskGroup`) is untouched stock asyncio.

## 2. `_run_once`: one iteration of the loop

`run_forever` just calls
[`BaseEventLoop._run_once`](https://github.com/python/cpython/blob/3.13/Lib/asyncio/base_events.py)
in a loop until `stop()` is called. One call is one *iteration*. It has five steps:

```
 _run_once()
 +------------------------------------------------------------------+
 | 1. Housekeeping   drop cancelled timers from the head of         |
 |                   _scheduled (or rebuild the heap if >50% of     |
 |                   100+ timers are cancelled)                     |
 | 2. Timeout        0            if _ready is non-empty / stopping |
 |                   head._when - time()   if there are timers      |
 |                   None         otherwise (block until I/O)       |
 | 3. Select         event_list = selector.select(timeout)          |
 |                   _process_events(event_list)  -> I/O callbacks  |
 |                   go to _ready                                   |
 | 4. Collect timers end = time() + _clock_resolution               |
 |                   pop every timer with _when < end from          |
 |                   _scheduled, append it to _ready                |
 | 5. Run            ntodo = len(_ready); popleft and run exactly   |
 |                   that many handles                              |
 +------------------------------------------------------------------+
```

Points worth knowing:

- **Step 5 runs only `ntodo` handles**, the number that were in `_ready` when step 5
  began. Anything a callback adds with `call_soon` waits for the *next* iteration
  (after another step 3 poll). This is why a task woken by a future runs one
  iteration after the future was resolved (see the worked example).
- **`time()` is read in step 2** (to compute the timeout) **and in step 4** (to decide
  what is due). Step 4 reads it once, so the set of due timers is a snapshot; the
  callbacks in step 5 do not change that set.
- **Step 3 is the only place a stock loop blocks.** With `timeout=None` or a positive
  timeout, `epoll_wait` (or the platform equivalent) sleeps. Everything desyncio does
  hangs off that fact.
- Timers and ready callbacks are both `Handle` objects. A timer is a `TimerHandle`,
  which is a `Handle` plus a `_when`. Step 4 is where a timer stops being a timer and
  becomes an ordinary ready callback.

## 3. The timer heap

`loop._scheduled` is a plain Python `list` kept in heap order by the `heapq` module:
`heappush` in `call_at`, `heappop` in `_run_once`. A binary min-heap guarantees only
that `_scheduled[0]` is *a* smallest element. That makes "what is the next timer?"
O(1) and "add a timer" O(log n), which is why the loop uses it.

`heapq` orders elements using `<` and nothing else. So the order of timers is whatever
`TimerHandle.__lt__` says.

### Why ties need explicit ordering

In CPython, [`TimerHandle.__lt__`](https://github.com/python/cpython/blob/3.13/Lib/asyncio/events.py)
is:

```python
def __lt__(self, other):
    if isinstance(other, TimerHandle):
        return self._when < other._when
    return NotImplemented
```

Two timers with the same `_when` are not "less than" each other, so the heap treats them
as interchangeable and pops them in whatever order its internal swaps left them.
`heapq` is **not stable**: it does not preserve insertion order for equal items.
`call_later`'s own docstring says so: "If two callbacks are scheduled for exactly the
same time, it is undefined which will be called first."

Real output on 3.13. Five stock `TimerHandle`s `b, c, d, e, f`, all at `when=1`, are
pushed with `heapq.heappush` in that order, then `a` at `when=2`; then the heap is
emptied with `heapq.heappop`:

```
pop order: b, d, c, e, f, a        # not b, c, d, e, f
```

The tied order also depends on what else is in the heap: pushing `a` *first* and then
`b`…`f` gives `b, c, f, e, d, a`.

A simulation where same-time events run in an arbitrary order cannot give the same trace
twice, and AGENTS.md makes determinism mandatory.

### The `(when, priority, sequence)` fix

[`scheduling.py`](../src/desyncio/scheduling.py) gives each timer a **unique** sort key:

| part       | meaning                                                      |
|------------|--------------------------------------------------------------|
| `when`     | simulation time; earlier first                               |
| `priority` | int, default `0` (`DEFAULT_PRIORITY`); lower first at equal time |
| `sequence` | per-loop counter stamped when the timer is scheduled; lower first |

`PriorityTimerHandle` subclasses `TimerHandle` and overrides `__lt__` (and the other
comparisons) to compare `sort_key()`. Because `sequence` never repeats, no two keys are
equal, so there is exactly one valid pop order and the heap's arbitrary choices stop
mattering. Equal `(when, priority)` pairs therefore pop FIFO in scheduling order.
CPython's heap code is not modified; it simply calls our `__lt__`.

`VirtualTimeLoop.call_at` builds these handles with priority 0, and
`call_at_priority(when, priority, cb, ...)` lets you choose. Nothing else needs to change,
because stock `call_later` calls `self.call_at`, `sleep` calls `call_later`, and
`asyncio.timeout` calls `call_at`, so every stock timer goes through our override.

Two caveats:

- Priority and sequence only order **timers**. A `call_soon` made from a timer callback
  still runs after the other timers that were due at that instant (they are already in
  `_ready`). That is stock behaviour.
- The sequence counter is per loop (`SequenceCounter`), so two runs of the same model
  stamp the same numbers.

### The `_clock_resolution` window

Step 4 treats a timer as due if `_when < time() + _clock_resolution`. A real loop sets
`_clock_resolution` to the monotonic clock's resolution (about 1e-9 s). All due timers
move to `_ready` in one go, and `time()` stays frozen while they run. So with a 1e-9
window, a timer at `1.0 + 5e-10` would run in the same iteration as a timer at `1.0`,
while `loop.time()` still says `1.0`. That timer would see a time earlier than the one it
asked for.

`VirtualTimeLoop._clock_resolution` is a property that returns
`math.nextafter(now, inf) - now`: the gap to the next representable float. Then
`now + resolution` is exactly the next float, and only timers at exactly `now` (or
earlier) are due. It cannot be 0: the loop's test is `_when >= end_time` means "stop",
so with 0 a timer at exactly `now` would never be due and the loop would spin. The
property depends on `now` and is recomputed on each read. The setter discards the value
`BaseEventLoop.__init__` assigns.

## 4. Jumping the clock

A virtual-time loop has two jobs: answer `time()` from a clock, and never wait.

- [`VirtualClock`](../src/desyncio/clock.py) holds a float. `now` reads it, `advance_to(t)`
  moves it forward. It refuses to go backwards, and `t == now` is a no-op. It has no
  asyncio in it.
- `VirtualTimeLoop.time()` returns `self.clock.now`.
- After `super().__init__()` has built the real selector and registered the loop's
  self-pipe on it, `VirtualTimeLoop` wraps it: `self._selector = _JumpingSelector(...)`.

### `_JumpingSelector.select`

Because step 3 is the one place a loop waits, it is the one place to intervene.
`select(timeout)` does this:

```
events = real_selector.select(0)        # poll only, never block
if events or timeout == 0:  return events
if _scheduled:                          # idle, but a timer exists
    clock.advance_to(_scheduled[0]._when)
    return events                       # empty list
raise SimulationDeadlock
```

- `timeout == 0` means `_run_once` has ready work (or is stopping): just poll.
- Otherwise the loop *would* block. Instead the clock jumps to the earliest timer. Step 4
  then finds that timer due (`_when < now + resolution`) and the loop carries on, having
  skipped the wait.
- Why `_scheduled[0]` is trustworthy: step 1 already popped cancelled timers off the head,
  so a cancelled timer can never move the clock.
- An **overdue** timer (`_when < now`) gives a timeout of 0 in step 2, so `select` only
  polls; the timer fires without moving the clock.

### The deadlock rule (D006) and threads

If nothing is ready, no timer exists and no fd is readable, then nothing can ever
happen, as far as the loop can tell. A stock loop would block in `epoll_wait` forever. A
virtual loop must not block, because blocking means waiting on the real world, which is
the opposite of a simulation. So `select` raises `SimulationDeadlock`. A deadlock in a
model is a bug, and failing loudly is better than hanging.

Consequence for threads (decision D006): the loop does not wait for them. A thread
resolves work by calling `call_soon_threadsafe`, which writes a byte to the self-pipe,
and the loop would have to *wait* for that byte. The poll is `select(0)`, which does not
wait. So, for a `threading.Thread` you start yourself that later calls
`loop.call_soon_threadsafe(...)`:

- no timers pending: the loop raises `SimulationDeadlock` immediately, before the thread
  has a chance to report;
- a timer pending: virtual time jumps to it, past whenever the thread would have
  finished.

Supporting real threads would need a real-time wait, which breaks determinism. The
loop's own executor entry points never get that far: `loop.run_in_executor` (and so
`asyncio.to_thread`, which is a thin wrapper over it) and `loop.set_default_executor`
raise `desyncio.guards.RealIOError` at the call, before any thread starts. One known gap:
a `concurrent.futures` executor you create and submit to directly, then await with
`asyncio.wrap_future`, bypasses the loop's methods, so it is not refused and behaves like
the user thread above.

- [`guards.py`](../src/desyncio/guards.py) lists everything the loop refuses with
  `RealIOError` (sockets, subprocesses, executors, DNS, signals) and explains why, in line
  with AGENTS.md principle 5 (no real I/O inside a simulation).

## 5. `asyncio.sleep(3600)` becomes a timer

Here is the full path for a task that does `await asyncio.sleep(3600)` at virtual time
`t=0`:

```
task step runs coroutine ──► asyncio.sleep(3600)                      tasks.py
                               future = loop.create_future()
                               loop.call_later(3600, _set_result_unless_cancelled, future, None)
                                 └► self.call_at(self.time() + 3600, ...)       base_events.py
                                      └► VirtualTimeLoop.call_at               loop.py (override)
                                           └► _schedule: PriorityTimerHandle(when=3600,
                                                priority=0, sequence=n)
                                              heapq.heappush(self._scheduled, timer)
                               await future   ──► task suspends, adds its wakeup as a
                                                  done-callback of `future`

_run_once iterations ... _ready is empty, so:
   step 2  timeout = _scheduled[0]._when - time() = 3600
   step 3  _JumpingSelector.select(3600): poll finds nothing, so
           clock.advance_to(3600); return []
   step 4  timer is due: pop it from the heap, append to _ready
   step 5  run timer: _set_result_unless_cancelled(future, None)
             └► future.set_result(None) ──► call_soon(task wakeup)   (future's callbacks)

_run_once next iteration:
   step 5  run task wakeup ──► Task.__step ──► coroutine resumes after `await sleep(...)`
```

Notes, each checked in the 3.13 source:

- [`sleep`](https://github.com/python/cpython/blob/3.13/Lib/asyncio/tasks.py) with
  `delay <= 0` does not make a timer at all; it yields once (`__sleep0`), so
  `sleep(0)` takes one trip round the loop without touching the heap.
- `call_later` computes `self.time() + delay`, so the virtual clock is what makes the
  deadline `3600`.
- Resolving the future does not resume the task directly. `Future.set_result` schedules
  the future's callbacks with `call_soon`, and the task wakeup is one of them. Per
  step 5's `ntodo` rule, it runs in the next iteration. The worked example below shows it.
- `sleep`'s `finally: h.cancel()` runs after the wait. The timer has already fired and left
  the heap, so this is harmless; it matters when the sleeping task is cancelled early, as
  it removes the pending timer's effect.
- The CPython task and future classes are normally the C accelerated ones (`_asyncio`),
  but behave as the pure-Python code in `tasks.py` and `futures.py` describe.

## 6. How `until` stops the run

The loop only advances the clock inside `_JumpingSelector.select`, and only when it is
idle. `desyncio.run(main(), until=T)` exploits this without changing the loop.

- The runner builds a `_BoundedClock(VirtualClock)` with a `limit` and gives it to
  `VirtualTimeLoop(clock=clock)`.
- `_BoundedClock.advance_to(t)`: if `t > limit`, it sets the clock to exactly `limit` and
  raises a private `_LimitReached`. Otherwise it advances normally.
- `_LimitReached` unwinds through `select` → `_run_once` → `run_forever` →
  `Runner.run`, and the runner catches it and returns `None`.
- Because the clock moves only when idle, everything ready, and every timer with
  `t <= until`, has already run by the time the loop asks to jump past `until`. A timer
  at exactly `until` runs: advancing to `t == limit` is allowed.

### Cleanup freezing

Leftover tasks are cancelled when the `Runner` closes, like `asyncio.run`. A cancellation
handler could `await asyncio.sleep(5)` and push the clock past `until`. To prevent that,
the runner sets `clock.limit = clock.now` *before* `runner.close()`. Cleanup code then
runs, but only code that needs no virtual time to pass (`sleep(0)`, releasing locks).
If a handler waits on a later timer, `advance_to` raises `_LimitReached` inside close,
and the runner converts it to `SimulationCleanupError`. The loop is still closed.

The same freeze applies when `main` finishes normally: the limit becomes the finish
time.

## 7. Private attributes desyncio depends on, and PSF-adapted code

desyncio overrides and wraps rather than copying `_run_once`, but it still reads private
state. If a CPython release changes any of these, the tests in `tests/` fail.

| attribute | where used | why |
|-----------|-----------|-----|
| `loop._scheduled` | `_JumpingSelector.select`, `VirtualTimeLoop._schedule` | peek at `[0]` to find the next timer; `heappush` new timers |
| `TimerHandle._when` | `_JumpingSelector.select`, `PriorityTimerHandle.sort_key` | the timer's due time |
| `loop._selector` | `VirtualTimeLoop.__init__` | swap in `_JumpingSelector` |
| `loop._clock_resolution` | property override | width of step 4's due window |
| `loop._check_closed`, `_debug`, `_check_thread`, `_check_callback` | `_schedule` | same validation as stock `call_at` |
| `Handle._source_traceback` | `PriorityTimerHandle.drop_creation_frame` | debug-mode creation stack trim |
| `TimerHandle._scheduled` | `mark_scheduled` | lets `cancel()` count cancelled timers in the heap |
| `BaseSelectorEventLoop._add_reader`, `_remove_reader` | not called; `RealIOGuardMixin` relies on them | the loop registers its own self-pipe through these private methods, so refusing the public `add_reader` does not break the loop |

### The PSF-adapted part (D009)

Stock `call_at` hardcodes `events.TimerHandle(...)`, so there is no hook to make it build
a different handle class. `VirtualTimeLoop._schedule` therefore re-creates the body of
`BaseEventLoop.call_at` with three changes: it builds a `PriorityTimerHandle`, it assigns
the next sequence number, and it takes the `api_name` for `_check_callback`. The
`drop_creation_frame` helper likewise comes from the traceback-trimming lines in
`call_at` and `TimerHandle.__init__`.

That is code adapted from CPython, which is under the PSF License. Per AGENTS.md §3
it is marked with comments in the source and the license text is in
[`LICENSES/PSF.txt`](../LICENSES/PSF.txt). Everything else only subclasses and overrides.
`test_cpython_adapted_code_carries_psf_notice` guards this.

## 8. Worked example

Three tasks, run on `VirtualTimeLoop` via `asyncio.Runner(loop_factory=...)`:

- `A` sleeps 2, then 2 (wakes at 2 and 4)
- `B` sleeps 1, then 3 (wakes at 1 and 4)
- `C` sleeps 4 (wakes at 4)

and `main` spawns them and does `await asyncio.gather(A, B, C)`. The trace comes from
a subclass of `VirtualTimeLoop` whose `_run_once` records `_ready`, `_scheduled` and the
clock around each call. Nothing in desyncio was modified.

Timers are written `timer#sequence@when`. The sequence numbers are `A`=0, `B`=1, `C`=2
for the first sleeps (the order the tasks first ran), then 3 for `B`'s second sleep
(scheduled at t=1) and 4 for `A`'s second (scheduled at t=2).

"heap" is `_scheduled` at the start of the iteration; "ready" is `_ready` at the start.
"before" and "after" are the clock before and after step 3.

| iter | clock before | ready (at start) | heap (at start) | clock after | ran in step 5 |
|-----:|:---:|---|---|:---:|---|
| 1 | 0 | step(main) | (empty) | 0 | step(main): creates A, B, C, awaits gather |
| 2 | 0 | step(A), step(B), step(C) | (empty) | 0 | A, B, C each call `sleep`, pushing timers #0@2, #1@1, #2@4 |
| 3 | 0 | (empty) | #1@1, #0@2, #2@4 | **1** | timer#1@1 (resolves B's future) |
| 4 | 1 | step(B) | #0@2, #2@4 | 1 | B resumes, sleeps 3: pushes #3@4 |
| 5 | 1 | (empty) | #0@2, #2@4, #3@4 | **2** | timer#0@2 (resolves A's future) |
| 6 | 2 | step(A) | #2@4, #3@4 | 2 | A resumes, sleeps 2: pushes #4@4 |
| 7 | 2 | (empty) | #2@4, #3@4, #4@4 | **4** | timer#2@4, timer#3@4, timer#4@4 (C, B, A futures) |
| 8 | 4 | step(C), step(B), step(A) | (empty) | 4 | each task resumes and finishes |

What to notice:

- **Iterations 3, 5, 7 are the jumps.** `_ready` is empty, so `select` is called with a
  non-zero timeout and `_JumpingSelector` advances the clock (0 → 1 → 2 → 4). A real
  loop would have waited 4 seconds in total; this ran instantly.
- **Each timer takes two iterations to resume its task.** The timer's callback runs in
  one iteration (3), and the task's `step` runs in the next (4) because `set_result`
  uses `call_soon`. The clock does not move between the two: iteration 4's "before" is 1.
- **The three-way tie at t=4** (iteration 7) fires in sequence order: C (#2), B (#3),
  A (#4). That is the order the timers were *scheduled*, not the order the tasks were
  created (A first). Iteration 8 resumes them in that same order, and the program prints
  `C done`, `B done`, `A done`. With stock `TimerHandle` ordering that tie would be
  decided by the heap's internal layout.
- The heap lists are sorted for display. The real list is in heap order, where only
  index 0 is guaranteed to be the smallest.

The iterations after 8 (not shown) are teardown: `gather`'s done-callbacks, `main`
finishing, and `Runner.close()` running its shutdown coroutines. The clock stays at 4.

To reproduce it, subclass `VirtualTimeLoop` and wrap `_run_once`:

```python
import asyncio
from desyncio.loop import VirtualTimeLoop

class Traced(VirtualTimeLoop):
    def _run_once(self):
        ready = list(self._ready)
        heap = sorted(self._scheduled)       # PriorityTimerHandle sorts by its key
        before = self.time()
        super()._run_once()
        print(before, len(ready), [(h._when, h.sequence) for h in heap], self.time())

with asyncio.Runner(loop_factory=Traced) as runner:
    runner.run(main())
```

(The full script used to produce the table also patches `Handle._run` to record what ran;
it is instrumentation only and lives outside the repository.)

## 9. Where to read next

- [`tests/test_loop.py`](../tests/test_loop.py): the jumping, deadlock and cancelled-timer
  behaviour above, as executable specs.
- [`tests/test_ordering.py`](../tests/test_ordering.py): tie-breaking and the resolution window.
- [`tests/test_run.py`](../tests/test_run.py): `until`, cleanup freezing and seeds.
- [`tests/test_guards.py`](../tests/test_guards.py): which loop methods raise
  `RealIOError`, and that every `AbstractEventLoop` method is classified.
- [`docs/decisions.md`](decisions.md): D006 (deadlock and threads), D008 (`until` and
  priority semantics), D009 (PSF-adapted code).
- [`docs/implementation-log.md`](implementation-log.md): how each piece was built.
