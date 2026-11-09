# Primitives guide

desyncio's primitives are the classic discrete-event building blocks (SimPy's `Resource`,
`Container`, `Store`, interrupts) written as ordinary `async`/`await` code on a real asyncio
event loop whose clock is virtual. This guide covers every public primitive with a runnable
example, says exactly what asyncio does for it and what desyncio adds, and states the ordering
and cancellation guarantees.

Every `python` block below is executed by `tests/test_docs_examples.py`, and the `text` block
under it is compared with what the example really prints, so a claim that stops being true
fails the test suite. All examples use top-level imports (`from desyncio import ...`).

Contents:

1. [Ground rules](#ground-rules)
2. [Resource](#resource) (bank tellers)
3. [PriorityResource](#priorityresource)
4. [PreemptiveResource and Preempted](#preemptiveresource-and-preempted) (machine shop)
5. [Container](#container) (fuel tank)
6. [Store, PriorityStore, FilterStore](#store-prioritystore-filterstore) (producer/consumer)
7. [Interrupts](#interrupts) (machine breakdown)
8. [Monitoring: the monitor hook and TimeSeries](#monitoring-the-monitor-hook-and-timeseries)
9. [Random streams: sim.random](#random-streams-simrandom) (car wash)

## Ground rules

- **Time is a unitless float**, "seconds by convention". `await sim.timeout(5)` waits 5 units
  of virtual time (it is `asyncio.sleep` underneath, so the clock jumps straight to the next
  event and the wait costs no real time). `sim.now` is the current virtual time.
- **A process is a coroutine.** `sim.spawn(coro)` starts one (a task, like
  `asyncio.create_task`); `asyncio.gather` and `asyncio.TaskGroup` work as usual.
- **Create primitives inside the run**, i.e. inside the coroutine you pass to
  `desyncio.run(...)`. They are built on asyncio objects (`Semaphore`, `Condition`, `Queue`,
  futures) that bind to the event loop that first uses them, and each `run` makes a fresh loop.
- **Primitives are used with `async with`** (D019): `async with resource:` acquires, and the
  exit releases, on normal exit, exception and cancellation alike.
- **Same model + same seed = same trace.** Wherever several tasks are ready at the same virtual
  instant, the order is fixed (timers fire in the order they were scheduled, tasks resume in the
  order they became ready), so the output of every example below is stable.
- **Cancellation is how asyncio stops a task.** `task.cancel()` makes the task's current
  `await` raise `CancelledError`; `asyncio.timeout()`, `TaskGroup` and desyncio's interrupts
  are all built on it. A waiting primitive must clean up when that happens. In this guide
  "cancellation guarantees" means what state a primitive is left in when a waiter is cancelled.

## Resource

`Resource(capacity=1, *, monitor=None)`: `capacity` interchangeable units, handed out
first-come first-served. Read-only `capacity`, `count` (units in use) and `queue_length`
(requests waiting).

### Example: bank tellers

```python
import asyncio

from desyncio import Resource, run, sim


async def customer(name: str, bank: Resource, arrives: float, service: float) -> None:
    await sim.timeout(arrives)
    async with bank:  # waits, in virtual time, for a free teller
        print(f"t={sim.now:g} {name} starts (waited {sim.now - arrives:g})")
        await sim.timeout(service)
    # the teller is released here, whatever happened in the body


async def main() -> None:
    bank = Resource(capacity=2)
    await asyncio.gather(
        customer("ann", bank, 0, 5),
        customer("bob", bank, 1, 5),
        customer("cy", bank, 2, 3),
        customer("di", bank, 3, 1),
    )
    print(f"done at t={sim.now:g}; in use={bank.count}, waiting={bank.queue_length}")


run(main())
```

```text
t=0 ann starts (waited 0)
t=1 bob starts (waited 0)
t=5 cy starts (waited 3)
t=6 di starts (waited 3)
done at t=8; in use=0, waiting=0
```

### What is asyncio, what is ours

| Piece | Who |
| --- | --- |
| The FIFO queue of waiters | asyncio: a stock `asyncio.Semaphore(capacity)` (a deque of futures) |
| "No barging": a newcomer arriving at the instant of a release queues behind existing waiters | asyncio: `Semaphore.acquire` refuses its fast path while anyone is queued |
| Cancelling a waiter removes it from the queue; a waiter cancelled after being granted but before resuming gives the unit back and wakes the next | asyncio: `Semaphore`'s own cleanup |
| Virtual time | nothing special: the semaphore only uses futures, which the virtual-time loop drives |
| `capacity`/`count`/`queue_length`, argument validation, the `monitor` hook and its error contract | desyncio (our own counters; we never read the semaphore's private attributes) |

### Guarantees

- **Order:** strictly FIFO by the moment the `async with` is entered. Requests made in the same
  virtual instant are served in the order their tasks run.
- **Release is synchronous** (D030): `__aexit__` has no `await`, so a cancel or interrupt
  arriving at the holder's exit cannot skip it, and an exception from the body is never
  replaced by a `CancelledError`.
- **Cancellation of a waiter** (`task.cancel()`, `asyncio.timeout()`, a `TaskGroup`
  shutting down, `sim.interrupt`) removes it from the queue, and `queue_length` drops at once.
- `queue_length` counts a request that was handed a unit but whose task has not resumed yet.
  `count` includes it only once the task resumes.

```python
import asyncio

from desyncio import Resource, run, sim


async def impatient(bank: Resource) -> None:
    try:
        async with asyncio.timeout(2):  # give up after 2 units of virtual time
            async with bank:
                print("never printed")
    except TimeoutError:
        print(f"t={sim.now:g} gave up; waiting={bank.queue_length}")


async def main() -> None:
    bank = Resource(capacity=1)
    async with bank:  # the only teller is busy for 10
        task = sim.spawn(impatient(bank))
        await sim.timeout(1)
        print(f"t={sim.now:g} waiting={bank.queue_length}")
        await task
        await sim.timeout(10)
    print(f"t={sim.now:g} in use={bank.count}")


run(main())
```

```text
t=1 waiting=1
t=2 gave up; waiting=0
t=12 in use=0
```

The "granted but not resumed" gap, made visible. When `ann` releases at t=1, `bob` is handed the
unit at once, but his task only runs on a later loop iteration (asyncio resumes a task through
`call_soon`, never inside the releaser's call). In between, `queue_length` is still 1 and `count`
is 0:

```python
import asyncio

from desyncio import Resource, run, sim


async def holder(res: Resource) -> None:
    async with res:
        await sim.timeout(1)


async def waiter(res: Resource) -> None:
    async with res:
        print(f"bob resumed: count={res.count}, waiting={res.queue_length}")


async def main() -> None:
    res = Resource(capacity=1)
    tasks = [sim.spawn(holder(res)), sim.spawn(waiter(res))]
    await sim.timeout(0)  # let both start
    await sim.timeout(1)  # our timer fires after the holder's, in the same instant
    print(f"in the gap: count={res.count}, waiting={res.queue_length}")
    await asyncio.gather(*tasks)


run(main())
```

```text
in the gap: count=0, waiting=1
bob resumed: count=1, waiting=0
```

An exception in the body propagates unchanged and the unit is still freed:

```python
from desyncio import Resource, run


async def main() -> None:
    res = Resource(capacity=1)
    try:
        async with res:
            raise ValueError("boom")
    except ValueError as exc:
        print(f"caught {exc}; in use={res.count}")


run(main())
```

```text
caught boom; in use=0
```

## PriorityResource

`PriorityResource(capacity=1, *, monitor=None)`: like `Resource`, but waiters are served in
`(priority, request order)` order. A lower number is served first; equal priorities are served
in the order they began waiting. Use `async with res.request(priority=p):`; a plain
`async with res:` is priority 0. `priority` is an `int` or `float` (not `bool`, not NaN).

```python
import asyncio

from desyncio import PriorityResource, run, sim


async def job(res: PriorityResource, name: str, priority: float) -> None:
    async with res.request(priority=priority):
        print(f"t={sim.now:g} {name} (priority {priority:g}) runs")
        await sim.timeout(1)


async def main() -> None:
    res = PriorityResource(capacity=1)
    async with asyncio.TaskGroup() as tg:
        tg.create_task(job(res, "first-come", 5))  # takes the free unit at once
        tg.create_task(job(res, "low", 5))
        tg.create_task(job(res, "urgent", 1))
        tg.create_task(job(res, "also-low", 5))


run(main())
```

```text
t=0 first-come (priority 5) runs
t=1 urgent (priority 1) runs
t=2 low (priority 5) runs
t=3 also-low (priority 5) runs
```

Bad priorities are rejected, and a request object can be used only once:

```python
from desyncio import PriorityResource, run


async def main() -> None:
    res = PriorityResource()
    for bad in (True, float("nan"), "high"):
        try:
            res.request(priority=bad)  # type: ignore[arg-type]
        except (TypeError, ValueError) as exc:
            print(type(exc).__name__)
    request = res.request(priority=1)
    async with request:
        pass
    try:
        async with request:
            pass
    except RuntimeError as exc:
        print(exc)


run(main())
```

```text
TypeError
ValueError
TypeError
a PriorityRequest can be used only once
```

### What is asyncio, what is ours

| Piece | Who |
| --- | --- |
| Waiting itself: each waiter awaits its own `loop.create_future()`; granting is `fut.set_result(None)` | asyncio futures |
| Cancellation (`task.cancel()`, `asyncio.timeout()`, `TaskGroup`, `sim.interrupt`) arriving as `CancelledError` at `await fut` | asyncio |
| The shape of `Semaphore`: synchronous release that hands the unit straight to the next waiter; a waiter cancelled after being granted passes the unit on | copied from `asyncio.Semaphore`'s design, re-implemented |
| The ordering (a list kept sorted with `bisect.insort`), the grant rule, counters, validation, the monitor | desyncio |

asyncio has no priority semaphore, so this is the one place the waiting logic is ours. It could
not be built on `asyncio.Condition`: `notify_all()` needs the Condition's lock, so release had to
`await`, and a cancel at that await leaked the unit (D030).

### Guarantees

- **Order:** `(priority, request order)`. "Request order" is the moment the `async with` is
  entered, not when `request()` was called.
- **No barging:** a newcomer takes a unit at once only if its key is better than every live
  waiter's. Otherwise it queues, even if a unit is free at that very instant. A newcomer with a
  strictly better priority legitimately goes first.
- **Release is synchronous** (D030): the decrement and the hand-over to the best waiter happen in
  one step. `PriorityRequest` is single-use: entering it twice raises `RuntimeError`.
- **Cancellation:** a pending waiter leaves the order at once. A waiter that was granted but
  cancelled before it resumed hands its unit to the next waiter.

## PreemptiveResource and Preempted

`PreemptiveResource(capacity=1, *, monitor=None)` is a `PriorityResource` whose holders can be
thrown off (SimPy semantics, D032). `request(priority=0, preempt=True)`:

- Every holder runs its body inside a `sim.interruptible()` block, so it can receive an
  `Interrupt`. Catch it **outside** the `async with`.
- `preempt=True` (the default) means *this request may preempt*. It does so only if no unit is
  free, it would be at the head of the queue, and its priority is **strictly** lower than the
  worst holder's. Equal priority never preempts. `preempt=False` waits like any
  `PriorityResource` request.
- The victim is the holder with the largest `(priority, request order)`: among the worst
  priority, the most recent one.
- `exc.cause` is a `Preempted(by, usage_since, resource)`: the preempting task, the virtual time
  the victim got the unit it just lost, and the resource.

### Example: machine shop

```python
import asyncio

from desyncio import Interrupt, Preempted, PreemptiveResource, run, sim


async def job(
    machine: PreemptiveResource, name: str, priority: float, work: float
) -> None:
    remaining = work
    while remaining > 0:
        try:
            async with machine.request(priority=priority):
                started = sim.now
                print(f"t={sim.now:g} {name} starts ({remaining:g} left)")
                await sim.timeout(remaining)
                remaining = 0
        except Interrupt as exc:  # outside the async with
            assert isinstance(exc.cause, Preempted)
            remaining -= sim.now - exc.cause.usage_since
            print(f"t={sim.now:g} {name} preempted, {remaining:g} left")
    print(f"t={sim.now:g} {name} done")


async def urgent(machine: PreemptiveResource) -> None:
    await sim.timeout(2)
    await job(machine, "urgent", 1, 3)


async def main() -> None:
    machine = PreemptiveResource(capacity=1)
    async with asyncio.TaskGroup() as tg:
        tg.create_task(job(machine, "routine", 5, 6))
        tg.create_task(urgent(machine))


run(main())
```

```text
t=0 routine starts (6 left)
t=2 urgent starts (3 left)
t=2 routine preempted, 4 left
t=5 urgent done
t=5 routine starts (4 left)
t=9 routine done
```

(`urgent starts` prints before `routine preempted` because preemption is synchronous: the unit
changes hands in the preemptor's step, and the victim only sees its `Interrupt` when it next runs.)

### What is asyncio, what is ours

| Piece | Who |
| --- | --- |
| Everything inherited from `PriorityResource` (futures, cancellation, synchronous release) | as in the `PriorityResource` table |
| Delivering the preemption to the victim | desyncio's interrupt (`sim.interrupt`), which is `task.cancel()` underneath, translated to `Interrupt` at the block's `__aexit__` |
| Choosing the victim, the `Preempted` record, tracking who holds a unit | desyncio |

### Guarantees and cases

- **Preemption is synchronous** (D030, D032), at the same virtual time: the victim is removed
  from the holders, `count` is decremented, the unit goes to the preemptor without passing
  through the queue, then the victim is interrupted. The victim's `async with` exits with
  `Interrupt` and does **not** release again (that would free the preemptor's unit). The monitor
  is called once, after the swap.
- **Release stays free of awaits:** `__aexit__` releases first, then leaves the interruptible
  scope, which never suspends, so no cancel can leak a unit.
- **A granted-but-not-resumed reservation can be preempted** (D034). When a holder releases, the
  best waiter is handed the unit immediately, but its task only resumes on a later loop
  iteration. A better request arriving in that gap preempts it, and the waiter's `async with`
  raises `Interrupt(Preempted)` straight from entering, so its body never starts. Nothing is
  cancelled underneath in that case. (This follows SimPy, where a granted request is already a
  user. It is awaiting the owner's confirmation.)
- **A preemptor never interrupts itself.** Units held by the preemptor's own task are not
  candidates.
- **A real cancel wins** over a preemption in the same instant (as for all interrupts): the
  victim ends with `CancelledError`, and the unit is still released exactly once.
- **Late preemption:** if the body swallows the cancellation (or has no `await` left), the scope
  exits with no `Interrupt`; the unit is already the preemptor's and the exit does not release.
- **Several preemptions in one instant** are processed one at a time in the order the
  preemptors run.
- **Nested `sim.interruptible()` caveat.** `sim.interrupt` is delivered to the *innermost*
  open block. If your body opens its own `interruptible()` block, the `Interrupt(Preempted)`
  lands inside *your* block, and by then the unit is already gone. If you swallow it and carry
  on, the rest of your body runs without the unit: it is not exclusive any more. Nothing is
  double-released, but the guarantee you wanted (one job on the machine) is lost.
  **Do not swallow an `Interrupt` whose `cause` is `Preempted`: re-raise it and catch it outside
  the `async with`.**

The caveat in code (the body keeps "working" after the unit was taken away):

```python
import asyncio

from desyncio import Interrupt, Preempted, PreemptiveResource, run, sim


async def victim(machine: PreemptiveResource) -> None:
    async with machine.request(priority=5):
        try:
            async with (
                sim.interruptible()
            ):  # nested: gets the Interrupt, not the outer block
                await sim.timeout(10)
        except Interrupt as exc:
            assert isinstance(exc.cause, Preempted)
            # swallowed: we carry on, but the unit is the preemptor's now
            print(f"t={sim.now:g} swallowed; units in use by anybody: {machine.count}")
            await sim.timeout(1)
            print(f"t={sim.now:g} victim thinks it still owns the machine")


async def bully(machine: PreemptiveResource) -> None:
    await sim.timeout(2)
    async with machine.request(priority=1):
        print(f"t={sim.now:g} bully has the machine, count={machine.count}")
        await sim.timeout(5)


async def main() -> None:
    machine = PreemptiveResource(capacity=1)
    async with asyncio.TaskGroup() as tg:
        tg.create_task(victim(machine))
        tg.create_task(bully(machine))


run(main())
```

```text
t=2 bully has the machine, count=1
t=2 swallowed; units in use by anybody: 1
t=3 victim thinks it still owns the machine
```

The reservation case (D034). `late` is first in line (`preempt=False`, so it waits politely). At
t=1 the holder releases and `late` is handed the unit, but `vip` arrives in that same instant,
before `late` has run, and takes it:

```python
import asyncio

from desyncio import Interrupt, Preempted, PreemptiveResource, run, sim


async def holder(res: PreemptiveResource) -> None:
    async with res.request(priority=5):
        await sim.timeout(1)


async def late(res: PreemptiveResource) -> None:
    try:
        async with res.request(priority=3, preempt=False):
            print("late: body runs")  # never printed
    except Interrupt as exc:
        assert isinstance(exc.cause, Preempted)
        print(f"t={sim.now:g} late: preempted before its body started")


async def vip(res: PreemptiveResource) -> None:
    await sim.timeout(1)
    async with res.request(priority=0):
        print(f"t={sim.now:g} vip runs, count={res.count}")


async def main() -> None:
    res = PreemptiveResource(capacity=1)
    async with asyncio.TaskGroup() as tg:
        tg.create_task(holder(res))
        tg.create_task(late(res))
        tg.create_task(vip(res))


run(main())
```

```text
t=1 vip runs, count=1
t=1 late: preempted before its body started
```

The other edge cases, run for real. `victim` holds a one-unit machine. At t=1 `vip` preempts it
and, in the same instant, a real `cancel()` also lands on `victim`: the real cancel wins and
`victim` ends with `CancelledError` (not `Interrupt`), and the unit is released exactly once, so
it is reusable afterwards. A body that swallows the cancellation exits quietly with no `Interrupt`
(late preemption), and the unit still belongs to the preemptor. With two units and two preemptors
in one instant, each takes a different victim (the worst holder first):

```python
import asyncio

from desyncio import Interrupt, PreemptiveResource, run, sim


async def real_cancel_wins() -> None:
    machine = PreemptiveResource(capacity=1)

    async def victim() -> None:
        try:
            async with machine.request(priority=5):
                await sim.timeout(10)
        except asyncio.CancelledError:
            print("victim: CancelledError")
            raise

    task = sim.spawn(victim())

    async def vip() -> None:
        await sim.timeout(1)
        async with machine.request(priority=0):
            task.cancel()  # a real cancel, same instant as the preemption
            await sim.timeout(1)

    await asyncio.gather(task, sim.spawn(vip()), return_exceptions=True)
    async with machine.request(priority=9):
        print(f"unit reusable, count={machine.count}")


async def late_preemption() -> None:
    machine = PreemptiveResource(capacity=1)

    async def victim() -> None:
        async with machine.request(priority=5):
            try:
                await sim.timeout(10)
            except asyncio.CancelledError:
                print("victim: swallowed the cancellation")
        print("victim: left the async with quietly, no Interrupt")

    async def vip() -> None:
        await sim.timeout(1)
        async with machine.request(priority=0):
            await sim.timeout(1)

    await asyncio.gather(victim(), vip())


async def two_preemptors() -> None:
    machine = PreemptiveResource(capacity=2)

    async def victim(name: str, priority: float) -> None:
        try:
            async with machine.request(priority=priority):
                await sim.timeout(10)
        except Interrupt:
            print(f"{name} preempted")

    async def vip(name: str, priority: float) -> None:
        await sim.timeout(1)
        async with machine.request(priority=priority):
            print(f"{name} in")
            await sim.timeout(1)

    await asyncio.gather(victim("v1", 5), victim("v2", 6), vip("x", 0), vip("y", 1))


async def main() -> None:
    await real_cancel_wins()
    await late_preemption()
    await two_preemptors()


run(main())
```

```text
victim: CancelledError
unit reusable, count=1
victim: swallowed the cancellation
victim: left the async with quietly, no Interrupt
x in
y in
v2 preempted
v1 preempted
```

## Container

`Container(capacity=math.inf, init=0, *, monitor=None)`: a level of homogeneous stuff (fuel,
water, money). `await c.put(n)` blocks while `level + n > capacity`; `await c.get(n)` blocks
while `level < n`. Amounts are real numbers `> 0` and `<= capacity`. Read-only `level` and
`capacity`.

### Example: fuel tank

```python
import asyncio

from desyncio import Container, run, sim


async def tanker(tank: Container) -> None:
    for _ in range(3):
        await sim.timeout(10)
        await tank.put(40)
        print(f"t={sim.now:g} tanker delivered 40, level={tank.level:g}")


async def truck(name: str, tank: Container, amount: float) -> None:
    await tank.get(amount)
    print(f"t={sim.now:g} {name} took {amount:g}, level={tank.level:g}")


async def main() -> None:
    tank = Container(capacity=100, init=20)
    async with asyncio.TaskGroup() as tg:
        tg.create_task(tanker(tank))
        tg.create_task(truck("big", tank, 50))  # needs 50: waits for the first delivery
        tg.create_task(truck("small", tank, 5))  # 5 would fit now, but "big" is ahead


run(main())
```

```text
t=10 tanker delivered 40, level=60
t=10 big took 50, level=10
t=10 small took 5, level=5
t=20 tanker delivered 40, level=45
t=30 tanker delivered 40, level=85
```

`small` could have been served at t=0 (the level is 20 and it wants 5), but `big` is ahead of it
in the line, so it waits until `big` is served at t=10. The `put` returns to the tanker first and
the woken getters run one step later, which is why the delivery line comes first in the instant.

### What is asyncio, what is ours

asyncio has no "wait until an amount is available" primitive, so `Container` is built on the
closest stock piece, `asyncio.Condition`.

| Piece | Who |
| --- | --- |
| Parking a task until a predicate is true and re-checking it after every wakeup (`Condition.wait_for`) | asyncio |
| Waking waiters (`notify_all`) | asyncio |
| Mutual exclusion on the level, FIFO lock hand-over (the `Lock` inside the `Condition`) | asyncio |
| Cancellation: `Condition.wait` re-acquires the lock even when cancelled, so cleanup always runs holding it | asyncio |
| Strict FIFO with head-of-line blocking: a deque of tickets per direction, and a predicate that also requires "my ticket is at the front" | desyncio (the only extra state; no hand-written futures) |
| Amount validation, `level`/`capacity`, the monitor | desyncio |

Why the tickets: `wait_for` makes each woken task re-check *its own* predicate, so without them a
small `get(1)` queued behind a big `get(10)` would see `level >= 1` and overtake it (D021 wants
strict FIFO).

### Guarantees

- **Order:** puts are served strictly FIFO among puts; gets strictly FIFO among gets. A large
  waiting `get` holds back smaller later ones until it is served (head-of-line blocking, as in
  the example). Puts and gets do not block each other's queues.
- **Cancellation:** a cancelled head leaves the line and the next waiter re-checks at once, so a
  cancelled big request cannot block the queue. A cancelled request changes no level.
- **Cost:** every put or get wakes all parked waiters (`notify_all`) to re-check: O(N) per
  operation for N waiters, though at most the heads can proceed.
- The monitor runs after each level change, not for blocked or cancelled requests.

```python
import asyncio

from desyncio import Container, run, sim


async def main() -> None:
    tank = Container(capacity=100, init=20)

    async def big() -> None:
        try:
            async with asyncio.timeout(3):
                await tank.get(50)
        except TimeoutError:
            print(f"t={sim.now:g} big gave up")

    async def small() -> None:
        await tank.get(5)
        print(f"t={sim.now:g} small served, level={tank.level:g}")

    async with asyncio.TaskGroup() as tg:
        tg.create_task(big())
        tg.create_task(small())  # stuck behind big until big leaves the line


run(main())
```

```text
t=3 big gave up
t=3 small served, level=15
```

## Store, PriorityStore, FilterStore

All three hold discrete items: `await store.put(item)` blocks while full, `await store.get()`
blocks while empty. `capacity` is an `int >= 1` or `math.inf` (the default). `len(store)` is the
item count and `store.items` is a read-only snapshot tuple.

### Example: producer/consumer

```python
import asyncio

from desyncio import Store, run, sim


async def producer(store: Store[str]) -> None:
    for i in range(5):
        await store.put(f"item-{i}")  # blocks while the store is full
        print(f"t={sim.now:g} put item-{i} (stored: {len(store)})")
        await sim.timeout(1)


async def consumer(store: Store[str]) -> None:
    for _ in range(5):
        item = await store.get()  # blocks while the store is empty
        print(f"t={sim.now:g} got {item}")
        await sim.timeout(3)  # slow consumer: the producer will block on a full store


async def main() -> None:
    store = Store[str](capacity=2)
    async with asyncio.TaskGroup() as tg:
        tg.create_task(producer(store))
        tg.create_task(consumer(store))


run(main())
```

```text
t=0 put item-0 (stored: 1)
t=0 got item-0
t=1 put item-1 (stored: 1)
t=2 put item-2 (stored: 2)
t=3 got item-1
t=3 put item-3 (stored: 2)
t=6 got item-2
t=6 put item-4 (stored: 2)
t=9 got item-3
t=12 got item-4
```

### Store: what is asyncio, what is ours

`Store` is a thin wrapper over `asyncio.Queue`, which already is a SimPy-style store.

| Piece | Who |
| --- | --- |
| FIFO order of items | asyncio (`Queue`'s deque) |
| FIFO order of blocked getters and of blocked putters | asyncio (two deques of futures) |
| Blocking `put` while full and `get` while empty | asyncio |
| Cancellation cleanup: a cancelled waiter removes its own future; if it had already been woken it passes the wake-up on, so an item is never lost or duplicated | asyncio |
| `shutdown(immediate=False)`: refuses new puts, wakes blocked waiters with `asyncio.QueueShutDown` | asyncio (`Store.shutdown` is a pass-through) |
| Capacity validation, `math.inf` mapped to `maxsize=0`, `len()`, `items`, the monitor | desyncio |

### PriorityStore

`PriorityStore(capacity=math.inf, *, monitor=None)`: `put(item, priority=0)`; `get()` returns the
item with the **lowest** priority number, ties in put order (D031). Items themselves are never
compared; priorities must be comparable with each other. If the priority is a function of the
item, call `put(job, priority=key(job))`. `items` is in the order `get` will return them.

```python
from desyncio import PriorityStore, run


async def main() -> None:
    store = PriorityStore[str]()
    await store.put("routine", priority=5)
    await store.put("urgent", priority=1)
    await store.put("also routine", priority=5)
    print(store.items)
    print([await store.get() for _ in range(3)])


run(main())
```

```text
('urgent', 'routine', 'also routine')
['urgent', 'routine', 'also routine']
```

| Piece | Who |
| --- | --- |
| Heap ordering, blocking, FIFO of blocked waiters, cancellation cleanup, `shutdown()` | asyncio (`PriorityQueue`) |
| Storing `(priority, sequence, item)` with an `itertools.count()` sequence so ties keep put order and items are never compared; validation; `items`; the monitor | desyncio |

### FilterStore

`FilterStore(capacity=math.inf, *, monitor=None)`: `get(filter=None)` removes and returns the
first item (oldest first) for which `filter(item)` is true; without a filter it is plain FIFO.

```python
import asyncio

from desyncio import FilterStore, run, sim


async def main() -> None:
    parts = FilterStore[str]()

    async def wheel_fitter() -> None:
        part = await parts.get(filter=lambda p: p.startswith("wheel"))
        print(f"t={sim.now:g} fitted {part}")

    async with asyncio.TaskGroup() as tg:
        tg.create_task(wheel_fitter())
        await sim.timeout(1)
        await parts.put("engine")  # no match: the fitter keeps waiting
        await sim.timeout(1)
        await parts.put("wheel-A")
    print(f"left in store: {parts.items}")


run(main())
```

```text
t=2 fitted wheel-A
left in store: ('engine',)
```

asyncio has no filtered queue, so `FilterStore` is built on `asyncio.Condition`, like `Container`.

| Piece | Who |
| --- | --- |
| Parking a getter and re-running its filter each time the store changes (`wait_for`), blocking `put` while full | asyncio (`Condition`) |
| Mutual exclusion and FIFO lock hand-over | asyncio (`Lock`) |
| Cancellation and `Interrupt`: `Condition.wait` re-acquires the lock before re-raising. The item is removed only inside the predicate, in the same synchronous step in which `get` returns, so a getter cancelled the instant an item arrives loses and duplicates nothing | asyncio |
| The item deque, the matching scan, validation, `items`, the monitor | desyncio |

### Ordering and cancellation guarantees (all three stores)

- **Items:** `Store` and `FilterStore` are FIFO; `PriorityStore` is `(priority, put order)`.
- **Blocked getters/putters:** `Store`/`PriorityStore` serve them FIFO (asyncio's queue).
  `FilterStore` retries parked getters longest-waiting first, and the first whose filter matches
  takes the first matching item. A getter whose filter matches nothing parks again, so it never
  blocks a later getter (**no head-of-line blocking**, unlike `Container`).
- **Same-instant barging (D031):** like `asyncio.Queue`, a fresh `get` arriving in the very
  instant of a `put` can take the item ahead of a getter that was already woken but has not run
  yet. All three stores behave identically here. It is deterministic (the same model and seed
  give the same order), just not strict arrival-order fairness; that would need hand-written
  ticket logic, which the project avoids in favour of asyncio's behaviour.
  Here is the barging, identical in all three stores. A getter has waited since t=0; at t=1 an
  item is put and a fresh `get` in the same step takes it, so the old waiter is still waiting:

```python
from desyncio import FilterStore, PriorityStore, Store, run, sim


async def barge(store: Store[int] | PriorityStore[int] | FilterStore[int]) -> str:
    waiter = sim.spawn(store.get())
    await sim.timeout(1)
    await store.put(1)
    await store.get()  # the fresh get wins
    await sim.timeout(1)
    outcome = "old waiter still waiting" if not waiter.done() else "old waiter served"
    waiter.cancel()
    return outcome


async def main() -> None:
    for make in (Store, PriorityStore, FilterStore):
        print(make.__name__, "->", await barge(make()))


run(main())
```

```text
Store -> old waiter still waiting
PriorityStore -> old waiter still waiting
FilterStore -> old waiter still waiting
```

- **Cancellation:** a cancelled `get` or `put` never loses or duplicates an item.
- **A filter that raises:** the exception comes out of that caller's `get`; nothing is removed,
  the lock is released and other waiters are unaffected.
- **`shutdown()`:** `Store` and `PriorityStore` have it (a pass-through to
  `Queue.shutdown`: later `put` raises `asyncio.QueueShutDown`; with `immediate=True` queued items
  are discarded and the monitor is called). **`FilterStore` has no `shutdown()`** (D031):
  `Condition.notify_all()` may only be called with the lock held, which needs an `await`, but
  `shutdown` is synchronous in `asyncio.Queue`. It is left out rather than half-implemented.

```python
import asyncio

from desyncio import FilterStore, Store, run


async def main() -> None:
    store = Store[int]()
    await store.put(1)
    store.shutdown()  # items already inside can still be drained
    print(await store.get())
    try:
        await store.get()
    except asyncio.QueueShutDown:
        print("Store: shut down and empty")
    print("FilterStore has shutdown:", hasattr(FilterStore(), "shutdown"))


run(main())
```

```text
1
Store: shut down and empty
FilterStore has shutdown: False
```

## Interrupts

`Interrupt` is the exception, and `sim.interruptible()` / `sim.interrupt(task, cause)` are how
you raise it (D020, D024). It works like `asyncio.timeout()`, with a different trigger:

| `asyncio.timeout(10)` | desyncio |
| --- | --- |
| `async with asyncio.timeout(10):` marks code that may be cut short | `async with sim.interruptible():` |
| a timer fires | another process calls `sim.interrupt(task, cause)` |
| the timer calls `task.cancel()` | `sim.interrupt` calls `task.cancel()` |
| the block's `__aexit__` turns the `CancelledError` into `TimeoutError` | the block's `__aexit__` turns it into `Interrupt(cause)` |
| `Timeout.expired()` | `scope.interrupted`, and `scope.cause` |

Because it is cancellation underneath, every asyncio primitive (`Semaphore`, `Queue`,
`Condition`, `TaskGroup`, ...) cleans up as it would for any cancel. The translation has to happen
in `__aexit__`: that runs in the *target* task's own frame, exactly where the `CancelledError`
leaves the block, which is the only place it can still be swapped for another exception.

### Example: machine breakdown

```python
import asyncio

from desyncio import Interrupt, run, sim


async def machine(work: float, repair: float) -> None:
    remaining = work
    while remaining > 0:
        started = sim.now
        try:
            async with sim.interruptible():
                await sim.timeout(remaining)
            remaining = 0
        except Interrupt as exc:  # outside the block
            remaining -= sim.now - started
            print(f"t={sim.now:g} broke ({exc.cause}); {remaining:g} of work left")
            await sim.timeout(repair)  # being repaired is not interruptible
    print(f"t={sim.now:g} finished")


async def gremlin(target: "asyncio.Task[None]") -> None:
    await sim.timeout(4)
    sim.interrupt(target, cause="overheated")
    await sim.timeout(5)  # at t=9 the machine is running again
    sim.interrupt(target, cause="jammed")


async def main() -> None:
    task = sim.spawn(machine(work=10, repair=3))
    await gremlin(task)
    await task


run(main())
```

```text
t=4 broke (overheated); 6 of work left
t=9 broke (jammed); 4 of work left
t=16 finished
```

### What is asyncio, what is ours

| Piece | Who |
| --- | --- |
| Delivering the interrupt: `task.cancel()` raising `CancelledError` at the current `await` | asyncio |
| `cancelling()`/`uncancel()` bookkeeping, so real cancels still win | asyncio's task API; the logic is adapted from CPython's `asyncio.Timeout` (PSF-licensed, marked in the source) |
| `Interrupt` (an ordinary `Exception`, deliberately **not** a `CancelledError`), `sim.interruptible()`, `sim.interrupt`, the per-task registry of open blocks | desyncio |

### Guarantees

- **Delivered at the current virtual time**, at the task's current `await` (or its next one, if
  its wait already completed this instant).
- **A real cancel always wins.** If a `task.cancel()`, an enclosing `asyncio.timeout()` or a
  `TaskGroup` also asked for cancellation, the `CancelledError` propagates and is not turned into
  `Interrupt`. The block's `__aexit__` calls `uncancel()` once, which undoes only the
  interrupt's own cancel. So after the block, `task.cancelling()` is back to its value before
  it only if the interrupt was the only cancel. If a real cancel also fired, `cancelling()` stays
  one higher, which is how asyncio (and an enclosing `asyncio.timeout()` or `TaskGroup`) knows
  that the cancel is still pending. The example below shows both cases.
- **First cause wins.** A second `sim.interrupt` into an already interrupted block is ignored.
  After the block has exited you need a new `interruptible()` block.
- **Only the innermost open block is hit** (see the nested caveat under
  [PreemptiveResource](#preemptiveresource-and-preempted)).
- **Errors:** interrupting a finished task, yourself, or a task that is not inside an
  `interruptible()` block raises `RuntimeError`.
- **Known limits:** code in the block that catches `CancelledError` and swallows it also
  swallows the interrupt, exactly as it would swallow a timeout. If a `TaskGroup` inside the
  block ends with an `ExceptionGroup` (a child really failed), the group propagates
  untranslated; `scope.interrupted` is still `True`.

```python
import asyncio

from desyncio import Interrupt, run, sim


async def quiet() -> None:
    async with sim.interruptible() as scope:
        await sim.timeout(10)


async def main() -> None:
    task = sim.spawn(quiet())
    await sim.timeout(1)
    sim.interrupt(task, "first")
    sim.interrupt(task, "second")  # ignored: the block is already interrupted
    try:
        await task
    except Interrupt as exc:
        print(
            "cause:",
            exc.cause,
            "| is a CancelledError:",
            isinstance(exc, asyncio.CancelledError),
        )

    outside = sim.spawn(sim.timeout(5))
    await sim.timeout(1)
    try:
        sim.interrupt(outside)
    except RuntimeError as exc:
        print("RuntimeError:", exc)
    outside.cancel()

    # A real cancel wins over an interrupt delivered in the same instant.
    task = sim.spawn(quiet())
    await sim.timeout(1)
    sim.interrupt(task, "ignored")
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        print("real cancel won")


run(main())
```

```text
cause: first | is a CancelledError: False
RuntimeError: cannot interrupt 'proc-2': it is not inside an 'async with sim.interruptible()' block
real cancel won
```

The `cancelling()` count after the block, with and without a real cancel:

```python
import asyncio

from desyncio import Interrupt, run, sim


async def watched(label: str) -> None:
    task = asyncio.current_task()
    assert task is not None
    before = task.cancelling()
    try:
        async with sim.interruptible():
            await sim.timeout(10)
    except Interrupt:
        print(f"{label}: Interrupt, cancelling() +{task.cancelling() - before}")
    except asyncio.CancelledError:
        print(f"{label}: CancelledError, cancelling() +{task.cancelling() - before}")
        raise


async def main() -> None:
    # Only the interrupt asked: uncancel() restores the count.
    task = sim.spawn(watched("interrupt only"))
    await sim.timeout(1)
    sim.interrupt(task, "breakdown")
    await task

    # A real cancel in the same instant: the count stays one higher.
    task = sim.spawn(watched("interrupt + cancel"))
    await sim.timeout(1)
    sim.interrupt(task, "breakdown")
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


run(main())
```

```text
interrupt only: Interrupt, cancelling() +0
interrupt + cancel: CancelledError, cancelling() +1
```

## Monitoring: the monitor hook and TimeSeries

Every primitive takes `monitor=callable` (D021). The core does no statistics (principle 4:
observable, not opinionated); `TimeSeries` is a consumer built on the hook.

### The monitor contract (D023 v2)

- `monitor(primitive)` is called **synchronously after every state change**, once the state has
  settled. Read the primitive's public properties (`count`, `queue_length`, `level`, `len()`,
  `items`) inside it. It must not block, and must not call `put`/`get` or acquire the primitive.
- **What counts as a change:** `Resource`/`PriorityResource`/`PreemptiveResource`: a request
  queueing, being granted, a queued request leaving (cancelled), a release (and a preemption,
  once, after the swap). `Container`: a level change (not blocked or cancelled requests).
  Stores: each completed `put`/`get`, and `shutdown(immediate=True)` discarding items (not
  cancelled or refused operations).
- **An `Exception` raised by the monitor goes to `loop.call_exception_handler(...)`** (logged by
  default), exactly as asyncio treats a raising `call_soon` callback. The operation **always
  completes**: the request still queues, the grant stands, the item is not lost, and a
  cancelled waiter still sees its `CancelledError`. A monitor can never mask a `CancelledError`.
  `KeyboardInterrupt` and `SystemExit` propagate. To make monitor errors fatal, install your own
  handler with `loop.set_exception_handler()`.

```python
import asyncio
from typing import Any

from desyncio import Store, run


def bad_monitor(store: Store[int]) -> None:
    raise RuntimeError("monitor bug")


async def main() -> None:
    seen: list[str] = []

    def handler(loop: asyncio.AbstractEventLoop, context: dict[str, Any]) -> None:
        seen.append(f"{context['message']}: {context['exception']}")

    asyncio.get_running_loop().set_exception_handler(handler)
    store = Store[int](monitor=bad_monitor)
    await store.put(42)
    print("put completed; item:", await store.get())
    print(seen)


run(main())
```

```text
put completed; item: 42
['Store monitor raised: monitor bug', 'Store monitor raised: monitor bug']
```

### TimeSeries

`TimeSeries()` records values stamped with `sim.now` and computes time-weighted statistics.
Monitors fire only on a change, so record the starting value yourself.

- `record(value)`: only inside `desyncio.run` (`RuntimeError` otherwise). Values are finite
  `int`/`float` (`bool` and other types raise `TypeError`; NaN/inf raise `ValueError`). Time may
  not go backwards (`ValueError`): use a fresh `TimeSeries` per run.
- `mean(until=None)`: the time-weighted mean over `[first record, until]`; each value holds until
  the next record, the last one until `until` (default `sim.now`). Pass `until=` explicitly after
  the run has ended.
- `min()`, `max()`, `last()`, `len()`, and `samples` (a tuple of `(time, value)`).
- With several records at the same time the last one wins for the mean, but all count for
  `min`/`max`.

```python
import asyncio

from desyncio import Resource, TimeSeries, run, sim


async def job(res: Resource, start: float, length: float) -> None:
    await sim.timeout(start)
    async with res:
        await sim.timeout(length)


async def main() -> None:
    busy = TimeSeries()
    server = Resource(1, monitor=lambda r: busy.record(r.count))
    busy.record(0)  # the monitor only fires on a change
    await asyncio.gather(job(server, 0, 4), job(server, 6, 2))
    await sim.timeout(2)  # the run lasts until t=10
    print(f"utilisation over [0, {sim.now:g}]: {busy.mean():.2f}")
    print("max in use:", busy.max())
    print("samples:", busy.samples)


run(main())
```

```text
utilisation over [0, 10]: 0.60
max in use: 1.0
samples: ((0.0, 0.0), (0.0, 1.0), (4.0, 0.0), (6.0, 1.0), (8.0, 0.0))
```

`TimeSeries` imports nothing from the primitives and knows nothing about them: any
`monitor=lambda p: series.record(...)` works.

## Random streams: sim.random

`sim.random(name)` returns the named random stream of the current run: a stdlib
`random.Random` seeded with the string `f"{seed}:{name}"` (D026), where `seed` is the one passed
to `desyncio.run(main(), seed=...)` (default 0). The same name returns the same object each
call, so repeated calls continue one sequence.

- **Same seed, same numbers**, in any process and under any `PYTHONHASHSEED` (CPython seeds a
  `str` with SHA-512, not `hash()`).
- **Streams are independent.** Give each source of randomness its own name (`"arrivals"`,
  `"wash"`): adding or removing draws from one stream does not shift the numbers of another.
- Only usable inside `desyncio.run(...)` (`RuntimeError` otherwise).
- What is asyncio's: nothing, randomness is not an asyncio concern. What is ours: the naming
  scheme and caching; the generator is the standard library's.

### Example: car wash

```python
import asyncio

from desyncio import Resource, run, sim


async def car(i: int, bays: Resource, waits: list[float]) -> None:
    arrived = sim.now
    async with bays:
        waits.append(sim.now - arrived)
        await sim.timeout(sim.random("wash").uniform(3, 5))


async def simulate(n_cars: int) -> list[float]:
    bays = Resource(capacity=2)
    waits: list[float] = []
    async with asyncio.TaskGroup() as tg:
        for i in range(n_cars):
            await sim.timeout(sim.random("arrivals").expovariate(1 / 2))  # mean gap 2
            tg.create_task(car(i, bays, waits))
    return waits


waits = run(simulate(8), seed=1)
print("waits:", [round(w, 2) for w in waits])
print("same seed, same run:", run(simulate(8), seed=1) == waits)
print("other seed differs:", run(simulate(8), seed=2) != waits)
```

```text
waits: [0.0, 0.0, 0.0, 0.0, 0.0, 1.37, 2.5, 3.89]
same seed, same run: True
other seed differs: True
```

The arrivals and the wash times come from two separate streams, so changing how the wash time
is drawn leaves every arrival time unchanged:

```python
from desyncio import run, sim


async def arrivals(also_draw_wash: bool) -> list[float]:
    assert sim.random("arrivals") is sim.random("arrivals")  # one object per name
    if also_draw_wash:
        for _ in range(100):
            sim.random("wash").random()
    return [round(sim.random("arrivals").random(), 4) for _ in range(3)]


quiet = run(arrivals(False), seed=7)
busy = run(arrivals(True), seed=7)
print("arrivals unaffected by the other stream:", quiet == busy)
```

```text
arrivals unaffected by the other stream: True
```

The rounded numbers in the car wash depend on the seed (CPython seeds a `str` deterministically),
not on the machine or the run.
