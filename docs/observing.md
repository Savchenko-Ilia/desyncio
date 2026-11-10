# Observing a simulation

A run produces a stream of **events**: small, immutable records that say what happened and
when. desyncio emits a few itself (the run starts, the run ends, a resource changes), and your
model can emit its own. You observe the stream by **subscribing** a handler: a plain function
that is called with each event. This is how a log, a recorder, a live chart or a platform built
on desyncio learns what a model does, without the model knowing who listens.

Every `python` block below is executed by `tests/test_docs_examples.py`, and the `text` block
under it is compared with what the example really prints (the same convention as the
[primitives guide](primitives.md)). All seeds are fixed, so the output is the same on every
run. The event classes live in the `desyncio.events` module. The examples write
`from desyncio import events` and then `events.Event`; `from desyncio.events import Event`
works just as well.

Contents:

1. [Subscribing](#subscribing)
2. [The built-in events](#the-built-in-events)
3. [Custom events](#custom-events)
4. [Delivery rules and handler errors](#delivery-rules-and-handler-errors)
5. [Debug mode: the observer guard](#debug-mode-the-observer-guard)
6. [Events, monitor= and the trace hook](#events-monitor-and-the-trace-hook)
7. [Determinism](#determinism)

## Subscribing

`sim.subscribe(handler, *, kinds=None)` subscribes `handler` to the events of the running
simulation, from now on:

- `handler` is a plain function taking one event. An `async def` handler raises `TypeError`:
  events are delivered synchronously, and a coroutine would never run.
- `kinds` is `None` (every event) or a non-empty tuple of event classes. A handler gets an
  event if `isinstance(event, kinds)` is true, so a subclass of a listed kind matches too.
- It returns a `Subscription`. `close()` ends it (a second `close()` does nothing), and so does
  leaving its `with` block.

```python
from desyncio import Resource, events, run, sim


async def main() -> None:
    server = Resource(1, name="server")
    changes = (events.ResourceChanged,)

    sub = sim.subscribe(lambda e: print("all:  ", e), kinds=changes)
    with sim.subscribe(lambda e: print("with: ", e.in_use), kinds=changes):
        async with server:
            await sim.timeout(2)
    # The with block has closed the second subscription; the first still listens.
    async with server:
        pass
    sub.close()
    async with server:  # nobody listens any more
        pass


run(main())
```

```text
all:   ResourceChanged(t=0.0, seq=1, resource='server', in_use=1, capacity=1, queue_len=0)
with:  1
all:   ResourceChanged(t=2.0, seq=2, resource='server', in_use=0, capacity=1, queue_len=0)
with:  0
all:   ResourceChanged(t=2.0, seq=3, resource='server', in_use=1, capacity=1, queue_len=0)
all:   ResourceChanged(t=2.0, seq=4, resource='server', in_use=0, capacity=1, queue_len=0)
```

`sim.subscribe` needs a running simulation, so a subscription made inside `main` misses the very
first event (`RunStarted`, `seq=0` above). To see the whole run, pass the handlers to
`desyncio.run(..., subscribers=[...])`. They are subscribed before anything runs, in list
order. Each item is a handler, or a `(handler, kinds)` pair.

```python
from desyncio import events, run, sim


async def main() -> str:
    await sim.timeout(5)
    return "done"


log: list[events.Event] = []
ends: list[events.Event] = []
only_end = (ends.append, (events.RunFinished,))  # a (handler, kinds) pair
result = run(main(), seed=7, subscribers=[log.append, only_end])
print(result)
for e in log:
    print(e)
print(ends == log[-1:])
```

```text
done
RunStarted(t=0.0, seq=0, seed=7, until=None)
RunFinished(t=5.0, seq=1, reason='main_returned')
True
```

`replicate(model, n, subscribers=[...])` passes the same handlers to every run: they get the
events of run 0, then of run 1, and so on, each from its `RunStarted` to its `RunFinished`.

```python
from desyncio import events, replicate, sim


async def model() -> float:
    await sim.timeout(sim.random("t").uniform(1, 2))
    return sim.now


starts: list[events.Event] = []
only_start = (starts.append, (events.RunStarted,))
results = replicate(model, 3, seed=10, subscribers=[only_start])
print([round(r, 3) for r in results])
print([e.to_dict()["seed"] for e in starts])
```

```text
[1.659, 1.469, 1.076]
[10, 11, 12]
```

## The built-in events

Every event is an instance of `events.Event`, a frozen dataclass with two fields:

- `t`: the virtual time of the event;
- `seq`: its number in this run, 0, 1, 2, ... in the order the events were emitted. It is a
  total order, also among events at the same `t`.

`to_dict()` returns the fields as a plain dict plus `"kind"` (the class name), ready for JSON, a
log line or a database row. `events.EVENT_SCHEMA_VERSION` (now 1) is bumped whenever a field of
a built-in event changes, so a stored log can say which layout it uses.

desyncio emits three kinds:

| Event | Fields | When |
|---|---|---|
| `RunStarted` | `seed`, `until` (a float, or `None`) | First event of a run, `seq` 0, before any process runs. |
| `RunFinished` | `reason`: `"main_returned"`, `"until"` or `"error"` | Last event of a run. |
| `ResourceChanged` | `resource`, `in_use`, `capacity`, `queue_len` | After every state change of a `Resource`, `PriorityResource`, `PreemptiveResource` or `ScheduledResource`. |

`ResourceChanged` carries the state *after* the change: `in_use` is the resource's `count`,
`queue_len` its `queue_length` (which counts a request that was granted but whose task has not
resumed yet). `resource` is the resource's name. Give one with `name=`; a resource without a name
gets `resource-1`, `resource-2`, ... in the order in which resources are first used in the run.
An automatic name skips a name already taken, and an explicit name that is already used in the
run raises `ValueError` (D064). The names belong to the run: the same unnamed resource can be
`resource-1` in one run and `resource-2` in another, if the model uses its resources in another
order.

A `RunFinished` handler is called after the loop has stopped, so it can read the event but
cannot use `sim`. The reason `"error"` covers anything `main` raises, including
`SimulationDeadlock`.

```python
from desyncio import Resource, events, run, sim


async def worker(res: Resource, hold: float) -> None:
    async with res:
        await sim.timeout(hold)


async def main() -> None:
    a = Resource(1)
    b = Resource(2, name="crane")
    c = Resource(1)
    await worker(c, 1)  # c is used first, so it is resource-1
    await worker(b, 1)
    await worker(a, 1)  # a is resource-2


def show(e: events.Event) -> None:
    d = e.to_dict()
    print(d.pop("kind"), d)


run(main(), until=10, subscribers=[show])
print(events.EVENT_SCHEMA_VERSION)
```

```text
RunStarted {'t': 0.0, 'seq': 0, 'seed': 0, 'until': 10.0}
ResourceChanged {'t': 0.0, 'seq': 1, 'resource': 'resource-1', 'in_use': 1, 'capacity': 1, 'queue_len': 0}
ResourceChanged {'t': 1.0, 'seq': 2, 'resource': 'resource-1', 'in_use': 0, 'capacity': 1, 'queue_len': 0}
ResourceChanged {'t': 1.0, 'seq': 3, 'resource': 'crane', 'in_use': 1, 'capacity': 2, 'queue_len': 0}
ResourceChanged {'t': 2.0, 'seq': 4, 'resource': 'crane', 'in_use': 0, 'capacity': 2, 'queue_len': 0}
ResourceChanged {'t': 2.0, 'seq': 5, 'resource': 'resource-2', 'in_use': 1, 'capacity': 1, 'queue_len': 0}
ResourceChanged {'t': 3.0, 'seq': 6, 'resource': 'resource-2', 'in_use': 0, 'capacity': 1, 'queue_len': 0}
RunFinished {'t': 3.0, 'seq': 7, 'reason': 'main_returned'}
1
```

`main` returned at t=3, before the limit, so the reason is `main_returned`, not `until`.

**`seq` never depends on who listens** (D060). Every emit point advances the counter, also when
nobody subscribed to that kind. A handler with a `kinds` filter therefore sees gaps in `seq`,
and the numbers it sees are the ones the unfiltered stream has. This makes filtered logs of the
same run comparable, and it means subscribing never renumbers anything.

```python
from desyncio import Resource, events, run, sim


async def main() -> None:
    res = Resource(1)
    for _ in range(2):
        async with res:
            await sim.timeout(1)


finished: list[events.Event] = []
run(main(), subscribers=[(finished.append, (events.RunStarted, events.RunFinished))])
print([e.seq for e in finished])
```

```text
[0, 5]
```

## Custom events

desyncio has no entities or blocks; a model or a platform defines the events it needs (D056).
A custom event is a subclass of `events.Event` with **exactly this decorator**:
`@dataclass(frozen=True, slots=True, kw_only=True)`.

- `frozen=True` makes the event immutable, so one handler cannot change what the next one sees.
- `slots=True` must be repeated on every subclass, or that subclass gets a `__dict__` again.
- `kw_only=True` lets a subclass add required fields after the base class's fields, and matches
  how events are built (by name).

Keep the fields to `int`, `float`, `str`, `bool` and `None`, so every event stays easy to
serialise and compare. `"kind"` in `to_dict()` is the bare class name, so two classes with the
same name collide: pick distinct names.

Emit one with `sim.emit(Kind, **fields)`. The sim fills in `t` and `seq`; do not pass them. The
event object is built only if some subscriber wants that kind; otherwise `emit` only advances
`seq`. So a wrong field name is reported (by the dataclass constructor, as `TypeError`) only
once somebody subscribes to that kind.

```python
from dataclasses import dataclass

from desyncio import events, run, sim


@dataclass(frozen=True, slots=True, kw_only=True)
class OrderShipped(events.Event):
    order_id: int
    carrier: str


async def main() -> None:
    sim.emit(OrderShipped, order_id=1, carier="post")  # typo, but nobody listens yet
    with sim.subscribe(print, kinds=(OrderShipped,)):
        await sim.timeout(1)
        sim.emit(OrderShipped, order_id=2, carrier="post")
        try:
            sim.emit(OrderShipped, order_id=3, carier="post")
        except TypeError as exc:
            print("TypeError, names the field:", "'carier'" in str(exc))


run(main())
```

```text
OrderShipped(t=1.0, seq=2, order_id=2, carrier='post')
TypeError, names the field: True
```

The first emit raised nothing, but it still took `seq` 1.

### A block-style model

A platform built on desyncio models a process as blocks: a source creates entities, a queue
and a service hold them, a sink disposes of them. Here each block is a plain coroutine, the
service is a named `Resource`, and each block reports what happens to an entity with its own
events. The model does not compute any statistic. The wait times are computed afterwards from
the stream: an entity waits from `EntityCreated` until its `ServiceStarted`.

```python
from dataclasses import dataclass

from desyncio import Resource, events, run, sim


@dataclass(frozen=True, slots=True, kw_only=True)
class EntityEvent(events.Event):
    entity_id: int
    block: str


@dataclass(frozen=True, slots=True, kw_only=True)
class EntityCreated(EntityEvent):
    pass


@dataclass(frozen=True, slots=True, kw_only=True)
class ServiceStarted(EntityEvent):
    pass


@dataclass(frozen=True, slots=True, kw_only=True)
class ServiceFinished(EntityEvent):
    pass


@dataclass(frozen=True, slots=True, kw_only=True)
class EntityDisposed(EntityEvent):
    pass


async def service(entity_id: int, desk: Resource) -> None:
    async with desk:  # the queue is the resource's own FIFO queue
        sim.emit(ServiceStarted, entity_id=entity_id, block="desk")
        await sim.timeout(sim.random("service").expovariate(1 / 4))
        sim.emit(ServiceFinished, entity_id=entity_id, block="desk")
    sim.emit(EntityDisposed, entity_id=entity_id, block="exit")


async def source(n: int) -> None:
    desk = Resource(1, name="desk")
    arrivals = sim.random("arrivals")
    jobs = []
    for entity_id in range(n):
        sim.emit(EntityCreated, entity_id=entity_id, block="door")
        jobs.append(sim.spawn(service(entity_id, desk)))
        await sim.timeout(arrivals.expovariate(1 / 5))
    for job in jobs:
        await job


stream: list[events.Event] = []
run(source(200), seed=1, subscribers=[(stream.append, (EntityEvent,))])

created = {e.entity_id: e.t for e in stream if isinstance(e, EntityCreated)}
waits = [e.t - created[e.entity_id] for e in stream if isinstance(e, ServiceStarted)]
print(len(stream), "entity events")
print(stream[0].to_dict())
print(f"mean wait {sum(waits) / len(waits):.2f}, max wait {max(waits):.2f}")
```

```text
800 entity events
{'t': 0.0, 'seq': 1, 'entity_id': 0, 'block': 'door', 'kind': 'EntityCreated'}
mean wait 13.20, max wait 54.47
```

The subscription asks for `EntityEvent`, the common base class, and gets all four kinds
(`isinstance` filtering). `seq` starts at 1 because `RunStarted` (seq 0) is not in the filter;
the `ResourceChanged` events of the desk take numbers in between too. The
[`examples/recorder.py`](../examples/recorder.py) script does the same with a columnar recorder
and also computes the desk's utilisation from its `ResourceChanged` events.

## Delivery rules and handler errors

- **Synchronous and inline.** `emit` calls the handlers itself, in the emitting task's step,
  before it returns. A handler sees the model exactly as it is at the emit point. Nothing is
  added to the loop: scheduling delivery with `call_soon` would add callbacks and change what
  runs when, so merely subscribing could change the model.
- **Subscription order.** Handlers get each event in the order they subscribed. Subscribing or
  closing while an event is being delivered takes effect from the next event.
- **Filtering** is `isinstance(event, kinds)`.

**Handler errors (D058).** An exception raised by a handler never reaches the task that emitted
the event: it would replace that task's own result, a model's `try`/`except` could swallow it,
and on a resource's grant path it would leak a unit. Instead the bus keeps the first exception
and stops delivering: the other handlers do not get that event, nobody gets a later one, and
there is **no `RunFinished`**. The loop stops after the current iteration, and `desyncio.run`
raises that same exception after closing the loop.

That gives a clean way to **stop a run early** (D061): a recorder that raises its own exception
when it has seen enough. Every event up to and including the one it raised on was delivered, and
teardown runs as usual.

```python
from dataclasses import dataclass

from desyncio import events, run, sim


@dataclass(frozen=True, slots=True, kw_only=True)
class Tick(events.Event):
    n: int


class EnoughEvents(Exception):
    pass


async def ticker() -> None:
    while True:  # never ends on its own
        sim.emit(Tick, n=round(sim.now))
        await sim.timeout(1)


recorded: list[events.Event] = []


def capped(e: events.Event) -> None:
    recorded.append(e)
    if len(recorded) == 4:
        raise EnoughEvents(f"stopped at t={e.t}")


try:
    run(ticker(), subscribers=[capped])
except EnoughEvents as exc:
    print("run raised:", exc)
print([type(e).__name__ for e in recorded])
```

```text
run raised: stopped at t=2.0
['RunStarted', 'Tick', 'Tick', 'Tick']
```

**What the stream does not contain.** The stream ends with `RunFinished`, and the bus is closed
right after it, *before* teardown (D057). At teardown, `desyncio.run` cancels the tasks that are
still running, as `asyncio.run` does. It cancels them in an order that depends on object
addresses, so events from teardown would differ between identical runs; they are not delivered.
So after an `until` cut, the last `ResourceChanged` of a resource may still show units in use
and requests in the queue: those holders were never released inside the run. A handler that
tracks open intervals (a busy period, a wait) should close them at `RunFinished.t`.

```python
from desyncio import Resource, events, run, sim


async def customer(desk: Resource) -> None:
    async with desk:
        await sim.timeout(10)


async def main() -> None:
    desk = Resource(1, name="desk")
    for _ in range(3):
        sim.spawn(customer(desk))
    await sim.timeout(100)


stream: list[events.Event] = []
run(main(), until=5, subscribers=[stream.append])
print(stream[-2])
print(stream[-1])
```

```text
ResourceChanged(t=0.0, seq=3, resource='desk', in_use=1, capacity=1, queue_len=2)
RunFinished(t=5.0, seq=4, reason='until')
```

## Debug mode: the observer guard

A handler observes; it must not change the run. Outside debug mode that is your responsibility.
In asyncio's debug mode desyncio checks it (D059). Switch debug mode on with
`desyncio.run(..., debug=True)` (passed on to `asyncio.Runner`, as in `asyncio.run`),
`replicate(..., debug=True)`, or `PYTHONASYNCIODEBUG=1` / `python -X dev` with the default
`debug=None`. Then these calls raise `events.ObserverError` (a `RuntimeError`) when an event
handler makes them:

- creating a task (`sim.spawn`, `asyncio.create_task`);
- scheduling a callback or timer (`loop.call_soon`, `call_later`, `call_at`);
- `sim.interrupt`;
- `sim.random` and `sim.np_random` (a draw in a handler would change the model's next value);
- `sim.emit`.

`sim.subscribe` and `Subscription.close` stay allowed. An `ObserverError` is a handler error,
so the run ends as above and `run` raises it. A handler cannot hide it with `try`/`except`: the
error is recorded before it is raised.

```python
from desyncio import Resource, events, run, sim


async def main() -> None:
    async with Resource(1):
        await sim.timeout(1)


def meddling(e: events.Event) -> None:
    if isinstance(e, events.ResourceChanged):
        try:
            sim.random("extra").random()  # would shift the model's random numbers
        except events.ObserverError:
            print("caught it, but the run still ends")


try:
    run(main(), debug=True, subscribers=[meddling])
except events.ObserverError as exc:
    print("run raised ObserverError:", str(exc).split(" was called")[0])
```

```text
caught it, but the run still ends
run raised ObserverError: sim.random
```

Two cases are **recorded instead of raised** (D066, D068): `loop.call_soon`, and a primitive's
own event such as `ResourceChanged` emitted while a handler runs. Both happen in the middle of a
state change (a handler that resolves a future someone awaits calls `call_soon` through asyncio;
a handler that drives a resource by hand reaches its event at the end of an acquire), and
raising there would lose a wake-up or leak a unit. The call completes, and the run still ends
with the `ObserverError`, whose notes show where the handler made the call.

What the guard **cannot see**: changes that schedule nothing, such as `store.shutdown()` with
nobody waiting, `task.cancel()` of a task that is already about to run, or a draw from a
`random.Random` stream that the handler fetched before the event. `call_soon_threadsafe` is not
guarded, because other threads (a real-time pacer's resume) and asyncio's async-generator
finalizer use it. One false alarm is possible (D068 addendum): if garbage collection runs during
a handler and closes a task that was leaked while it held a resource, that task's release emits
a `ResourceChanged` and the guard records an `ObserverError` that blames the handler. It needs a
model bug (a lost pending task, which asyncio already reports) and depends on GC timing.

In a run where no handler breaks the rule, debug mode does not change the result.

## Events, monitor= and the trace hook

desyncio has three ways to observe, at three levels:

- **`monitor=`** on a primitive (see the [primitives guide](primitives.md#monitoring-the-monitor-hook-and-timeseries)):
  a callback for one primitive, called with the primitive itself after each state change. It
  feeds `TimeSeries` and works on any primitive, including `Container` and `Store`, which emit no
  events. A monitor's error is logged and the run carries on (D023 v2). On a resource the monitor
  is called first, then `ResourceChanged` is emitted.
- **Events** (this guide): one stream for the whole run, typed and serialisable, with the
  model's own events in it. A handler's error ends the run (D058), because a subscriber is often
  the only record of the run.
- **The trace hook** (`desyncio.run(..., trace=sink)`, see `desyncio.trace`): loop internals,
  one record per task start, resume, finish and cancel. Useful to see scheduling; it knows
  nothing about resources or entities (D016).

All three observe without changing the model. Events cost something only when a handler wants
them; on a model that does almost nothing but use one resource, one no-op subscriber for every
event measured about +12% to +27% (informative; the owner accepts up to 30%, D069). See
[`bench/README.md`](../bench/README.md).

## Determinism

The same model with the same seed gives the same list of events: the same kinds, fields, `t`
and `seq`, in the same order. That holds in the same process and across processes (a test
compares a run with one in a fresh interpreter). Subscribing never changes the model, and a
`kinds` filter never changes the `seq` numbers. The guarantee is for one runtime build (the same
Python, platform and math library): some random distributions (`expovariate`, `gauss`, ...) call
the C math library, whose results can differ in the last bits on another build.

```python
from desyncio import Resource, events, run, sim


async def main() -> None:
    res = Resource(1)
    rng = sim.random("work")

    async def job() -> None:
        async with res:
            await sim.timeout(rng.expovariate(1.0))

    for _ in range(20):
        sim.spawn(job())
        await sim.timeout(rng.expovariate(1.2))


def record(seed: int) -> list[dict[str, object]]:
    log: list[events.Event] = []
    run(main(), seed=seed, subscribers=[log.append])
    return [e.to_dict() for e in log]


print(record(3) == record(3), record(3) == record(4))
```

```text
True False
```
