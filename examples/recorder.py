"""A columnar event recorder, and statistics computed from the event stream.

What it models: customers arrive at a desk with two clerks. Each customer is an entity
that moves through four blocks (door, queue, desk, exit), the way a block-based platform
built on desyncio would model it. The blocks emit their own events; the model computes
no statistics at all. A recorder subscribed to the run keeps every event in columns
(one list per field), and the waiting times and the clerks' utilisation are computed
afterwards from those columns with numpy.

desyncio features shown: custom events (subclasses of `desyncio.events.Event`) emitted
with `sim.emit`, `run(subscribers=[...])`, the built-in `ResourceChanged` and
`RunFinished` events, a named `Resource`, and `sim.random`.

How to run it: uv run python examples/recorder.py   (takes well under a second)
"""

from dataclasses import dataclass

import numpy as np

from desyncio import Resource, run, sim
from desyncio.events import Event, ResourceChanged, RunFinished

N_CUSTOMERS = 2000
ARRIVAL_RATE = 1.5  # customers per minute
SERVICE_RATE = 1.0  # customers per minute, per clerk
CLERKS = 2


# The platform's own events. Every one carries the entity and the block it is in;
# the decorator must be exactly this one (see docs/observing.md).
@dataclass(frozen=True, slots=True, kw_only=True)
class EntityEvent(Event):
    entity_id: int
    block: str


@dataclass(frozen=True, slots=True, kw_only=True)
class EntityCreated(EntityEvent):
    pass


@dataclass(frozen=True, slots=True, kw_only=True)
class ServiceStarted(EntityEvent):
    pass


@dataclass(frozen=True, slots=True, kw_only=True)
class EntityDisposed(EntityEvent):
    pass


class ColumnarRecorder:
    """Keep events as columns: ``t``, ``seq``, ``kind`` and the chosen fields.

    Call it with any ``Event``. A field an event does not have is stored as None,
    so every column has one entry per event and row i is event i.
    """

    def __init__(self, fields: tuple[str, ...]) -> None:
        self.columns: dict[str, list[object]] = {
            name: [] for name in ("t", "seq", "kind", *fields)
        }
        self._fields = fields

    def __call__(self, event: Event) -> None:
        # Called inline at every emit point, so keep it cheap: appends only.
        self.columns["t"].append(event.t)
        self.columns["seq"].append(event.seq)
        self.columns["kind"].append(type(event).__name__)
        for name in self._fields:
            self.columns[name].append(getattr(event, name, None))

    def array(self, name: str) -> np.ndarray:
        """One column as a numpy array (an object array for mixed columns)."""
        return np.asarray(self.columns[name])


async def customer(entity_id: int, desk: Resource) -> None:
    sim.emit(EntityCreated, entity_id=entity_id, block="door")
    service = sim.random("service")
    async with desk:  # waiting here is the queue block
        sim.emit(ServiceStarted, entity_id=entity_id, block="desk")
        await sim.timeout(service.expovariate(SERVICE_RATE))
    sim.emit(EntityDisposed, entity_id=entity_id, block="exit")


async def source() -> None:
    desk = Resource(CLERKS, name="desk")
    arrivals = sim.random("arrivals")
    customers = []
    for entity_id in range(N_CUSTOMERS):
        customers.append(sim.spawn(customer(entity_id, desk)))
        await sim.timeout(arrivals.expovariate(ARRIVAL_RATE))
    for task in customers:
        await task  # the run ends when the last customer has left


def main() -> None:
    rec = ColumnarRecorder(fields=("entity_id", "resource", "in_use", "capacity"))
    run(source(), seed=2026, subscribers=[rec])

    t, kind = rec.array("t").astype(float), rec.array("kind")
    print(f"events recorded: {len(t)} (last seq {rec.columns['seq'][-1]})")

    # Waits: the time from EntityCreated to ServiceStarted, matched by entity_id.
    # The "kind" column holds class names, as in Event.to_dict().
    ids = rec.array("entity_id")
    new = kind == EntityCreated.__name__
    created = dict(zip(ids[new], t[new], strict=True))
    started = kind == ServiceStarted.__name__
    waits = t[started] - np.array([created[i] for i in ids[started]])
    print(f"customers served: {len(waits)}")
    print(f"mean wait: {waits.mean():.2f} min, max {waits.max():.2f} min")
    print(f"90% of customers waited less than {np.quantile(waits, 0.9):.2f} min")

    # Utilisation: time-weighted mean of in_use / capacity. Each ResourceChanged holds
    # the state *after* a change, which lasts until the next one; the last state
    # lasts until RunFinished (the stream ends there, docs/observing.md).
    changes = (kind == ResourceChanged.__name__) & (rec.array("resource") == "desk")
    times = np.append(t[changes], t[kind == RunFinished.__name__][0])
    in_use = rec.array("in_use")[changes].astype(float)
    busy = float(np.sum(in_use * np.diff(times)))
    capacity = rec.array("capacity")[changes][0]
    print(f"clerk utilisation: {busy / (times[-1] * capacity):.3f}")
    print(f"theory (long run): {ARRIVAL_RATE / (CLERKS * SERVICE_RATE):.3f}")


if __name__ == "__main__":
    main()
