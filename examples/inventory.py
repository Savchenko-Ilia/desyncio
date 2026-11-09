"""A shop's stock with an (s, S) reorder policy and a delivery lead time.

What it models: every day a random number of units is demanded. Customers who find
the shelf empty are lost (lost sales). The shop checks its stock once a day. When
the stock plus what is already on order drops to `s` or below, it orders enough to
get back up to `S`. The supplier needs a random 2 to 4 days to deliver, so a
delivery is a process of its own that sleeps and then adds to the stock.

desyncio features shown: `Container` (the stock), `TimeSeries` fed by the
container's `monitor=` hook (`mean` and `time_fractions`), `sim.np_random` (numpy
distributions on a named seeded stream), `sim.random` and `sim.spawn`.

How to run it: uv run python examples/inventory.py   (takes well under a second)
"""

from desyncio import Container, TimeSeries, run, sim

S_LOW, S_HIGH = 25, 70  # reorder when position <= S_LOW, order up to S_HIGH
MEAN_DEMAND = 10  # units per day (Poisson)
DAYS = 365  # one time unit is one day


async def main() -> None:
    # Capacity S_HIGH: the position never exceeds it, so a delivery never has to wait.
    level = TimeSeries()
    stock = Container(
        capacity=S_HIGH, init=S_HIGH, monitor=lambda c: level.record(c.level)
    )
    level.record(stock.level)  # the monitor only fires on a change: record the start
    demand_rng = sim.np_random("demand")  # a numpy Generator
    lead_rng = sim.random("lead-time")  # a stdlib random.Random
    on_order = 0  # units ordered but not yet delivered
    orders = stockout_days = 0
    unmet = 0  # an int: demand and stock are whole units

    async def deliver(quantity: int) -> None:
        nonlocal on_order
        await sim.timeout(lead_rng.randint(2, 4))  # days in transit
        await stock.put(quantity)  # adds to the level (it would wait if full)
        on_order -= quantity

    for _ in range(DAYS):
        demand = int(demand_rng.poisson(MEAN_DEMAND))
        # stock.get(n) does NOT fail when the level is below n: it waits until
        # deliveries lift the level to n (back-orders), and everything behind it in
        # the line waits too. To model lost sales we never ask for more than we have.
        # `level` is typed float; here it only ever changes by whole units, so int()
        # loses nothing and keeps `served` and `unmet` integers.
        served = min(demand, int(stock.level))
        if served > 0:
            await stock.get(served)  # enough stock, so this does not wait
        if served < demand:
            stockout_days += 1
            unmet += demand - served

        # End-of-day review. The inventory position counts what is on its way.
        position = stock.level + on_order
        if position <= S_LOW:
            quantity = S_HIGH - int(position)
            on_order += quantity
            orders += 1
            # A new task, so that this loop keeps running while the truck is away.
            sim.spawn(deliver(quantity))
        await sim.timeout(1)  # next day

    # main returns here, which ends the run; deliveries still in transit are cancelled.
    fractions = level.time_fractions(until=sim.now)
    print(f"orders placed: {orders}")
    print(f"days with unmet demand: {stockout_days} of {DAYS}")
    print(f"units of demand lost: {unmet}")
    print(f"mean stock level: {level.mean(until=sim.now):.1f}")
    print(f"fraction of time at zero stock: {fractions.get(0, 0.0):.3f}")


if __name__ == "__main__":
    run(main(), seed=5)
