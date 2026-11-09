"""Tests for ``desyncio.monitor.TimeSeries`` (MON-01)."""

import asyncio
import math
from collections.abc import Awaitable, Callable

import pytest

import desyncio
from desyncio import sim
from desyncio.monitor import TimeSeries
from desyncio.resource import Resource


def run_ts(body: Callable[[TimeSeries], Awaitable[None]]) -> TimeSeries:
    """Run ``body(ts)`` (an async function) in a fresh sim; return the series."""
    ts = TimeSeries()

    async def main() -> None:
        await body(ts)

    desyncio.run(main())
    return ts


def _record_both(busy: TimeSeries, waiting: TimeSeries) -> Callable[[Resource], None]:
    """A Resource monitor: record the count and the queue length."""

    def monitor(r: Resource) -> None:
        busy.record(r.count)
        waiting.record(r.queue_length)

    return monitor


async def _steps(ts: TimeSeries) -> None:
    ts.record(2)  # t=0
    await sim.timeout(2)
    ts.record(4)  # t=2
    await sim.timeout(2)
    ts.record(1)  # t=4


def test_time_weighted_mean_min_max_last() -> None:
    ts = run_ts(_steps)
    # 2 for 2s, 4 for 2s, 1 for 4s over [0, 8] -> (4 + 8 + 4) / 8
    assert ts.mean(until=8) == pytest.approx(2.0)
    assert (ts.min(), ts.max(), ts.last()) == (1, 4, 1)
    assert ts.samples == ((0.0, 2.0), (2.0, 4.0), (4.0, 1.0))


def test_mean_and_record_outside_run_raise_like_sim_now() -> None:
    ts = run_ts(_steps)
    with pytest.raises(RuntimeError):
        ts.mean()  # no `until`, so it needs sim.now
    with pytest.raises(RuntimeError):
        ts.record(1)


def test_mean_starts_at_first_record_not_zero() -> None:
    async def body(ts: TimeSeries) -> None:
        await sim.timeout(5)
        ts.record(3)
        await sim.timeout(1)
        ts.record(5)

    ts = run_ts(body)
    # span [5, 7]: 3 for 1s, 5 for 1s
    assert ts.mean(until=7) == pytest.approx(4.0)


def test_until_before_last_record_is_value_error() -> None:
    ts = run_ts(_steps)
    with pytest.raises(ValueError):
        ts.mean(until=3.9)
    assert ts.mean(until=4) == pytest.approx((4 + 8) / 4)  # equal is fine


def test_empty_series_raises_value_error() -> None:
    ts = TimeSeries()
    calls: list[Callable[[], object]] = [
        lambda: ts.mean(until=1),
        ts.min,
        ts.max,
        ts.last,
    ]
    for call in calls:
        with pytest.raises(ValueError):
            call()
    assert ts.samples == ()
    assert len(ts) == 0


def test_zero_length_span_returns_last_value() -> None:
    async def body(ts: TimeSeries) -> None:
        ts.record(1)
        ts.record(7)  # same instant

    ts = run_ts(body)
    assert ts.mean(until=0) == 7


def test_same_instant_last_wins_for_mean_but_all_count_for_min_max() -> None:
    async def body(ts: TimeSeries) -> None:
        ts.record(1)
        ts.record(100)  # zero weight in the mean
        ts.record(3)
        await sim.timeout(2)

    ts = run_ts(body)
    assert ts.mean(until=2) == pytest.approx(3.0)
    assert ts.max() == 100
    assert ts.min() == 1
    assert len(ts.samples) == 3


@pytest.mark.parametrize(
    ("bad", "exc"),
    [
        (True, TypeError),
        ("1", TypeError),
        (math.nan, ValueError),
        (math.inf, ValueError),
        pytest.param(10**400, ValueError, id="too-big"),  # the OverflowError branch
    ],
)
def test_record_rejects_bad_value(bad: object, exc: type[Exception]) -> None:
    async def main() -> TimeSeries:
        ts = TimeSeries()
        ts.record(1)
        with pytest.raises(exc):
            ts.record(bad)  # type: ignore[arg-type]
        return ts

    assert desyncio.run(main()).samples == ((0.0, 1.0),)  # unchanged


def test_mean_rejects_bad_until() -> None:
    ts = run_ts(_steps)
    with pytest.raises(TypeError):
        ts.mean(until=True)
    with pytest.raises(ValueError):
        ts.mean(until=math.nan)


# --- M/M/1 -----------------------------------------------------------------

RHO = 0.6  # Lq = 0.9 != rho, so swapping the two series is caught
N_CUSTOMERS = 20_000
SEED = 7


def mm1_stats(seed: int) -> tuple[float, float]:
    async def main() -> tuple[float, float]:
        busy = TimeSeries()
        waiting = TimeSeries()
        server = Resource(
            1,
            monitor=_record_both(busy, waiting),
        )
        busy.record(0)
        waiting.record(0)
        arrivals = sim.random("arrivals")
        service = sim.random("service")

        async def customer() -> None:
            async with server:
                await sim.timeout(service.expovariate(1.0))

        tasks = []
        for _ in range(N_CUSTOMERS):
            await sim.timeout(arrivals.expovariate(RHO))
            tasks.append(sim.spawn(customer()))
        await asyncio.gather(*tasks)
        return busy.mean(), waiting.mean()

    return desyncio.run(main(), seed=seed)


def test_mm1_utilisation_and_queue_length_match_theory() -> None:
    util, queue = mm1_stats(SEED)
    # Theory: utilisation rho = 0.6, Lq = rho^2 / (1 - rho) = 0.9.
    # rho is chosen so that the two differ by 0.3: recording r.count into the
    # queue series, or swapping the series, fails both asserts.
    # Bands: over seeds 1..10 checked locally (20k customers, rho=0.6) the
    # error was within +-0.012 for utilisation and +-0.040 for Lq. The bands
    # are about 3x that (0.03 and 0.12): a sanity check on the integral, not a
    # statistical test, and still well below the 0.3 gap. The seed is fixed.
    assert util == pytest.approx(RHO, abs=0.03)
    assert queue == pytest.approx(RHO**2 / (1 - RHO), abs=0.12)


def test_resource_count_and_queue_length_recorded_separately() -> None:
    async def main() -> tuple[TimeSeries, TimeSeries]:
        busy = TimeSeries()
        waiting = TimeSeries()
        res = Resource(
            1,
            monitor=_record_both(busy, waiting),
        )
        busy.record(0)
        waiting.record(0)

        async def user() -> None:
            async with res:
                await sim.timeout(2)

        first = sim.spawn(user())
        await sim.timeout(1)  # first customer in service, nobody waiting
        assert (busy.last(), waiting.last()) == (1, 0)
        second = sim.spawn(user())
        await sim.timeout(0.5)  # second customer queued behind the first
        assert (busy.last(), waiting.last()) == (1, 1)
        await asyncio.gather(first, second)
        return busy, waiting

    busy, waiting = desyncio.run(main())
    assert busy.last() == 0
    assert waiting.last() == 0
    assert waiting.max() == 1
    assert busy.max() == 1


def mm1_customer_records(
    seed: int, monitored: bool
) -> list[tuple[int, float, float, float]]:
    """Per-customer ``(id, arrival, service start, departure)`` of an M/M/1 run.

    With ``monitored=False`` the Resource has no monitor at all; with
    ``monitored=True`` two TimeSeries are wired in as in ``mm1_stats``.
    """

    async def main() -> list[tuple[int, float, float, float]]:
        records: list[tuple[int, float, float, float]] = []
        if monitored:
            busy = TimeSeries()
            waiting = TimeSeries()
            server = Resource(
                1,
                monitor=_record_both(busy, waiting),
            )
            busy.record(0)
            waiting.record(0)
        else:
            server = Resource(1)
        arrivals = sim.random("arrivals")
        service = sim.random("service")

        async def customer(i: int) -> None:
            arrived = sim.now
            async with server:
                started = sim.now
                await sim.timeout(service.expovariate(1.0))
            records.append((i, arrived, started, sim.now))

        tasks = []
        for i in range(2_000):
            await sim.timeout(arrivals.expovariate(RHO))
            tasks.append(sim.spawn(customer(i)))
        await asyncio.gather(*tasks)
        if monitored:
            assert len(busy) > 1000  # the monitors really fired
        return records

    return desyncio.run(main(), seed=seed)


def test_wiring_timeseries_in_does_not_change_the_model() -> None:
    # D016 / D029: observability must not alter the model. Same seed, with
    # and without monitors: identical per-customer records.
    plain = mm1_customer_records(SEED, monitored=False)
    observed = mm1_customer_records(SEED, monitored=True)
    assert len(plain) == 2_000
    assert observed == plain


def test_record_earlier_than_last_record_raises_and_leaves_series_unchanged() -> None:
    # D029: a series belongs to one run. A second desyncio.run restarts the
    # clock at 0, so recording there would go back in time.
    ts = TimeSeries()

    async def first() -> None:
        await sim.timeout(5)
        ts.record(3)

    async def second() -> None:
        with pytest.raises(ValueError):
            ts.record(1)  # t=0 < 5

    desyncio.run(first())
    desyncio.run(second())
    assert ts.samples == ((5.0, 3.0),)
    assert ts.mean(until=7) == pytest.approx(3.0)
    assert (ts.min(), ts.max(), ts.last()) == (3, 3, 3)


# --- time-weighted distribution (STA-02) -----------------------------------


async def _dist(ts: TimeSeries) -> None:
    ts.record(1)  # t=0, 1 for 1s
    await sim.timeout(1)
    ts.record(5)  # t=1, 5 for 1s
    ts.record(2)  # same instant: last wins, 5 gets no entry
    await sim.timeout(1)
    ts.record(1)  # t=2, 1 again


def test_distribution_exact_on_hand_made_series() -> None:
    ts = run_ts(_dist)
    # until=4: 1 for 1s, 2 for 1s, 1 for 2s
    fr = ts.time_fractions(until=4)
    assert fr == {1.0: 0.75, 2.0: 0.25}  # 5.0 was held for zero time: no entry
    assert math.fsum(fr.values()) == pytest.approx(1.0, abs=1e-12)
    assert ts.mean(until=4) == pytest.approx(1.25)
    assert ts.variance(until=4) == pytest.approx(0.75 * 0.25**2 + 0.25 * 0.75**2)
    q = [ts.quantile(p, until=4) for p in (0, 0.5, 0.75, 0.76, 1)]
    assert q == [1.0, 1.0, 1.0, 2.0, 2.0]
    assert all(type(x) is float for x in [*q, ts.variance(until=4)])


def test_distribution_zero_length_span() -> None:
    async def body(ts: TimeSeries) -> None:
        ts.record(1)
        ts.record(7)

    ts = run_ts(body)
    assert ts.time_fractions(until=0) == {7.0: 1.0}
    assert ts.quantile(0.3, until=0) == 7.0
    assert ts.variance(until=0) == 0.0


def test_distribution_errors_like_mean() -> None:
    ts = run_ts(_dist)
    calls: list[Callable[..., object]] = [
        ts.time_fractions,
        ts.variance,
        lambda until=None: ts.quantile(0.5, until=until),
    ]
    for call in calls:
        with pytest.raises(RuntimeError):
            call()  # until=None outside a run
        with pytest.raises(ValueError):
            call(until=1.5)  # earlier than the last record (t=2)
        with pytest.raises(TypeError):
            call(until=True)
        with pytest.raises(ValueError):
            call(until=math.nan)
    empty = TimeSeries()
    for call in (
        lambda: empty.time_fractions(until=1),
        lambda: empty.quantile(0.5, until=1),
        lambda: empty.variance(until=1),
    ):
        with pytest.raises(ValueError):
            call()
    for p in (-0.1, 1.1, math.nan):
        with pytest.raises(ValueError):
            ts.quantile(p, until=4)


def test_mm1_time_fractions_match_geometric_law() -> None:
    rho = 0.5

    async def main() -> dict[float, float]:
        number = TimeSeries()
        count = 0

        def change(delta: int) -> None:
            nonlocal count
            count += delta
            number.record(count)

        number.record(0)
        arrivals, service = sim.random("arrivals"), sim.random("service")
        lock = asyncio.Lock()

        async def customer() -> None:
            change(1)
            async with lock:
                await sim.timeout(service.expovariate(1.0))
            change(-1)

        tasks = []
        for _ in range(10_000):
            await sim.timeout(arrivals.expovariate(rho))
            tasks.append(sim.spawn(customer()))
        await asyncio.gather(*tasks)
        return number.time_fractions()

    # Bands: seeds 1..10 were checked locally (10k customers); the error of
    # P(N=k) was within about +-0.02, so the band is 0.05 (P(N=3) = 0.0625).
    for seed in (1, 2, 3):
        fr = desyncio.run(main(), seed=seed)
        for k in range(4):
            assert fr[float(k)] == pytest.approx((1 - rho) * rho**k, abs=0.05)
