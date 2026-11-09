"""Tests for ``desyncio.monitor.TimeSeries`` (MON-01)."""

import asyncio
import math

import pytest

import desyncio
from desyncio import sim
from desyncio.monitor import TimeSeries
from desyncio.resource import Resource


def run_ts(body):  # type: ignore[no-untyped-def]
    """Run ``body(ts)`` (an async function) in a fresh sim; return the series."""
    ts = TimeSeries()

    async def main() -> None:
        await body(ts)

    desyncio.run(main())
    return ts


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


def test_mean_default_until_is_sim_now_inside_run() -> None:
    async def main() -> float:
        ts = TimeSeries()
        ts.record(0)
        await sim.timeout(1)
        ts.record(10)
        await sim.timeout(1)
        return ts.mean()  # 0 for 1s, 10 for 1s

    assert desyncio.run(main()) == pytest.approx(5.0)


def test_mean_without_until_outside_run_raises_like_sim_now() -> None:
    ts = run_ts(_steps)
    with pytest.raises(RuntimeError):
        ts.mean()
    with pytest.raises(RuntimeError):
        sim.now  # noqa: B018 - the error mean() must match


def test_record_outside_run_raises() -> None:
    with pytest.raises(RuntimeError):
        TimeSeries().record(1)


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
    for call in (lambda: ts.mean(until=1), ts.min, ts.max, ts.last):
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


@pytest.mark.parametrize("bad", [True, "1", None, 1 + 2j])
def test_record_rejects_non_real_values(bad: object) -> None:
    async def main() -> None:
        with pytest.raises(TypeError):
            TimeSeries().record(bad)  # type: ignore[arg-type]

    desyncio.run(main())


@pytest.mark.parametrize("bad", [math.nan, math.inf, -math.inf])
def test_record_rejects_non_finite_values(bad: float) -> None:
    async def main() -> None:
        with pytest.raises(ValueError):
            TimeSeries().record(bad)

    desyncio.run(main())


def test_mean_rejects_bad_until() -> None:
    ts = run_ts(_steps)
    with pytest.raises(TypeError):
        ts.mean(until=True)  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        ts.mean(until=math.nan)


def test_failed_record_leaves_series_unchanged() -> None:
    async def main() -> TimeSeries:
        ts = TimeSeries()
        ts.record(1)
        with pytest.raises(ValueError):
            ts.record(math.nan)
        return ts

    assert desyncio.run(main()).samples == ((0.0, 1.0),)


def test_plugs_into_resource_monitor_hook() -> None:
    async def main() -> TimeSeries:
        busy = TimeSeries()
        res = Resource(1, monitor=lambda r: busy.record(r.count))
        busy.record(0)

        async def user() -> None:
            async with res:
                await sim.timeout(3)

        t = sim.spawn(user())
        await sim.timeout(1)
        await t
        await sim.timeout(2)
        return busy

    busy = desyncio.run(main())
    assert busy.samples == ((0.0, 0.0), (0.0, 1.0), (3.0, 0.0))
    assert busy.mean(until=6) == pytest.approx(0.5)


# --- M/M/1 -----------------------------------------------------------------

RHO = 0.5
N_CUSTOMERS = 20_000
SEED = 7


def mm1_stats(seed: int) -> tuple[float, float, TimeSeries]:
    async def main() -> tuple[float, float, TimeSeries]:
        busy = TimeSeries()
        waiting = TimeSeries()
        server = Resource(
            1,
            monitor=lambda r: (busy.record(r.count), waiting.record(r.queue_length)),
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
        return busy.mean(), waiting.mean(), busy

    return desyncio.run(main(), seed=seed)


def test_mm1_utilisation_and_queue_length_match_theory() -> None:
    util, queue, _ = mm1_stats(SEED)
    # Theory: utilisation rho = 0.5, Lq = rho^2 / (1 - rho) = 0.5.
    # Bands: over six seeds checked locally (20k customers, rho=0.5) the error
    # was within +-0.003 for utilisation and +-0.015 for Lq. The bands are
    # about 3x that: a sanity check on the integral, not a statistical test.
    # The seed is fixed.
    assert util == pytest.approx(RHO, abs=0.01)
    assert queue == pytest.approx(RHO**2 / (1 - RHO), abs=0.05)


def test_mm1_same_seed_gives_identical_samples() -> None:
    a = mm1_stats(SEED)[2].samples
    b = mm1_stats(SEED)[2].samples
    assert a == b
    assert len(a) > 1000
