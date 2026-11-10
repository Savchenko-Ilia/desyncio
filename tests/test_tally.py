"""STA-01: ``Tally``, statistics of observations (not time-weighted)."""

import asyncio
import math
import statistics
from collections.abc import Callable
from typing import Any

import pytest

import desyncio
from desyncio import sim
from desyncio.tally import Tally

DATA = [2.5, 1.0, 4.0, 4.0, 7.5, 3.0, 9.0, 0.5]


def make(values: list[float] = DATA) -> Tally:
    t = Tally()
    for v in values:
        t.record(v)
    return t


def test_basic_statistics_match_statistics_module() -> None:
    t = make()
    assert len(t) == len(DATA)
    assert t.samples == tuple(DATA)
    assert t.mean() == pytest.approx(statistics.mean(DATA))
    assert t.variance() == pytest.approx(statistics.variance(DATA))
    assert t.stdev() == pytest.approx(statistics.stdev(DATA))
    assert (t.min(), t.max()) == (0.5, 9.0)
    for x in (t.mean(), t.variance(), t.stdev(), t.min(), t.max(), *t.samples):
        assert type(x) is float
    assert make([1, 2]).samples == (1.0, 2.0)  # ints are stored as floats


@pytest.mark.parametrize("n", [4, 10, 100])
def test_quantile_matches_statistics_inclusive(n: int) -> None:
    t = make()
    expected = statistics.quantiles(DATA, n=n, method="inclusive")
    got = t.quantile([i / n for i in range(1, n)])
    assert isinstance(got, tuple)
    assert got == pytest.approx(expected)
    assert all(type(x) is float for x in got)


def test_quantile_scalar_and_range() -> None:
    t = make()
    q = t.quantile(0.5)
    assert type(q) is float and q == pytest.approx(statistics.median(DATA))
    assert (t.quantile(0), t.quantile(1)) == (0.5, 9.0)
    for bad in (-0.1, 1.1, [0.5, 2]):
        with pytest.raises(ValueError):
            t.quantile(bad)


def test_histogram() -> None:
    counts, edges = make().histogram(3)
    assert counts == (4, 2, 2)
    assert edges == pytest.approx((0.5, 3.3333333333, 6.1666666667, 9.0))
    assert all(type(c) is int for c in counts)
    assert all(type(e) is float for e in edges)
    counts, edges = make().histogram([0, 3, 10])
    assert counts == (3, 5) and edges == (0.0, 3.0, 10.0)


@pytest.mark.parametrize("bad", [True, "1", None, 1 + 2j])
def test_record_rejects_non_real(bad: Any) -> None:
    t = make([1.0])
    with pytest.raises(TypeError):
        t.record(bad)
    assert t.samples == (1.0,)


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), -float("inf"), 10**400])
def test_record_rejects_non_finite(bad: float) -> None:
    t = make([1.0])
    with pytest.raises(ValueError):
        t.record(bad)
    assert t.samples == (1.0,)


def test_record_works_outside_a_run_and_does_not_read_the_clock() -> None:
    make([1.0]).record(2.0)  # no desyncio.run: no RuntimeError


STATS: list[Callable[[Tally], object]] = [
    lambda t: t.mean(),
    lambda t: t.variance(),
    lambda t: t.stdev(),
    lambda t: t.min(),
    lambda t: t.max(),
    lambda t: t.quantile(0.5),
    lambda t: t.quantile([0.5]),
    lambda t: t.histogram(3),
]


@pytest.mark.parametrize("stat", STATS)
def test_empty_tally_raises(stat: Callable[[Tally], object]) -> None:
    with pytest.raises(ValueError):
        stat(Tally())


def test_variance_needs_two_values() -> None:
    t = make([1.0])
    assert t.mean() == 1.0
    for stat in (t.variance, t.stdev):
        with pytest.raises(ValueError):
            stat()


# --- warm-up (STA-03) ---------------------------------------------------------


def test_warmup_validation() -> None:
    assert Tally().warmup == 0.0
    assert Tally(warmup=3).warmup == 3.0
    for bad in (True, "1", None):
        with pytest.raises(TypeError):
            Tally(warmup=bad)  # type: ignore[arg-type]
    for bad_value in (-0.5, math.nan, math.inf, -math.inf):
        with pytest.raises(ValueError):
            Tally(warmup=bad_value)


def test_warmup_drops_observations_before_it() -> None:
    t = Tally(warmup=2)

    async def main() -> None:
        t.record(1)  # t=0: dropped
        await sim.timeout(1.5)
        t.record(2)  # dropped
        await sim.timeout(0.5)
        t.record(3)  # t=2 == warmup: kept
        with pytest.raises(ValueError):
            t.record(math.nan)  # still validated, even when it would be dropped
        await sim.timeout(1)
        t.record(4)

    desyncio.run(main())
    assert t.samples == (3.0, 4.0)
    assert len(t) == 2
    assert t.mean() == 3.5


def test_warmup_needs_a_run_only_when_positive() -> None:
    zero = Tally()
    zero.record(1)  # outside a run: unchanged
    assert zero.samples == (1.0,)
    with pytest.raises(RuntimeError):
        Tally(warmup=1).record(1)
    with pytest.raises(TypeError):  # validation comes first
        Tally(warmup=1).record(True)


def test_quantile_validates_p_like_timeseries() -> None:
    t = make()
    bad: list[tuple[Any, type[Exception]]] = [
        (True, TypeError),
        ("0.5", TypeError),
        (None, TypeError),
        ([0.5, "0.5"], TypeError),
        ([0.5, True], TypeError),
        ((0.5, None), TypeError),
        (math.nan, ValueError),
        (-0.1, ValueError),
        (1.1, ValueError),
        ([0.5, math.nan], ValueError),
        ([0.5, 2], ValueError),
        (math.inf, ValueError),
    ]
    for p, error in bad:
        with pytest.raises(error):
            t.quantile(p)
    assert t.quantile(1) == 9.0
    assert t.quantile([0, 1]) == (0.5, 9.0)


def _mm1_waits(seed: int, *tallies: Tally) -> None:
    """M/M/1 (lambda=0.5, mu=1) with 50 customers already waiting at t=0."""

    async def main() -> None:
        arrivals, service = sim.random("arrivals"), sim.random("service")
        lock = asyncio.Lock()

        async def customer() -> None:
            arrived = sim.now
            async with lock:
                await sim.timeout(service.expovariate(1.0))
            for t in tallies:
                t.record(sim.now - arrived)  # time in system, at departure

        tasks = [sim.spawn(customer()) for _ in range(50)]
        while sim.now < 1500:
            await sim.timeout(arrivals.expovariate(0.5))
            tasks.append(sim.spawn(customer()))
        await asyncio.gather(*tasks)

    desyncio.run(main(), seed=seed)


def test_warmup_removes_initial_transient_from_waiting_times() -> None:
    # Theory: mean time in system W = 1/(mu-lambda) = 2. The 50 initial
    # customers wait up to ~100 and drag the mean up. Measured over seeds 1..8
    # (arrivals until t=1500, ~750-840 customers): no warm-up 3.46..6.87
    # (always > 3); warmup=200 1.92..2.16 (largest error 0.16). The band 0.3
    # is about 2x that; the test runs seeds 1..3.
    for seed in (1, 2, 3):
        cold, warm = Tally(), Tally(warmup=200)
        _mm1_waits(seed, cold, warm)
        assert len(warm) < len(cold)
        assert cold.mean() > 3
        assert warm.mean() == pytest.approx(2.0, abs=0.3)
