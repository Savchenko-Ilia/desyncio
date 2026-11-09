"""STA-01: ``Tally``, statistics of observations (not time-weighted)."""

import statistics
from collections.abc import Callable
from typing import Any

import pytest

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
