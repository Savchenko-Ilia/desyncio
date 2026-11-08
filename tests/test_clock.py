"""Tests for the virtual clock (LOOP-01)."""

import math

import pytest
from hypothesis import given
from hypothesis import strategies as st

from desyncio.clock import VirtualClock


def test_default_start_is_zero() -> None:
    assert VirtualClock().now == 0.0


def test_custom_start() -> None:
    assert VirtualClock(start=5.5).now == 5.5


def test_int_start_is_coerced_to_float() -> None:
    clock = VirtualClock(start=3)
    assert isinstance(clock.now, float)
    assert clock.now == 3.0


def test_advance_forward() -> None:
    clock = VirtualClock()
    clock.advance_to(2.5)
    assert clock.now == 2.5
    clock.advance_to(4)
    assert clock.now == 4.0
    assert isinstance(clock.now, float)


def test_advance_to_same_time_is_noop() -> None:
    clock = VirtualClock(start=1.0)
    clock.advance_to(1.0)
    assert clock.now == 1.0


def test_advance_backwards_raises_with_both_times() -> None:
    clock = VirtualClock(start=10.0)
    with pytest.raises(ValueError, match=r"10\.0.*9\.5"):
        clock.advance_to(9.5)
    assert clock.now == 10.0  # unchanged after the failed call


@pytest.mark.parametrize("bad", [math.nan, math.inf, -math.inf])
def test_non_finite_start_rejected(bad: float) -> None:
    with pytest.raises(ValueError, match="finite"):
        VirtualClock(start=bad)


@pytest.mark.parametrize("bad", [math.nan, math.inf, -math.inf])
def test_non_finite_advance_rejected(bad: float) -> None:
    clock = VirtualClock()
    with pytest.raises(ValueError, match="finite"):
        clock.advance_to(bad)
    assert clock.now == 0.0


finite = st.floats(min_value=-1e9, max_value=1e9, allow_nan=False, allow_infinity=False)


gaps = st.lists(st.floats(min_value=0, max_value=1e6), max_size=50)


@given(start=finite, deltas=gaps)
def test_advancing_through_sorted_times_ends_at_last(
    start: float, deltas: list[float]
) -> None:
    times: list[float] = []
    t = start
    for d in deltas:
        t += d
        times.append(t)
    times.sort()  # already non-decreasing; sort guards against float rounding

    clock = VirtualClock(start=start)
    previous = clock.now
    for t in times:
        clock.advance_to(t)
        assert clock.now >= previous
        previous = clock.now
    assert clock.now == (times[-1] if times else start)
