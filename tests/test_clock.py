"""Tests for the virtual clock (LOOP-01)."""

import math

import pytest
from hypothesis import assume, given
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


sorted_times = st.lists(finite, max_size=50).map(sorted)


@given(start=finite, times=sorted_times)
def test_advancing_through_sorted_times_ends_at_last(
    start: float, times: list[float]
) -> None:
    times = [max(t, start) for t in times]  # keep every target >= start
    clock = VirtualClock(start=start)
    previous = clock.now
    for t in times:
        clock.advance_to(t)
        assert clock.now >= previous
        previous = clock.now
    assert clock.now == (times[-1] if times else start)


@given(now=finite, t=finite)
def test_advancing_backwards_raises_and_keeps_now(now: float, t: float) -> None:
    assume(t < now)
    clock = VirtualClock(start=now)
    with pytest.raises(ValueError, match="backwards"):
        clock.advance_to(t)
    assert clock.now == now


class _Int(int):
    pass


class _Float(float):
    pass


BAD_TYPES = ["5", b"5", True, False, None, 1 + 2j, object(), [1]]


@pytest.mark.parametrize("bad", BAD_TYPES)
def test_start_rejects_non_real(bad: object) -> None:
    with pytest.raises(TypeError, match=rf"start.*{type(bad).__name__}"):
        VirtualClock(start=bad)  # type: ignore[arg-type]


@pytest.mark.parametrize("bad", BAD_TYPES)
def test_advance_to_rejects_non_real(bad: object) -> None:
    clock = VirtualClock(start=1.0)
    with pytest.raises(TypeError, match=rf"advance_to.*{type(bad).__name__}"):
        clock.advance_to(bad)  # type: ignore[arg-type]
    assert clock.now == 1.0


def test_int_and_float_subclasses_accepted() -> None:
    assert VirtualClock(start=_Int(3)).now == 3.0
    assert VirtualClock(start=_Float(2.5)).now == 2.5


@pytest.mark.parametrize("big", [10**400, -(10**400)])
def test_huge_int_raises_value_error(big: int) -> None:
    with pytest.raises(ValueError):
        VirtualClock(start=big)
    clock = VirtualClock()
    with pytest.raises(ValueError):
        clock.advance_to(big)
    assert clock.now == 0.0


def test_negative_zero_normalised() -> None:
    assert str(VirtualClock(-0.0).now) == "0.0"
    clock = VirtualClock(start=-1.0)
    clock.advance_to(-0.0)
    assert str(clock.now) == "0.0"
