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


# --- CLN-05: shared float validator -----------------------------------------

_BIG = 10**400


def _helper_kwargs() -> dict[str, str]:
    return {
        "type_error": "x must be real, got {type}",
        "overflow_error": "x too large",
    }


def test_helper_rejects_bool_and_non_numbers() -> None:
    from desyncio._validate import to_float

    for bad in (True, False, "1", None, 1j):
        with pytest.raises(
            TypeError, match="x must be real, got " + type(bad).__name__
        ):
            to_float(bad, **_helper_kwargs())


def test_helper_converts_int_and_float() -> None:
    from desyncio._validate import to_float

    result = to_float(3, **_helper_kwargs())
    assert result == 3.0
    assert isinstance(result, float)
    assert to_float(2.5, **_helper_kwargs()) == 2.5


def test_helper_overflow_is_value_error() -> None:
    from desyncio._validate import to_float

    with pytest.raises(ValueError, match="x too large"):
        to_float(_BIG, **_helper_kwargs())


def test_helper_inf_and_nan_pass_without_finite_error() -> None:
    from desyncio._validate import to_float

    assert to_float(math.inf, **_helper_kwargs()) == math.inf
    assert math.isnan(to_float(math.nan, **_helper_kwargs()))


def test_helper_finite_error_rejects_inf_and_nan() -> None:
    from desyncio._validate import to_float

    for bad in (math.inf, -math.inf, math.nan):
        with pytest.raises(ValueError, match="x not finite: "):
            to_float(bad, finite_error="x not finite: {value!r}", **_helper_kwargs())


def test_helper_normalise_zero_option() -> None:
    from desyncio._validate import to_float

    assert math.copysign(1.0, to_float(-0.0, **_helper_kwargs())) == -1.0
    kept = to_float(-0.0, normalise_zero=True, **_helper_kwargs())
    assert math.copysign(1.0, kept) == 1.0


def test_clock_messages_unchanged() -> None:
    with pytest.raises(TypeError) as t:
        VirtualClock(start="a")  # type: ignore[arg-type]
    assert str(t.value) == "start must be a real number (int or float), got str"
    with pytest.raises(ValueError) as o:
        VirtualClock(start=_BIG)
    assert (
        str(o.value)
        == "start must be a finite number, got an int too large for a float"
    )
    with pytest.raises(ValueError) as n:
        VirtualClock().advance_to(math.nan)
    assert str(n.value) == "advance_to must be a finite number, got nan"


def test_until_messages_unchanged() -> None:
    from desyncio.runners import _check_until

    with pytest.raises(TypeError) as t:
        _check_until("a")
    assert str(t.value) == "until must be a real number or None, got str"
    with pytest.raises(ValueError) as o:
        _check_until(_BIG)
    assert str(o.value) == "until must be finite, got an int too large for a float"
    with pytest.raises(ValueError) as n:
        _check_until(math.nan)
    assert str(n.value) == "until must be finite, got nan"
    with pytest.raises(ValueError) as neg:
        _check_until(-1)
    assert str(neg.value) == "until must be >= the start time 0.0, got -1.0"
    assert math.copysign(1.0, _check_until(-0.0)) == -1.0


def test_delay_messages_unchanged() -> None:
    from desyncio.sim import _check_delay

    with pytest.raises(TypeError) as t:
        _check_delay("a")
    assert str(t.value) == "dt must be a real number, got str"
    with pytest.raises(ValueError) as o:
        _check_delay(_BIG)
    assert str(o.value) == "dt must be finite, got an int too large"
    with pytest.raises(ValueError) as n:
        _check_delay(math.nan)
    assert str(n.value) == "dt must be finite, got nan"
    with pytest.raises(ValueError) as neg:
        _check_delay(-1)
    assert str(neg.value) == "dt must be >= 0, got -1.0"


def test_speed_messages_unchanged_and_overflow_is_value_error() -> None:
    from desyncio.realtime import _check_speed

    with pytest.raises(TypeError) as t:
        _check_speed("a")
    assert str(t.value) == "speed must be a real number, got str"
    with pytest.raises(ValueError) as n:
        _check_speed(math.nan)
    assert str(n.value) == "speed must be > 0 (or math.inf), got nan"
    with pytest.raises(ValueError) as z:
        _check_speed(0)
    assert str(z.value) == "speed must be > 0 (or math.inf), got 0"
    assert _check_speed(math.inf) == math.inf
    # D040: was OverflowError.
    with pytest.raises(ValueError, match="speed must be .*int too large for a float"):
        _check_speed(_BIG)
